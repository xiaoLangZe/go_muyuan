package download

import (
	"time"
)

// EventType classifies an event published by the hub.
type EventType string

const (
	// EventTaskAdded is published when a task is registered.
	EventTaskAdded EventType = "task_added"
	// EventTaskState is published on every legal state transition and on
	// updates that change the public task description. Never dropped.
	EventTaskState EventType = "task_state"
	// EventTaskProgress carries a sampled progress reading. May be coalesced.
	EventTaskProgress EventType = "task_progress"
	// EventTaskRemoved is published after a task record is deleted. Never
	// dropped.
	EventTaskRemoved EventType = "task_removed"
	// EventWorkerChange is published when the worker pool is resized. Never
	// dropped.
	EventWorkerChange EventType = "worker_change"
)

// Event is the unit of the process-local event stream.
//
// Seq is globally monotonic per Role. Consumers can use it to detect gaps
// (re-snapshot when one is found) and, together with WithSince and a replay
// buffer, to resume after a disconnect. SSE transports can map Seq to the
// "id:" field and turn Last-Event-ID into WithSince.
//
// Payload types:
//   - task_added, task_state: TaskInfo
//   - task_progress: Progress
//   - task_removed: RemovedPayload
//   - worker_change: WorkerChange
type Event struct {
	Seq       uint64    `json:"seq"`
	Type      EventType `json:"type"`
	TaskID    TaskID    `json:"task_id,omitempty"`
	Timestamp time.Time `json:"timestamp"`
	Payload   any       `json:"payload,omitempty"`
}

// RemovedPayload is the payload of EventTaskRemoved.
type RemovedPayload struct {
	ID   TaskID `json:"id"`
	Path string `json:"path"`
}

// WorkerChange is the payload of EventWorkerChange.
type WorkerChange struct {
	Workers int `json:"workers"`
	Busy    int `json:"busy"`
}

const defaultSubscribeBuffer = 64

type subscribeConfig struct {
	types            map[EventType]struct{}
	tasks            map[TaskID]struct{}
	buffer           int
	progress         bool
	progressInterval time.Duration
	since            uint64
	sinceSet         bool
}

func defaultSubscribeConfig() subscribeConfig {
	return subscribeConfig{buffer: defaultSubscribeBuffer, progress: true}
}

func (c subscribeConfig) matches(ev Event) bool {
	if c.types != nil {
		if _, ok := c.types[ev.Type]; !ok {
			return false
		}
	}
	if c.tasks != nil {
		if _, ok := c.tasks[ev.TaskID]; !ok {
			return false
		}
	}
	return true
}

// SubscribeOption configures a subscription.
type SubscribeOption func(*subscribeConfig)

// WithEventTypes limits the subscription to the given event types. Supplying
// this option without EventTaskProgress turns progress sampling off for the
// subscription.
func WithEventTypes(types ...EventType) SubscribeOption {
	return func(c *subscribeConfig) {
		c.types = make(map[EventType]struct{}, len(types))
		for _, t := range types {
			c.types[t] = struct{}{}
		}
		c.progress = false
		if _, ok := c.types[EventTaskProgress]; ok {
			c.progress = true
		}
	}
}

// WithSubscribeTasks limits the subscription to the given task IDs.
func WithSubscribeTasks(ids ...TaskID) SubscribeOption {
	return func(c *subscribeConfig) {
		c.tasks = make(map[TaskID]struct{}, len(ids))
		for _, id := range ids {
			c.tasks[id] = struct{}{}
		}
	}
}

// WithSubscribeBuffer sets the subscriber channel capacity. A slow consumer
// that fills the buffer on a non-droppable event is disconnected (its channel is
// closed) rather than blocking the publisher.
func WithSubscribeBuffer(size int) SubscribeOption {
	return func(c *subscribeConfig) {
		if size > 0 {
			c.buffer = size
		}
	}
}

// WithSubscribeInterval throttles progress delivery for this subscription. A
// zero or negative duration delivers every sampled progress event.
func WithSubscribeInterval(d time.Duration) SubscribeOption {
	return func(c *subscribeConfig) {
		if d < 0 {
			d = 0
		}
		c.progressInterval = d
	}
}

// WithSince requests replay of buffered events after seq. It requires a replay
// buffer (WithReplayBuffer). If seq has already fallen out of the buffer the
// returned channel is closed immediately; the caller should fall back to
// Snapshot.
func WithSince(seq uint64) SubscribeOption {
	return func(c *subscribeConfig) {
		c.since = seq
		c.sinceSet = true
	}
}

// Subscribe returns a channel of events plus a cancel function. The channel is
// closed by the package (on cancel, Role.Close, or when the consumer is too
// slow); the consumer must only receive from it.
//
// To avoid a race between a fresh subscriber and the first event, subscribe
// first and then call Snapshot: skip live events with Seq <= snapshot.Seq and
// apply newer ones to the local view.
func (r *Role) Subscribe(opts ...SubscribeOption) (<-chan Event, func()) {
	cfg := defaultSubscribeConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if r.closed.Load() {
		return closedEventChan(), func() {}
	}
	return r.hub.subscribe(cfg)
}
