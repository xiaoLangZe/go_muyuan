package download

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// hub is the single exit point for state changes. Every event gets a globally
// monotonic sequence number.
type hub struct {
	// publishMu serialises assignment and delivery so subscribers observe
	// strictly increasing sequence numbers.
	publishMu sync.Mutex

	mu        sync.Mutex
	seq       uint64
	subs      map[uint64]*subscriber
	nextSubID uint64
	replay    []Event
	replayCap int
	closed    bool

	dropped atomic.Uint64
}

func newHub(replayCap int) *hub {
	return &hub{
		subs:      make(map[uint64]*subscriber),
		replayCap: replayCap,
	}
}

func (h *hub) currentSeq() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.seq
}

func (h *hub) droppedCount() uint64 { return h.dropped.Load() }

// publish assigns the next sequence number and fans the event out. It never
// blocks on a subscriber.
func (h *hub) publish(typ EventType, taskID TaskID, payload any) uint64 {
	h.publishMu.Lock()
	defer h.publishMu.Unlock()

	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return 0
	}
	h.seq++
	ev := Event{
		Seq:       h.seq,
		Type:      typ,
		TaskID:    taskID,
		Timestamp: time.Now(),
		Payload:   payload,
	}
	if h.replayCap > 0 {
		h.replay = append(h.replay, ev)
		if len(h.replay) > h.replayCap {
			// Drop the oldest quarter at once to amortise copying.
			cut := h.replayCap / 4
			if cut < 1 {
				cut = 1
			}
			h.replay = append(h.replay[:0], h.replay[cut:]...)
		}
	}
	subs := make([]*subscriber, 0, len(h.subs))
	for _, s := range h.subs {
		subs = append(subs, s)
	}
	h.mu.Unlock()

	for _, s := range subs {
		s.deliver(ev)
	}
	return ev.Seq
}

// subscribe registers a subscriber. Replay events are queued while the hub lock
// is held so no live event can overtake them.
func (h *hub) subscribe(cfg subscribeConfig) (<-chan Event, func()) {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return closedEventChan(), func() {}
	}
	replay, ok := h.replaySince(cfg)
	if !ok {
		h.mu.Unlock()
		return closedEventChan(), func() {}
	}
	size := cfg.buffer
	if size < 1 {
		size = defaultSubscribeBuffer
	}
	if len(replay)+1 > size {
		size = len(replay) + 1
	}
	ch := make(chan Event, size)
	h.nextSubID++
	id := h.nextSubID
	s := &subscriber{
		hub:     h,
		id:      id,
		ch:      ch,
		cfg:     cfg,
		pending: make(map[TaskID]Event),
	}
	// Safe to send without blocking: the channel was sized for the replay.
	for _, ev := range replay {
		ch <- ev
	}
	h.subs[id] = s
	h.mu.Unlock()

	var once sync.Once
	cancel := func() {
		once.Do(func() { s.close() })
	}
	return ch, cancel
}

// replaySince returns the buffered events after cfg.since, with progress
// coalesced to the latest sample per task. ok is false when the requested
// sequence has already fallen out of the buffer.
func (h *hub) replaySince(cfg subscribeConfig) ([]Event, bool) {
	if !cfg.sinceSet {
		return nil, true
	}
	if cfg.since >= h.seq {
		return nil, true
	}
	if h.replayCap == 0 || len(h.replay) == 0 {
		return nil, false
	}
	if cfg.since+1 < h.replay[0].Seq {
		return nil, false
	}
	latestProgress := make(map[TaskID]Event)
	var out []Event
	for _, ev := range h.replay {
		if ev.Seq <= cfg.since || !cfg.matches(ev) {
			continue
		}
		if ev.Type == EventTaskProgress {
			latestProgress[ev.TaskID] = ev
			continue
		}
		out = append(out, ev)
	}
	for _, ev := range latestProgress {
		out = append(out, ev)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out, true
}

func (h *hub) removeSub(id uint64) {
	h.mu.Lock()
	delete(h.subs, id)
	h.mu.Unlock()
}

// close shuts every subscriber channel. It must not hold h.mu while taking a
// subscriber lock.
func (h *hub) close() {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return
	}
	h.closed = true
	subs := make([]*subscriber, 0, len(h.subs))
	for _, s := range h.subs {
		subs = append(subs, s)
	}
	h.subs = make(map[uint64]*subscriber)
	h.mu.Unlock()

	for _, s := range subs {
		s.close()
	}
}

func closedEventChan() <-chan Event {
	ch := make(chan Event)
	close(ch)
	return ch
}

type subscriber struct {
	hub *hub
	id  uint64
	ch  chan Event
	cfg subscribeConfig

	mu           sync.Mutex
	closed       bool
	pending      map[TaskID]Event
	lastProgress time.Time
}

// deliver applies filtering and backpressure. State events are never dropped:
// if the buffer is full the subscriber is kicked and its channel closed so the
// consumer can reconnect from a fresh Snapshot.
func (s *subscriber) deliver(ev Event) {
	if !s.cfg.matches(ev) {
		return
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	if ev.Type == EventTaskProgress {
		if !s.cfg.progress {
			s.mu.Unlock()
			return
		}
		if s.cfg.progressInterval > 0 && time.Since(s.lastProgress) < s.cfg.progressInterval {
			s.mu.Unlock()
			return
		}
		s.pending[ev.TaskID] = ev
		s.flushLocked()
		s.mu.Unlock()
		return
	}
	// A state event supersedes any stale progress sample for the same task.
	delete(s.pending, ev.TaskID)
	s.flushLocked()
	select {
	case s.ch <- ev:
		s.mu.Unlock()
	default:
		s.failLocked()
		s.mu.Unlock()
	}
}

func (s *subscriber) flushLocked() {
	for id, ev := range s.pending {
		select {
		case s.ch <- ev:
			delete(s.pending, id)
			s.lastProgress = time.Now()
		default:
			return
		}
	}
}

func (s *subscriber) failLocked() {
	if s.closed {
		return
	}
	s.closed = true
	close(s.ch)
	s.hub.dropped.Add(1)
	s.hub.removeSub(s.id)
}

func (s *subscriber) close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	close(s.ch)
	s.mu.Unlock()
	s.hub.removeSub(s.id)
}
