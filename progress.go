package download

import (
	"fmt"
	"time"
)

// RoleStats summarises the runtime state of a Role.
type RoleStats struct {
	Total         int     `json:"total"`
	Pending       int     `json:"pending"`
	Probing       int     `json:"probing"`
	Downloading   int     `json:"downloading"`
	Paused        int     `json:"paused"`
	Completed     int     `json:"completed"`
	Failed        int     `json:"failed"`
	Canceled      int     `json:"canceled"`
	Workers       int     `json:"workers"`
	BusyWorkers   int     `json:"busy_workers"`
	QueuedTasks   int     `json:"queued_tasks"`
	QueuedChunks  int     `json:"queued_chunks"`
	GlobalSpeed   float64 `json:"global_speed"`
	DroppedEvents uint64  `json:"dropped_events"`
}

// StatusSnapshot is a point-in-time view of the whole Role.
type StatusSnapshot struct {
	GeneratedAt time.Time  `json:"generated_at"`
	Seq         uint64     `json:"seq"`
	Consistent  bool       `json:"consistent"`
	Summary     RoleStats  `json:"summary"`
	Tasks       []TaskInfo `json:"tasks,omitempty"`
}

// SnapshotOption configures Snapshot.
type SnapshotOption func(*snapshotConfig)

type snapshotConfig struct {
	details bool
	ids     []TaskID
	sortBy  SortField
	desc    bool
}

// WithSnapshotDetails controls whether task details are included. Details are
// included by default.
func WithSnapshotDetails(include bool) SnapshotOption {
	return func(c *snapshotConfig) { c.details = include }
}

// WithSnapshotIDs limits the detail list to the given task IDs. The summary is
// always global.
func WithSnapshotIDs(ids ...TaskID) SnapshotOption {
	return func(c *snapshotConfig) { c.ids = append(c.ids, ids...) }
}

// WithSnapshotSort orders the detail list.
func WithSnapshotSort(field SortField, desc bool) SnapshotOption {
	return func(c *snapshotConfig) {
		c.sortBy = field
		c.desc = desc
	}
}

// Snapshot returns global counters plus, optionally, task details. Seq is taken
// before the details are read; combined with a subscription that started
// earlier, events with Seq > snapshot.Seq can be applied without loss.
func (r *Role) Snapshot(opts ...SnapshotOption) (StatusSnapshot, error) {
	cfg := snapshotConfig{details: true, sortBy: SortByCreatedAt}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if !validSortField(cfg.sortBy) {
		return StatusSnapshot{}, fmt.Errorf("%w: unknown sort field %q", ErrInvalidFilter, cfg.sortBy)
	}
	seq := r.hub.currentSeq()
	infos := r.reg.infos()
	snap := StatusSnapshot{
		GeneratedAt: time.Now(),
		Seq:         seq,
		Consistent:  true,
		Summary:     r.buildStats(infos),
	}
	if !cfg.details {
		return snap, nil
	}
	if len(cfg.ids) > 0 {
		allowed := make(map[TaskID]struct{}, len(cfg.ids))
		for _, id := range cfg.ids {
			allowed[id] = struct{}{}
		}
		kept := infos[:0]
		for _, info := range infos {
			if _, ok := allowed[info.ID]; ok {
				kept = append(kept, info)
			}
		}
		infos = kept
	}
	sortInfos(infos, cfg.sortBy, cfg.desc)
	if infos == nil {
		infos = []TaskInfo{}
	}
	snap.Tasks = infos
	return snap, nil
}

// Stats returns the same global counters as Snapshot without task details.
func (r *Role) Stats() RoleStats {
	return r.buildStats(r.reg.infos())
}

func (r *Role) buildStats(infos []TaskInfo) RoleStats {
	st := RoleStats{Total: len(infos)}
	for _, info := range infos {
		switch info.Status {
		case StatusPending:
			st.Pending++
		case StatusProbing:
			st.Probing++
		case StatusDownloading:
			st.Downloading++
			st.GlobalSpeed += info.Speed
		case StatusPaused:
			st.Paused++
		case StatusCompleted:
			st.Completed++
		case StatusFailed:
			st.Failed++
		case StatusCanceled:
			st.Canceled++
		}
	}
	ex := r.exec.snapshot()
	st.Workers = ex.workers
	st.BusyWorkers = ex.busy
	st.QueuedTasks = ex.queuedTasks
	st.QueuedChunks = ex.queuedChunks
	st.DroppedEvents = r.hub.droppedCount()
	return st
}

// sampleProgress is the single writer of speed/ETA. It runs on the sampling
// tick and publishes one coalescible progress event per active task.
func (r *Role) sampleProgress(now time.Time) {
	for _, t := range r.reg.all() {
		if p, ok := t.sample(now, 0.4); ok {
			r.hub.publish(EventTaskProgress, t.id, p)
		}
	}
}
