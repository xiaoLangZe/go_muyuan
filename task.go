package download

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// TaskID identifies a download task for the lifetime of its Role.
type TaskID string

// TaskStatus is the lifecycle state of a task.
//
// The legal transitions are:
//
//	pending     -> probing, paused, canceled, removed
//	probing     -> downloading, paused, canceled, failed, removed
//	downloading -> paused, canceled, completed, failed, removed
//	paused      -> pending, canceled, removed
//	canceled    -> pending, removed
//	failed      -> removed
//	completed   -> removed
type TaskStatus string

const (
	StatusPending     TaskStatus = "pending"
	StatusProbing     TaskStatus = "probing"
	StatusDownloading TaskStatus = "downloading"
	StatusPaused      TaskStatus = "paused"
	StatusCompleted   TaskStatus = "completed"
	StatusFailed      TaskStatus = "failed"
	StatusCanceled    TaskStatus = "canceled"
	StatusRemoved     TaskStatus = "removed"
)

// Terminal reports whether no further download work will happen without an
// explicit Resume or a new Add.
func (s TaskStatus) Terminal() bool {
	switch s {
	case StatusCompleted, StatusFailed, StatusCanceled, StatusRemoved:
		return true
	}
	return false
}

// OverwritePolicy controls what Add does when the target file already exists.
type OverwritePolicy string

const (
	// OverwriteError refuses to create a task and returns ErrFileExists.
	OverwriteError OverwritePolicy = "error"
	// OverwriteSkip refuses to create a task and returns ErrFileExists. It is
	// named "skip" because callers that treat ErrFileExists as success can use
	// it to mean "already downloaded".
	OverwriteSkip OverwritePolicy = "skip"
	// OverwriteReplace discards any existing file and sidecar and downloads
	// from scratch.
	OverwriteReplace OverwritePolicy = "replace"
	// OverwriteResume keeps the existing file and sidecar so an interrupted
	// download can continue.
	OverwriteResume OverwritePolicy = "resume"
)

// Sentinel errors returned by the package.
var (
	ErrTaskNotFound    = errors.New("download: task not found")
	ErrRoleClosed      = errors.New("download: role is closed")
	ErrTaskCanceled    = errors.New("download: task canceled")
	ErrTaskRemoved     = errors.New("download: task removed")
	ErrFileExists      = errors.New("download: target file already exists")
	ErrInvalidURL      = errors.New("download: invalid url")
	ErrInvalidFileName = errors.New("download: invalid file name")
	ErrPathEscape      = errors.New("download: path escapes the allowed root")
	ErrInvalidFilter   = errors.New("download: invalid filter")
	ErrInvalidPatch    = errors.New("download: invalid patch")
	ErrInvalidState    = errors.New("download: invalid task state")
	ErrInvalidOption   = errors.New("download: invalid option")
	ErrPrivateHost     = errors.New("download: private, loopback or reserved host is not allowed")
)

// errRangeChanged signals that the server answered a ranged request with 200,
// meaning the remote file changed and the transfer must restart.
var errRangeChanged = errors.New("download: ranged request returned 200, remote file changed")

// DuplicateTaskError is returned when an active task already owns the resolved
// target path.
type DuplicateTaskError struct {
	ID   TaskID
	Path string
}

func (e *DuplicateTaskError) Error() string {
	return fmt.Sprintf("download: %s is already used by active task %s", e.Path, e.ID)
}

// InvalidStateError is returned when an operation is not legal in the task's
// current state.
type InvalidStateError struct {
	ID   TaskID
	From TaskStatus
	Op   string
}

func (e *InvalidStateError) Error() string {
	return fmt.Sprintf("download: cannot %s task %s in state %s", e.Op, e.ID, e.From)
}

// Is makes errors.Is(err, ErrInvalidState) work for all state errors.
func (e *InvalidStateError) Is(target error) bool { return target == ErrInvalidState }

// Progress is the payload of EventTaskProgress. Progress events are samples:
// they may be coalesced and are the only event type that can be dropped.
type Progress struct {
	TaskID     TaskID        `json:"task_id"`
	Downloaded int64         `json:"downloaded"`
	Total      int64         `json:"total"`
	Speed      float64       `json:"speed"`
	ETA        time.Duration `json:"eta"`
}

// TaskInfo is an immutable snapshot of a task. It contains no pointers into
// internal state and is safe to retain or serialize.
type TaskInfo struct {
	ID          TaskID            `json:"id"`
	URL         string            `json:"url"`
	FileName    string            `json:"file_name"`
	Path        string            `json:"path"`
	Status      TaskStatus        `json:"status"`
	Priority    int               `json:"priority"`
	Tags        map[string]string `json:"tags,omitempty"`
	RateLimit   int64             `json:"rate_limit,omitempty"`
	Resume      bool              `json:"resume"`
	TotalSize   int64             `json:"total_size"`
	Downloaded  int64             `json:"downloaded"`
	Progress    float64           `json:"progress"`
	Speed       float64           `json:"speed"`
	ETA         time.Duration     `json:"eta"`
	ChunksTotal int               `json:"chunks_total"`
	ChunksDone  int               `json:"chunks_done"`
	Retries     int               `json:"retries"`
	Error       string            `json:"error,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
	CompletedAt *time.Time        `json:"completed_at,omitempty"`
}

// taskConfig is the resolved per-task option set.
type taskConfig struct {
	fileName   string
	priority   int
	rateLimit  int64
	resume     bool
	userAgent  string
	tags       map[string]string
	idempotent bool
	overwrite  OverwritePolicy
	maxRetries int
	retryBase  time.Duration
}

type stopReason int

const (
	stopNone stopReason = iota
	stopPause
	stopCancel
)

type Task struct {
	role *Role

	// id and seq are written once while the registry lock is held, then read
	// without synchronisation.
	id        TaskID
	seq       uint64
	createdAt time.Time

	// mu protects every mutable field below that is not an atomic.
	mu          sync.Mutex
	url         string
	saveDir     string
	fileName    string
	path        string
	pathKey     string
	status      TaskStatus
	updatedAt   time.Time
	completedAt *time.Time
	priority    int
	tags        map[string]string
	cfg         taskConfig

	total          int64
	etag           string
	lastModified   string
	rangeSupported bool

	chunkSize  int64
	chunkDone  []bool
	chunksDone int
	speed      float64
	eta        time.Duration
	retries    int
	lastErr    string

	// run control
	ctx        context.Context
	cancel     context.CancelFunc
	stop       stopReason
	done       chan struct{}
	doneClosed bool
	termErr    error
	runDone    chan struct{}

	metaPath string

	// last sample state for speed/ETA
	lastSampleAt    time.Time
	lastSampleBytes int64

	// downloaded is the number of bytes currently on disk for this task. It is
	// updated by chunk workers, so it is atomic rather than guarded by mu.
	downloaded atomic.Int64

	// limiter is the effective per-task limiter (nil means unlimited). It can
	// be replaced at runtime by Update.
	limiter atomic.Pointer[limiter]

	globalLimiter *limiter
}

func newTask(url, saveDir, fileName, path string, cfg taskConfig, role *Role) *Task {
	now := time.Now()
	t := &Task{
		role:      role,
		url:       url,
		saveDir:   saveDir,
		fileName:  fileName,
		path:      path,
		pathKey:   pathKey(path),
		status:    StatusPending,
		createdAt: now,
		updatedAt: now,
		priority:  cfg.priority,
		tags:      cfg.tags,
		cfg:       cfg,
		metaPath:  path + ".dltmp.json",
		done:      make(chan struct{}),
	}
	if cfg.rateLimit > 0 {
		t.limiter.Store(newLimiter(cfg.rateLimit))
	}
	t.globalLimiter = role.globalLimiter
	return t
}

func canTransition(from, to TaskStatus) bool {
	if from == to {
		return true
	}
	switch from {
	case StatusPending:
		return to == StatusProbing || to == StatusPaused || to == StatusCanceled ||
			to == StatusFailed || to == StatusRemoved
	case StatusProbing:
		return to == StatusDownloading || to == StatusPaused || to == StatusCanceled ||
			to == StatusCompleted || to == StatusFailed || to == StatusRemoved
	case StatusDownloading:
		return to == StatusPaused || to == StatusCanceled || to == StatusCompleted ||
			to == StatusFailed || to == StatusRemoved
	case StatusPaused:
		return to == StatusPending || to == StatusCanceled || to == StatusRemoved
	case StatusCanceled:
		return to == StatusPending || to == StatusRemoved
	case StatusFailed:
		return to == StatusRemoved
	case StatusCompleted:
		return to == StatusRemoved
	}
	return false
}

// startRun prepares a fresh run context. On resume it also replaces the closed
// done channel with a new one.
func (t *Task) startRun(resume bool) {
	t.mu.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	t.ctx = ctx
	t.cancel = cancel
	t.stop = stopNone
	t.runDone = make(chan struct{})
	t.lastErr = ""
	t.lastSampleAt = time.Now()
	t.lastSampleBytes = t.downloaded.Load()
	if resume {
		t.done = make(chan struct{})
		t.doneClosed = false
		t.termErr = nil
		t.speed = 0
		t.eta = 0
	}
	t.mu.Unlock()
}

func (t *Task) finishRun() {
	t.mu.Lock()
	if t.runDone != nil {
		select {
		case <-t.runDone:
		default:
			close(t.runDone)
		}
	}
	t.mu.Unlock()
}

// beginResume atomically checks that the task can resume, installs a fresh run
// context and done channel, and transitions to Pending. Two concurrent Resume
// calls cannot both succeed.
func (t *Task) beginResume() (TaskInfo, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.status != StatusPaused && t.status != StatusCanceled {
		return TaskInfo{}, false
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.ctx = ctx
	t.cancel = cancel
	t.stop = stopNone
	t.runDone = make(chan struct{})
	t.done = make(chan struct{})
	t.doneClosed = false
	t.termErr = nil
	t.lastErr = ""
	t.speed = 0
	t.eta = 0
	t.status = StatusPending
	t.updatedAt = time.Now()
	t.lastSampleAt = time.Now()
	t.lastSampleBytes = t.downloaded.Load()
	return t.infoLocked(), true
}

func (t *Task) context() context.Context {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.ctx
}

func (t *Task) runContext() context.Context {
	return t.context()
}

func (t *Task) runFinished() <-chan struct{} {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.runDone
}

func (t *Task) setStop(reason stopReason) {
	t.mu.Lock()
	t.stop = reason
	t.mu.Unlock()
}

func (t *Task) stopReason() stopReason {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.stop
}

func (t *Task) cancelRun() {
	t.mu.Lock()
	cancel := t.cancel
	t.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (t *Task) markTerminal(err error) {
	t.mu.Lock()
	if !t.doneClosed {
		t.doneClosed = true
		t.termErr = err
		close(t.done)
	}
	t.mu.Unlock()
}

func (t *Task) doneChan() <-chan struct{} {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.done
}

func (t *Task) terminalErr() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.termErr
}

func (t *Task) urlValue() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.url
}

func (t *Task) pathValue() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.path
}

func (t *Task) fileNameValue() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.fileName
}

func (t *Task) statusValue() TaskStatus {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.status
}

func (t *Task) resumeEnabled() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.cfg.resume
}

func (t *Task) retryConfig() (int, time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.cfg.maxRetries, t.cfg.retryBase
}

func (t *Task) userAgentValue() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.cfg.userAgent
}

func (t *Task) priorityValue() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.priority
}

func (t *Task) info() TaskInfo {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.infoLocked()
}

func (t *Task) infoLocked() TaskInfo {
	down := t.downloaded.Load()
	progress := 0.0
	if t.total > 0 {
		progress = float64(down) / float64(t.total)
		if progress > 1 {
			progress = 1
		}
	}
	tags := make(map[string]string, len(t.tags))
	for k, v := range t.tags {
		tags[k] = v
	}
	speed := t.speed
	eta := t.eta
	if t.status != StatusDownloading {
		speed = 0
		eta = 0
	}
	var completed *time.Time
	if t.completedAt != nil {
		c := *t.completedAt
		completed = &c
	}
	return TaskInfo{
		ID:          t.id,
		URL:         t.url,
		FileName:    t.fileName,
		Path:        t.path,
		Status:      t.status,
		Priority:    t.priority,
		Tags:        tags,
		RateLimit:   t.cfg.rateLimit,
		Resume:      t.cfg.resume,
		TotalSize:   t.total,
		Downloaded:  down,
		Progress:    progress,
		Speed:       speed,
		ETA:         eta,
		ChunksTotal: len(t.chunkDone),
		ChunksDone:  t.chunksDone,
		Retries:     t.retries,
		Error:       t.lastErr,
		CreatedAt:   t.createdAt,
		UpdatedAt:   t.updatedAt,
		CompletedAt: completed,
	}
}

func (t *Task) applyProbe(info *probeInfo) {
	t.mu.Lock()
	t.total = info.total
	t.etag = info.etag
	t.lastModified = info.lastModified
	t.rangeSupported = info.supportsRange
	t.updatedAt = time.Now()
	t.mu.Unlock()
}

func (t *Task) applyPlan(chunkSize int64, done []bool) {
	t.mu.Lock()
	t.chunkSize = chunkSize
	t.chunkDone = done
	t.chunksDone = countTrue(done)
	t.mu.Unlock()
}

func (t *Task) setChunkDone(index int) {
	t.mu.Lock()
	if index >= 0 && index < len(t.chunkDone) && !t.chunkDone[index] {
		t.chunkDone[index] = true
		t.chunksDone++
	}
	t.updatedAt = time.Now()
	t.mu.Unlock()
}

func (t *Task) chunkBitmap() []bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]bool, len(t.chunkDone))
	copy(out, t.chunkDone)
	return out
}

func (t *Task) setDownloaded(n int64) {
	t.downloaded.Store(n)
	t.mu.Lock()
	t.lastSampleBytes = n
	t.lastSampleAt = time.Now()
	t.mu.Unlock()
}

func (t *Task) addRetry() {
	t.mu.Lock()
	t.retries++
	t.updatedAt = time.Now()
	t.mu.Unlock()
}

func (t *Task) setError(err error) {
	if err == nil {
		return
	}
	t.mu.Lock()
	t.lastErr = err.Error()
	t.updatedAt = time.Now()
	t.mu.Unlock()
}

func (t *Task) resetDownload() {
	t.downloaded.Store(0)
	t.mu.Lock()
	t.chunkDone = nil
	t.chunksDone = 0
	t.chunkSize = 0
	t.lastSampleBytes = 0
	t.lastSampleAt = time.Now()
	t.mu.Unlock()
}

func (t *Task) setTotal(total int64) {
	t.mu.Lock()
	t.total = total
	t.updatedAt = time.Now()
	t.mu.Unlock()
}

func (t *Task) metaSnapshot() *fileMeta {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.chunkDone) == 0 || t.chunkSize <= 0 {
		return nil
	}
	done := make([]bool, len(t.chunkDone))
	copy(done, t.chunkDone)
	return &fileMeta{
		Version:      metaVersion,
		URL:          t.url,
		ETag:         t.etag,
		LastModified: t.lastModified,
		Total:        t.total,
		ChunkSize:    t.chunkSize,
		Done:         done,
		SavedAt:      time.Now().UTC(),
	}
}

func (t *Task) saveMeta() error {
	if !t.resumeEnabled() {
		return nil
	}
	m := t.metaSnapshot()
	if m == nil {
		return nil
	}
	return saveMetaFile(t.metaPathValue(), m)
}

func (t *Task) metaPathValue() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.metaPath
}

func (t *Task) waitRate(ctx context.Context, n int) error {
	if t.globalLimiter != nil {
		if err := t.globalLimiter.waitN(ctx, n); err != nil {
			return err
		}
	}
	if l := t.limiter.Load(); l != nil {
		if err := l.waitN(ctx, n); err != nil {
			return err
		}
	}
	return nil
}

// sample updates speed/ETA from the byte counter and reports whether a
// progress event is worth publishing.
func (t *Task) sample(now time.Time, alpha float64) (Progress, bool) {
	t.mu.Lock()
	active := t.status == StatusDownloading
	lastAt := t.lastSampleAt
	lastBytes := t.lastSampleBytes
	t.mu.Unlock()
	if !active {
		return Progress{}, false
	}
	down := t.downloaded.Load()
	dt := now.Sub(lastAt).Seconds()
	var instant float64
	if dt > 0 && down >= lastBytes {
		instant = float64(down-lastBytes) / dt
	}
	t.mu.Lock()
	if t.lastSampleAt.Equal(lastAt) {
		if t.speed == 0 {
			t.speed = instant
		} else if dt > 0 {
			t.speed = alpha*instant + (1-alpha)*t.speed
		}
		t.lastSampleAt = now
		t.lastSampleBytes = down
		speed := t.speed
		eta := time.Duration(0)
		if speed > 1 && t.total > down {
			eta = time.Duration(float64(t.total-down) / speed * float64(time.Second))
		}
		t.eta = eta
		p := Progress{TaskID: t.id, Downloaded: down, Total: t.total, Speed: speed, ETA: eta}
		t.mu.Unlock()
		return p, true
	}
	t.mu.Unlock()
	return Progress{}, false
}
