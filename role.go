package download

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// roleConfig is the resolved Role configuration.
type roleConfig struct {
	threads          int
	userAgent        string
	proxy            string
	proxyFromEnv     bool
	rootDir          string
	strictRoot       bool
	maxRetries       int
	retryBase        time.Duration
	globalRate       int64
	resume           bool
	maxConcurrent    int
	replayBuffer     int
	progressInterval time.Duration
	allowPrivate     bool
	client           *http.Client
}

func defaultRoleConfig() roleConfig {
	return roleConfig{
		threads:          4,
		strictRoot:       true,
		maxRetries:       3,
		retryBase:        500 * time.Millisecond,
		resume:           true,
		progressInterval: 500 * time.Millisecond,
	}
}

// Option configures a Role.
type Option func(*roleConfig) error

// WithThreads sets the number of global download slots. It must be at least 1;
// the pool can be resized later with AddWorker, RemoveWorker and SetThreads.
func WithThreads(n int) Option {
	return func(c *roleConfig) error {
		if n < 1 {
			return fmt.Errorf("%w: threads must be at least 1", ErrInvalidOption)
		}
		c.threads = n
		return nil
	}
}

// WithUserAgent sets the User-Agent sent with every request, including probes
// and redirects. A per-task override wins.
func WithUserAgent(ua string) Option {
	return func(c *roleConfig) error {
		c.userAgent = ua
		return nil
	}
}

// WithProxy routes all requests through the given proxy URL.
func WithProxy(proxyURL string) Option {
	return func(c *roleConfig) error {
		c.proxy = proxyURL
		return nil
	}
}

// WithProxyFromEnvironment uses HTTP_PROXY, HTTPS_PROXY and NO_PROXY.
func WithProxyFromEnvironment() Option {
	return func(c *roleConfig) error {
		c.proxyFromEnv = true
		return nil
	}
}

// WithRootDir confines relative save directories (and, with strict mode,
// absolute ones) to dir. The directory is created if needed and checked for
// writability during New.
func WithRootDir(dir string) Option {
	return func(c *roleConfig) error {
		if dir == "" {
			return fmt.Errorf("%w: root dir must not be empty", ErrInvalidOption)
		}
		c.rootDir = dir
		return nil
	}
}

// WithStrictRoot controls whether an absolute saveDir outside rootDir is
// rejected. It defaults to true and has no effect without WithRootDir.
func WithStrictRoot(strict bool) Option {
	return func(c *roleConfig) error {
		c.strictRoot = strict
		return nil
	}
}

// WithMaxRetries sets the number of retries after the first attempt for chunk
// and probe failures. 0 disables retries.
func WithMaxRetries(n int) Option {
	return func(c *roleConfig) error {
		if n < 0 {
			return fmt.Errorf("%w: retries must not be negative", ErrInvalidOption)
		}
		c.maxRetries = n
		return nil
	}
}

// WithRetryBase sets the base delay for exponential backoff.
func WithRetryBase(d time.Duration) Option {
	return func(c *roleConfig) error {
		if d <= 0 {
			return fmt.Errorf("%w: retry base must be positive", ErrInvalidOption)
		}
		c.retryBase = d
		return nil
	}
}

// WithGlobalRateLimit caps the aggregate download bandwidth in bytes per
// second. 0 means unlimited.
func WithGlobalRateLimit(bytesPerSecond int64) Option {
	return func(c *roleConfig) error {
		if bytesPerSecond < 0 {
			return fmt.Errorf("%w: rate limit must not be negative", ErrInvalidOption)
		}
		c.globalRate = bytesPerSecond
		return nil
	}
}

// WithResume enables sidecar-based resumption for new tasks. It defaults to
// true; individual tasks can override it.
func WithResume(enabled bool) Option {
	return func(c *roleConfig) error {
		c.resume = enabled
		return nil
	}
}

// WithMaxConcurrentTasks limits how many files download at once. Tasks over the
// limit stay Pending. 0 (the default) means unlimited.
func WithMaxConcurrentTasks(n int) Option {
	return func(c *roleConfig) error {
		if n < 0 {
			return fmt.Errorf("%w: max concurrent tasks must not be negative", ErrInvalidOption)
		}
		c.maxConcurrent = n
		return nil
	}
}

// WithReplayBuffer keeps the last n events so WithSince can resume a stream.
// 0 (the default) disables replay.
func WithReplayBuffer(n int) Option {
	return func(c *roleConfig) error {
		if n < 0 {
			return fmt.Errorf("%w: replay buffer must not be negative", ErrInvalidOption)
		}
		c.replayBuffer = n
		return nil
	}
}

// WithProgressInterval sets how often progress is sampled. Default 500ms.
func WithProgressInterval(d time.Duration) Option {
	return func(c *roleConfig) error {
		if d <= 0 {
			return fmt.Errorf("%w: progress interval must be positive", ErrInvalidOption)
		}
		c.progressInterval = d
		return nil
	}
}

// WithAllowPrivateHosts permits requests to loopback, private and reserved
// addresses. It is off by default, which protects against SSRF; enable it for
// intranet downloads and tests.
func WithAllowPrivateHosts(allow bool) Option {
	return func(c *roleConfig) error {
		c.allowPrivate = allow
		return nil
	}
}

// WithHTTPClient uses a caller-supplied client. Its transport is wrapped for
// User-Agent injection (and the private-address guard when possible).
func WithHTTPClient(hc *http.Client) Option {
	return func(c *roleConfig) error {
		if hc == nil {
			return fmt.Errorf("%w: nil http client", ErrInvalidOption)
		}
		c.client = hc
		return nil
	}
}

// Role is a long-lived download manager: configuration, task registry, worker
// pool and event hub. It is safe for concurrent use.
type Role struct {
	cfg           roleConfig
	client        *http.Client
	reg           *registry
	exec          *executor
	hub           *hub
	globalLimiter *limiter
	closed        atomic.Bool
	lifecycleMu   sync.Mutex
	closeOnce     sync.Once
	closing       chan struct{}
	wg            sync.WaitGroup // coordinator goroutines
	samplerWG     sync.WaitGroup
}

// New constructs a Role and validates its configuration, including proxy syntax
// and rootDir writability.
func New(opts ...Option) (*Role, error) {
	cfg := defaultRoleConfig()
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(&cfg); err != nil {
			return nil, err
		}
	}
	if cfg.proxy != "" && cfg.proxyFromEnv {
		return nil, fmt.Errorf("%w: proxy url and proxy-from-environment are mutually exclusive", ErrInvalidOption)
	}
	if cfg.rootDir != "" {
		abs, err := filepath.Abs(cfg.rootDir)
		if err != nil {
			return nil, fmt.Errorf("%w: root dir: %v", ErrInvalidOption, err)
		}
		cfg.rootDir = filepath.Clean(abs)
		if err := ensureWritableDir(cfg.rootDir); err != nil {
			return nil, err
		}
	}
	client := cfg.client
	if client != nil {
		client = wrapCustomClient(client, &cfg)
	} else {
		c, err := newHTTPClient(&cfg)
		if err != nil {
			return nil, err
		}
		client = c
	}
	r := &Role{
		cfg:     cfg,
		client:  client,
		reg:     newRegistry(),
		hub:     newHub(cfg.replayBuffer),
		closing: make(chan struct{}),
	}
	if cfg.globalRate > 0 {
		r.globalLimiter = newLimiter(cfg.globalRate)
	}
	r.exec = newExecutor(cfg.threads, cfg.maxConcurrent)
	r.startSampler()
	return r, nil
}

func ensureWritableDir(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("%w: root dir: %v", ErrInvalidOption, err)
	}
	f, err := os.CreateTemp(dir, ".download-writecheck-*")
	if err != nil {
		return fmt.Errorf("%w: root dir is not writable: %v", ErrInvalidOption, err)
	}
	name := f.Name()
	f.Close()
	_ = os.Remove(name)
	return nil
}

// Close stops all active tasks (they transition to Canceled), stops the worker
// pool and closes every subscription channel. Sidecars are kept so a later
// process can resume. Query methods remain usable afterwards.
func (r *Role) Close() error {
	r.closeOnce.Do(func() {
		// Holding lifecycleMu across the flag change means any Add or Resume
		// that already passed its check has registered its goroutine before
		// wg.Wait runs below.
		r.lifecycleMu.Lock()
		r.closed.Store(true)
		close(r.closing)
		r.lifecycleMu.Unlock()

		// Cancel every non-terminal task, including Paused ones, so nothing
		// remains waiting forever after Close.
		for _, t := range r.reg.all() {
			if !t.statusValue().Terminal() {
				_ = r.Cancel(t.id)
			}
		}
		r.wg.Wait()
		r.samplerWG.Wait()
		r.exec.stop()
		r.hub.close()
		r.client.CloseIdleConnections()
	})
	return nil
}

func (r *Role) startTask(t *Task) {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		r.runTask(t)
	}()
}

// AddWorker starts n additional download workers.
func (r *Role) AddWorker(n int) error {
	if r.closed.Load() {
		return ErrRoleClosed
	}
	if err := r.exec.add(n); err != nil {
		return err
	}
	r.workerEvent()
	return nil
}

// RemoveWorker gracefully retires n workers: each finishes its current chunk
// and exits. At least one worker always remains.
func (r *Role) RemoveWorker(n int) error {
	if r.closed.Load() {
		return ErrRoleClosed
	}
	if err := r.exec.remove(n); err != nil {
		return err
	}
	r.workerEvent()
	return nil
}

// SetThreads resizes the worker pool to n.
func (r *Role) SetThreads(n int) error {
	if r.closed.Load() {
		return ErrRoleClosed
	}
	if err := r.exec.setThreads(n); err != nil {
		return err
	}
	r.workerEvent()
	return nil
}

// NumWorkers reports the current worker count.
func (r *Role) NumWorkers() int { return r.exec.snapshot().workers }

func (r *Role) workerEvent() {
	st := r.exec.snapshot()
	r.hub.publish(EventWorkerChange, "", WorkerChange{Workers: st.workers, Busy: st.busy})
}

// setStatus performs a validated transition and publishes it.
func (r *Role) setStatus(t *Task, to TaskStatus, err error) bool {
	t.mu.Lock()
	if t.status == to {
		if err != nil {
			t.lastErr = err.Error()
		}
		t.mu.Unlock()
		return false
	}
	if !canTransition(t.status, to) {
		t.mu.Unlock()
		return false
	}
	t.status = to
	t.updatedAt = time.Now()
	if to == StatusCompleted {
		now := t.updatedAt
		t.completedAt = &now
	}
	if err != nil {
		t.lastErr = err.Error()
	}
	info := t.infoLocked()
	t.mu.Unlock()
	r.hub.publish(EventTaskState, t.id, info)
	return true
}

func (r *Role) releasePath(t *Task) {
	t.mu.Lock()
	key, id := t.pathKey, t.id
	t.mu.Unlock()
	r.reg.releasePath(key, id)
}

// Pause stops a task, keeps its sidecar, and leaves it queryable. A Paused task
// can be resumed with Resume.
func (r *Role) Pause(id TaskID) error {
	t, ok := r.reg.get(id)
	if !ok {
		return ErrTaskNotFound
	}
	t.mu.Lock()
	switch t.status {
	case StatusPaused:
		t.mu.Unlock()
		return nil
	case StatusPending, StatusProbing, StatusDownloading:
	default:
		st := t.status
		t.mu.Unlock()
		return &InvalidStateError{ID: id, From: st, Op: "pause"}
	}
	t.stop = stopPause
	t.status = StatusPaused
	t.updatedAt = time.Now()
	cancel := t.cancel
	info := t.infoLocked()
	t.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	r.hub.publish(EventTaskState, id, info)
	return nil
}

// Cancel stops a task permanently. It can still be resumed because the sidecar
// is retained until Remove(..., true).
func (r *Role) Cancel(id TaskID) error {
	t, ok := r.reg.get(id)
	if !ok {
		return ErrTaskNotFound
	}
	t.mu.Lock()
	switch t.status {
	case StatusCompleted, StatusCanceled, StatusFailed, StatusRemoved:
		t.mu.Unlock()
		return nil
	}
	t.stop = stopCancel
	t.status = StatusCanceled
	t.updatedAt = time.Now()
	cancel := t.cancel
	info := t.infoLocked()
	t.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	r.hub.publish(EventTaskState, id, info)
	t.markTerminal(ErrTaskCanceled)
	r.releasePath(t)
	return nil
}

// Resume restarts a Paused or Canceled task. It re-probes the server and only
// schedules chunks that are not already complete.
func (r *Role) Resume(id TaskID) error {
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	if r.closed.Load() {
		return ErrRoleClosed
	}
	t, ok := r.reg.get(id)
	if !ok {
		return ErrTaskNotFound
	}
	t.mu.Lock()
	status := t.status
	key := t.pathKey
	path := t.path
	t.mu.Unlock()
	if status != StatusPaused && status != StatusCanceled {
		return &InvalidStateError{ID: id, From: status, Op: "resume"}
	}
	// Let the previous coordinator finish before a new run starts.
	if done := t.runFinished(); done != nil {
		<-done
	}
	owned := false
	if cur, ok := r.reg.findPath(key); ok && cur == id {
		owned = true
	}
	if !owned {
		if err := r.reg.claimPath(key, path, id); err != nil {
			return err
		}
	}
	info, ok := t.beginResume()
	if !ok {
		if !owned {
			r.reg.releasePath(key, id)
		}
		t.mu.Lock()
		st := t.status
		t.mu.Unlock()
		return &InvalidStateError{ID: id, From: st, Op: "resume"}
	}
	r.hub.publish(EventTaskState, id, info)
	r.startTask(t)
	return nil
}

// Remove cancels the task if needed, waits for its coordinator to exit and
// deletes the record. With cleanup it also deletes the sidecar and any partial
// file (a completed file is kept).
func (r *Role) Remove(id TaskID, cleanup bool) error {
	t, ok := r.reg.get(id)
	if !ok {
		return ErrTaskNotFound
	}
	if err := r.Cancel(id); err != nil && !errors.Is(err, ErrTaskNotFound) {
		return err
	}
	if done := t.runFinished(); done != nil {
		<-done
	}
	info, ok := r.reg.remove(id)
	if !ok {
		return ErrTaskNotFound
	}
	r.hub.publish(EventTaskRemoved, id, RemovedPayload{ID: id, Path: info.Path})
	if cleanup {
		removeMetaFile(info.Path + ".dltmp.json")
		if info.Status != StatusCompleted {
			_ = os.Remove(info.Path)
		}
	}
	return nil
}

// TaskPatch is the whitelist of runtime task edits accepted by Update.
type TaskPatch struct {
	// Priority may be changed in any state; it affects admission order while
	// Pending.
	Priority *int
	// RateLimit changes the per-task limit in bytes per second in any state.
	// 0 means unlimited.
	RateLimit *int64
	// URL may only change while Pending.
	URL *string
	// SaveDir may only change while Pending.
	SaveDir *string
	// FileName may only change while Pending.
	FileName *string
}

// Update applies a whitelisted edit. Illegal edits are rejected rather than
// silently ignored.
func (r *Role) Update(id TaskID, p TaskPatch) error {
	if r.closed.Load() {
		return ErrRoleClosed
	}
	t, ok := r.reg.get(id)
	if !ok {
		return ErrTaskNotFound
	}
	if p.Priority != nil {
		t.mu.Lock()
		t.priority = *p.Priority
		t.updatedAt = time.Now()
		info := t.infoLocked()
		t.mu.Unlock()
		r.hub.publish(EventTaskState, id, info)
		r.exec.signal()
	}
	if p.RateLimit != nil {
		if *p.RateLimit < 0 {
			return fmt.Errorf("%w: rate limit must not be negative", ErrInvalidPatch)
		}
		t.mu.Lock()
		t.cfg.rateLimit = *p.RateLimit
		t.updatedAt = time.Now()
		info := t.infoLocked()
		t.mu.Unlock()
		if *p.RateLimit > 0 {
			t.limiter.Store(newLimiter(*p.RateLimit))
		} else {
			t.limiter.Store(nil)
		}
		r.hub.publish(EventTaskState, id, info)
	}
	if p.URL == nil && p.SaveDir == nil && p.FileName == nil {
		return nil
	}

	t.mu.Lock()
	status := t.status
	oldKey := t.pathKey
	curURL := t.url
	curDir := t.saveDir
	curName := t.fileName
	t.mu.Unlock()
	if status != StatusPending {
		return &InvalidStateError{ID: id, From: status, Op: "update path"}
	}

	newURL := curURL
	if p.URL != nil {
		if _, err := validateURL(*p.URL, r.cfg.allowPrivate); err != nil {
			return err
		}
		newURL = *p.URL
	}
	newName := curName
	if p.FileName != nil {
		newName = *p.FileName
	} else if p.URL != nil {
		name, err := fileNameFromURL(newURL)
		if err != nil {
			return err
		}
		newName = name
	}
	newDir := curDir
	if p.SaveDir != nil {
		newDir = *p.SaveDir
	}
	newPath, err := resolveTarget(r.cfg.rootDir, newDir, newName, r.cfg.strictRoot)
	if err != nil {
		return err
	}
	newKey := pathKey(newPath)

	r.reg.mu.Lock()
	t.mu.Lock()
	if t.status != StatusPending {
		st := t.status
		t.mu.Unlock()
		r.reg.mu.Unlock()
		return &InvalidStateError{ID: id, From: st, Op: "update path"}
	}
	oldKey = t.pathKey
	if newKey != oldKey {
		if cur, ok := r.reg.paths[newKey]; ok && cur != id {
			t.mu.Unlock()
			r.reg.mu.Unlock()
			return &DuplicateTaskError{ID: cur, Path: newPath}
		}
		delete(r.reg.paths, oldKey)
		r.reg.paths[newKey] = id
	}
	t.url = newURL
	t.saveDir = newDir
	t.fileName = newName
	t.path = newPath
	t.pathKey = newKey
	t.metaPath = newPath + ".dltmp.json"
	t.updatedAt = time.Now()
	info := t.infoLocked()
	t.mu.Unlock()
	r.reg.mu.Unlock()
	r.hub.publish(EventTaskState, id, info)
	return nil
}

// Get returns one task snapshot.
func (r *Role) Get(id TaskID) (TaskInfo, bool) {
	t, ok := r.reg.get(id)
	if !ok {
		return TaskInfo{}, false
	}
	return t.info(), true
}

// ListAll returns every task, unfiltered and unpaginated.
func (r *Role) ListAll() []TaskInfo {
	out, _ := r.List(ListFilter{})
	return out
}

// List returns tasks after filtering, sorting and pagination. Invalid filter
// values return an error; a filter that matches nothing returns an empty slice.
func (r *Role) List(filter ListFilter) ([]TaskInfo, error) {
	return filterInfos(r.reg.infos(), filter)
}

func (r *Role) startSampler() {
	interval := r.cfg.progressInterval
	r.samplerWG.Add(1)
	go func() {
		defer r.samplerWG.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-r.closing:
				return
			case now := <-ticker.C:
				r.sampleProgress(now)
			}
		}
	}()
}

// Wait blocks until the task reaches a terminal state (Completed, Failed,
// Canceled or Removed). ctx only controls the wait; it does not stop the task.
func (r *Role) Wait(ctx context.Context, id TaskID) error {
	t, ok := r.reg.get(id)
	if !ok {
		return ErrTaskNotFound
	}
	done := t.doneChan()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
	}
	if err := t.terminalErr(); err != nil {
		return err
	}
	if info, ok := r.Get(id); ok && info.Status == StatusFailed {
		if info.Error != "" {
			return errors.New(info.Error)
		}
		return fmt.Errorf("download: task %s failed", id)
	}
	return nil
}

// Download is Add followed by Wait. The ctx only bounds the wait.
func (r *Role) Download(ctx context.Context, fileURL, saveDir string) error {
	id, err := r.Add(fileURL, saveDir)
	if err != nil {
		return err
	}
	return r.Wait(ctx, id)
}
