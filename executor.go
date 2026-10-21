package download

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// workerHandle is the registration for one dynamically managed worker.
type workerHandle struct {
	quit chan struct{}
}

// admissionWaiter is a coordinator queued for a concurrent-task slot.
type admissionWaiter struct {
	task     *Task
	admit    chan struct{}
	seq      uint64
	canceled bool
}

type execStats struct {
	workers      int
	busy         int
	queuedTasks  int
	queuedChunks int
}

// executor is the shared global worker pool. Coordinators feed chunk jobs into
// one queue; the number of workers is the total download concurrency for the
// whole Role.
type executor struct {
	mu      sync.Mutex
	workers map[*workerHandle]struct{}
	closed  bool
	jobs    chan *chunkJob
	quit    chan struct{}
	busy    atomic.Int64

	waiters       []*admissionWaiter
	running       int
	maxConcurrent int
	wake          chan struct{}
	waitSeq       atomic.Uint64

	workerWG sync.WaitGroup
	schedWG  sync.WaitGroup
}

func newExecutor(threads, maxConcurrent int) *executor {
	e := &executor{
		workers:       make(map[*workerHandle]struct{}),
		jobs:          make(chan *chunkJob, max(256, threads*8)),
		quit:          make(chan struct{}),
		maxConcurrent: maxConcurrent,
		wake:          make(chan struct{}, 1),
	}
	e.mu.Lock()
	e.addWorkersLocked(threads)
	e.mu.Unlock()
	e.schedWG.Add(1)
	go e.schedule()
	return e
}

func (e *executor) addWorkersLocked(n int) {
	for i := 0; i < n; i++ {
		h := &workerHandle{quit: make(chan struct{})}
		e.workers[h] = struct{}{}
		e.workerWG.Add(1)
		go e.worker(h)
	}
}

func (e *executor) worker(h *workerHandle) {
	defer e.workerWG.Done()
	for {
		select {
		case <-h.quit:
			return
		case <-e.quit:
			return
		case job := <-e.jobs:
			e.busy.Add(1)
			job.run()
			e.busy.Add(-1)
		}
	}
}

func (e *executor) add(n int) error {
	if n < 1 {
		return fmt.Errorf("%w: worker count must be at least 1", ErrInvalidOption)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return ErrRoleClosed
	}
	e.addWorkersLocked(n)
	return nil
}

func (e *executor) remove(n int) error {
	if n < 1 {
		return fmt.Errorf("%w: worker count must be at least 1", ErrInvalidOption)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return ErrRoleClosed
	}
	if len(e.workers)-n < 1 {
		return fmt.Errorf("%w: at least one worker must remain", ErrInvalidOption)
	}
	for i := 0; i < n; i++ {
		for h := range e.workers {
			delete(e.workers, h)
			close(h.quit)
			break
		}
	}
	return nil
}

func (e *executor) setThreads(n int) error {
	if n < 1 {
		return fmt.Errorf("%w: threads must be at least 1", ErrInvalidOption)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return ErrRoleClosed
	}
	cur := len(e.workers)
	switch {
	case n > cur:
		e.addWorkersLocked(n - cur)
	case n < cur:
		for i := 0; i < cur-n; i++ {
			for h := range e.workers {
				delete(e.workers, h)
				close(h.quit)
				break
			}
		}
	}
	return nil
}

func (e *executor) stop() {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return
	}
	e.closed = true
	close(e.quit)
	e.mu.Unlock()
	e.workerWG.Wait()
	e.schedWG.Wait()
	e.mu.Lock()
	e.workers = make(map[*workerHandle]struct{})
	e.mu.Unlock()
}

func (e *executor) snapshot() execStats {
	e.mu.Lock()
	st := execStats{
		workers:     len(e.workers),
		queuedTasks: len(e.waiters),
	}
	e.mu.Unlock()
	st.busy = int(e.busy.Load())
	st.queuedChunks = len(e.jobs)
	return st
}

func (e *executor) signal() {
	select {
	case e.wake <- struct{}{}:
	default:
	}
}

// acquire waits for a concurrent-task slot. It returns false when ctx is
// canceled or the pool is shutting down.
func (e *executor) acquire(ctx context.Context, t *Task) bool {
	if ctx.Err() != nil {
		return false
	}
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return false
	}
	if e.maxConcurrent <= 0 {
		e.running++
		e.mu.Unlock()
		return true
	}
	w := &admissionWaiter{
		task:  t,
		admit: make(chan struct{}),
		seq:   e.waitSeq.Add(1),
	}
	e.waiters = append(e.waiters, w)
	e.mu.Unlock()
	e.signal()

	select {
	case <-w.admit:
		return true
	case <-ctx.Done():
		e.mu.Lock()
		w.canceled = true
		e.mu.Unlock()
		e.signal()
		return false
	case <-e.quit:
		return false
	}
}

func (e *executor) release() {
	e.mu.Lock()
	if e.running > 0 {
		e.running--
	}
	e.mu.Unlock()
	e.signal()
}

func (e *executor) schedule() {
	defer e.schedWG.Done()
	for {
		select {
		case <-e.quit:
			return
		case <-e.wake:
		}
		for {
			e.mu.Lock()
			if e.closed {
				e.mu.Unlock()
				return
			}
			if e.maxConcurrent > 0 && e.running >= e.maxConcurrent {
				e.mu.Unlock()
				break
			}
			idx := e.bestWaiterLocked()
			if idx < 0 {
				e.mu.Unlock()
				break
			}
			w := e.waiters[idx]
			e.waiters = append(e.waiters[:idx], e.waiters[idx+1:]...)
			e.running++
			e.mu.Unlock()
			close(w.admit)
		}
	}
}

// bestWaiterLocked picks the highest priority waiter, breaking ties by arrival
// order. Canceled waiters are pruned.
func (e *executor) bestWaiterLocked() int {
	best := -1
	for i := 0; i < len(e.waiters); i++ {
		w := e.waiters[i]
		if w.canceled {
			e.waiters = append(e.waiters[:i], e.waiters[i+1:]...)
			i--
			continue
		}
		if best < 0 {
			best = i
			continue
		}
		b := e.waiters[best]
		if w.task.priorityValue() > b.task.priorityValue() ||
			(w.task.priorityValue() == b.task.priorityValue() && w.seq < b.seq) {
			best = i
		}
	}
	return best
}

func (e *executor) submit(ctx context.Context, job *chunkJob) bool {
	select {
	case e.jobs <- job:
		return true
	case <-ctx.Done():
		return false
	case <-e.quit:
		return false
	}
}
