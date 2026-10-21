package download

import (
	"fmt"
	"sync"
	"time"
)

// registry owns the task collection and the resolved-path uniqueness index.
// Lock order is registry.mu before Task.mu; nothing acquires them the other way
// round.
type registry struct {
	mu    sync.RWMutex
	tasks map[TaskID]*Task
	paths map[string]TaskID
	seq   uint64
}

func newRegistry() *registry {
	return &registry{
		tasks: make(map[TaskID]*Task),
		paths: make(map[string]TaskID),
	}
}

// insert assigns an ID and registers the task, claiming its path.
func (r *registry) insert(t *Task) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if id, ok := r.paths[t.pathKey]; ok {
		return &DuplicateTaskError{ID: id, Path: t.path}
	}
	r.seq++
	t.id = TaskID(fmt.Sprintf("task-%06d", r.seq))
	t.seq = r.seq
	t.createdAt = time.Now()
	r.tasks[t.id] = t
	r.paths[t.pathKey] = t.id
	return nil
}

func (r *registry) get(id TaskID) (*Task, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.tasks[id]
	return t, ok
}

func (r *registry) findPath(key string) (TaskID, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	id, ok := r.paths[key]
	return id, ok
}

func (r *registry) all() []*Task {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Task, 0, len(r.tasks))
	for _, t := range r.tasks {
		out = append(out, t)
	}
	return out
}

func (r *registry) infos() []TaskInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]TaskInfo, 0, len(r.tasks))
	for _, t := range r.tasks {
		out = append(out, t.info())
	}
	return out
}

func (r *registry) releasePath(key string, id TaskID) {
	r.mu.Lock()
	if cur, ok := r.paths[key]; ok && cur == id {
		delete(r.paths, key)
	}
	r.mu.Unlock()
}

// claimPath reserves a path for an existing task (used by Resume).
func (r *registry) claimPath(key, path string, id TaskID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.tasks[id]; !ok {
		return ErrTaskNotFound
	}
	if cur, ok := r.paths[key]; ok && cur != id {
		return &DuplicateTaskError{ID: cur, Path: path}
	}
	r.paths[key] = id
	return nil
}

// remove deletes the task record and returns its final snapshot.
func (r *registry) remove(id TaskID) (TaskInfo, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	t, ok := r.tasks[id]
	if !ok {
		return TaskInfo{}, false
	}
	delete(r.tasks, id)
	t.mu.Lock()
	key := t.pathKey
	t.mu.Unlock()
	if cur, ok := r.paths[key]; ok && cur == id {
		delete(r.paths, key)
	}
	return t.info(), true
}

// rekey atomically moves a task's path claim. The caller must already have
// updated the task's path fields while holding both locks.
func (r *registry) rekey(oldKey, newKey string, id TaskID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if cur, ok := r.paths[newKey]; ok && cur != id {
		return &DuplicateTaskError{ID: cur, Path: newKey}
	}
	if cur, ok := r.paths[oldKey]; ok && cur == id {
		delete(r.paths, oldKey)
	}
	if _, ok := r.tasks[id]; !ok {
		return ErrTaskNotFound
	}
	r.paths[newKey] = id
	return nil
}

func (r *registry) counts() map[TaskStatus]int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	counts := make(map[TaskStatus]int, 8)
	for _, t := range r.tasks {
		counts[t.info().Status]++
	}
	return counts
}
