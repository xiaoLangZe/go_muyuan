package download

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
)

// TaskOption configures one task. Options are applied in order, so the last one
// wins; nil options are ignored.
type TaskOption func(*taskConfig) error

// WithFileName sets the target file name explicitly instead of deriving it from
// the URL path.
func WithFileName(name string) TaskOption {
	return func(c *taskConfig) error {
		if err := validateFileName(name); err != nil {
			return err
		}
		c.fileName = name
		return nil
	}
}

// WithPriority sets the task priority. Higher values are admitted first when
// WithMaxConcurrentTasks is in use.
func WithPriority(p int) TaskOption {
	return func(c *taskConfig) error {
		c.priority = p
		return nil
	}
}

// WithTaskRateLimit sets a per-task bandwidth cap in bytes per second. 0 means
// unlimited.
func WithTaskRateLimit(bytesPerSecond int64) TaskOption {
	return func(c *taskConfig) error {
		if bytesPerSecond < 0 {
			return fmt.Errorf("%w: rate limit must not be negative", ErrInvalidOption)
		}
		c.rateLimit = bytesPerSecond
		return nil
	}
}

// WithTaskResume overrides resume support for this task.
func WithTaskResume(enabled bool) TaskOption {
	return func(c *taskConfig) error {
		c.resume = enabled
		return nil
	}
}

// WithTaskUserAgent overrides the Role User-Agent for this task.
func WithTaskUserAgent(ua string) TaskOption {
	return func(c *taskConfig) error {
		c.userAgent = ua
		return nil
	}
}

// WithTags attaches arbitrary tags used by List filters and snapshots.
func WithTags(tags map[string]string) TaskOption {
	return func(c *taskConfig) error {
		copied := make(map[string]string, len(tags))
		for k, v := range tags {
			copied[k] = v
		}
		c.tags = copied
		return nil
	}
}

// WithIdempotent makes Add return the existing task ID instead of an error when
// an active task already owns the resolved path.
func WithIdempotent(enabled bool) TaskOption {
	return func(c *taskConfig) error {
		c.idempotent = enabled
		return nil
	}
}

// WithOverwritePolicy controls behavior when the target file already exists.
func WithOverwritePolicy(p OverwritePolicy) TaskOption {
	return func(c *taskConfig) error {
		switch p {
		case OverwriteError, OverwriteSkip, OverwriteReplace, OverwriteResume:
		default:
			return fmt.Errorf("%w: unknown overwrite policy %q", ErrInvalidOption, p)
		}
		c.overwrite = p
		return nil
	}
}

func (r *Role) taskDefaults() taskConfig {
	return taskConfig{
		resume:     r.cfg.resume,
		maxRetries: r.cfg.maxRetries,
		retryBase:  r.cfg.retryBase,
		overwrite:  OverwriteError,
	}
}

// fileNameFromURL derives the target name from the last path segment.
func fileNameFromURL(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrInvalidURL, err)
	}
	p := u.EscapedPath()
	if p == "" || strings.HasSuffix(p, "/") {
		return "", fmt.Errorf("%w: url has no file name; use WithFileName", ErrInvalidFileName)
	}
	last := p[strings.LastIndexByte(p, '/')+1:]
	name, err := url.PathUnescape(last)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrInvalidFileName, err)
	}
	if err := validateFileName(name); err != nil {
		return "", err
	}
	return name, nil
}

// Add registers a download of fileURL into saveDir (a directory) and returns
// immediately. It performs only local validation: no network and no bulk IO.
//
// Runtime failures (404, disk full, connection loss) are observable through
// Get, List, Snapshot and events.
func (r *Role) Add(fileURL, saveDir string) (TaskID, error) {
	return r.AddWithOptions(fileURL, saveDir)
}

// AddWithOptions is Add with per-task options.
func (r *Role) AddWithOptions(fileURL, saveDir string, opts ...TaskOption) (TaskID, error) {
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	if r.closed.Load() {
		return "", ErrRoleClosed
	}
	if _, err := validateURL(fileURL, r.cfg.allowPrivate); err != nil {
		return "", err
	}
	cfg := r.taskDefaults()
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(&cfg); err != nil {
			return "", err
		}
	}
	name := cfg.fileName
	if name == "" {
		var err error
		name, err = fileNameFromURL(fileURL)
		if err != nil {
			return "", err
		}
	}
	path, err := resolveTarget(r.cfg.rootDir, saveDir, name, r.cfg.strictRoot)
	if err != nil {
		return "", err
	}
	key := pathKey(path)

	if existing, ok := r.reg.findPath(key); ok {
		if cfg.idempotent {
			return existing, nil
		}
		return "", &DuplicateTaskError{ID: existing, Path: path}
	}
	if _, err := os.Stat(path); err == nil {
		switch cfg.overwrite {
		case OverwriteReplace:
			removeMetaFile(path + ".dltmp.json")
		case OverwriteResume:
			// Keep both the file and the sidecar.
		default:
			return "", fmt.Errorf("%w: %s", ErrFileExists, path)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}

	t := newTask(fileURL, saveDir, name, path, cfg, r)
	if err := r.reg.insert(t); err != nil {
		var dup *DuplicateTaskError
		if errors.As(err, &dup) && cfg.idempotent {
			return dup.ID, nil
		}
		return "", err
	}
	t.startRun(false)
	info := t.info()
	r.hub.publish(EventTaskAdded, t.id, info)
	r.startTask(t)
	return t.id, nil
}
