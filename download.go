package download

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const copyBufferSize = 32 * 1024

type chunkResult struct {
	index int
	err   error
}

// chunkJob is one ranged fetch executed by a worker.
type chunkJob struct {
	task         *Task
	file         *os.File
	chunk        chunkSpec
	etag         string
	lastModified string
	results      chan<- chunkResult
	wg           *sync.WaitGroup
	ctx          context.Context
}

func (j *chunkJob) run() {
	defer j.wg.Done()
	err := j.task.downloadChunk(j.ctx, j.file, j.chunk, j.etag, j.lastModified)
	j.results <- chunkResult{index: j.chunk.index, err: err}
}

// runTask is the per-task coordinator. It occupies no download slot while it
// waits; only chunk jobs run on workers.
func (r *Role) runTask(t *Task) {
	defer t.finishRun()
	defer func() {
		if p := recover(); p != nil {
			err := fmt.Errorf("download: internal error: %v", p)
			t.setError(err)
			r.setStatus(t, StatusFailed, err)
			t.markTerminal(err)
			r.releasePath(t)
		}
	}()
	ctx := t.runContext()
	defer func() {
		t.mu.Lock()
		cancel := t.cancel
		t.mu.Unlock()
		if cancel != nil {
			cancel()
		}
	}()

	if !r.exec.acquire(ctx, t) {
		r.settleStop(t)
		return
	}
	defer r.exec.release()

	err := r.performTask(ctx, t)
	if ctx.Err() != nil {
		r.settleStop(t)
		return
	}
	if err != nil {
		t.setError(err)
		r.setStatus(t, StatusFailed, err)
		t.markTerminal(err)
		r.releasePath(t)
		return
	}
	if !r.setStatus(t, StatusCompleted, nil) {
		// Paused or canceled between the last write and completion.
		r.settleStop(t)
		return
	}
	t.markTerminal(nil)
	r.releasePath(t)
}

// settleStop classifies why the run context ended and applies the matching
// terminal/paused state.
func (r *Role) settleStop(t *Task) {
	switch t.stopReason() {
	case stopPause:
		r.setStatus(t, StatusPaused, nil)
	case stopCancel:
		r.setStatus(t, StatusCanceled, nil)
		t.markTerminal(ErrTaskCanceled)
		r.releasePath(t)
	default:
		r.setStatus(t, StatusCanceled, nil)
		t.markTerminal(ErrTaskCanceled)
		r.releasePath(t)
	}
}

func (r *Role) performTask(ctx context.Context, t *Task) error {
	if !r.setStatus(t, StatusProbing, nil) {
		return ctx.Err()
	}
	info, err := r.probeWithRetry(ctx, t)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	t.applyProbe(info)

	if info.total == 0 && info.supportsRange {
		return r.finishEmptyTask(t)
	}
	if !info.supportsRange {
		return r.downloadSequential(ctx, t, info)
	}

	restarted := false
	for {
		err := r.downloadRange(ctx, t, info)
		if errors.Is(err, errRangeChanged) && !restarted && ctx.Err() == nil {
			// The remote file changed during a resumed transfer: start over
			// with a fresh probe rather than mixing versions.
			restarted = true
			t.resetDownload()
			removeMetaFile(t.metaPathValue())
			info, err = r.probeWithRetry(ctx, t)
			if err != nil {
				return err
			}
			t.applyProbe(info)
			if info.total == 0 && info.supportsRange {
				return r.finishEmptyTask(t)
			}
			if !info.supportsRange {
				return r.downloadSequential(ctx, t, info)
			}
			continue
		}
		return err
	}
}

func (r *Role) finishEmptyTask(t *Task) error {
	path := t.pathValue()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	removeMetaFile(t.metaPathValue())
	t.setDownloaded(0)
	return nil
}

// downloadRange plans chunks (or reuses a sidecar) and dispatches every
// outstanding chunk to the shared pool.
func (r *Role) downloadRange(ctx context.Context, t *Task, info *probeInfo) error {
	chunkSize := planChunks(info.total)
	chunks := buildChunks(info.total, chunkSize)
	if len(chunks) == 0 {
		return r.finishEmptyTask(t)
	}

	done := make([]bool, len(chunks))
	resumeUsed := false
	path := t.pathValue()
	if t.resumeEnabled() {
		if m := loadMetaFile(t.metaPathValue()); m != nil &&
			validateMeta(m, t.urlValue(), info.total, chunkSize) == nil {
			// A sidecar without a matching target file cannot be trusted: the
			// "completed" chunks may not exist on disk.
			if fi, err := os.Stat(path); err == nil && fi.Size() == info.total {
				done = m.Done
				resumeUsed = true
			}
		}
	}
	if !resumeUsed {
		removeMetaFile(t.metaPathValue())
	}
	t.applyPlan(chunkSize, done)
	startBytes := int64(0)
	if resumeUsed {
		startBytes = bytesDone(done, chunkSize, info.total)
	}
	t.setDownloaded(startBytes)

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	// Preallocation surfaces disk-full early. Never O_APPEND: WriteAt must stay
	// valid for concurrent writers.
	if err := f.Truncate(info.total); err != nil {
		return fmt.Errorf("download: preallocate: %w", err)
	}
	_ = t.saveMeta()

	if !r.setStatus(t, StatusDownloading, nil) {
		return ctx.Err()
	}

	results := make(chan chunkResult, len(chunks))
	var wg sync.WaitGroup
	submitted := 0
	for _, c := range chunks {
		if done[c.index] {
			continue
		}
		wg.Add(1)
		job := &chunkJob{
			task:         t,
			file:         f,
			chunk:        c,
			etag:         info.etag,
			lastModified: info.lastModified,
			results:      results,
			wg:           &wg,
			ctx:          ctx,
		}
		if !r.exec.submit(ctx, job) {
			wg.Done()
			break
		}
		submitted++
	}

	remaining := submitted
	var firstErr error
collect:
	for remaining > 0 {
		select {
		case res := <-results:
			remaining--
			if res.err == nil {
				t.setChunkDone(res.index)
				_ = t.saveMeta()
				continue
			}
			if errors.Is(res.err, context.Canceled) || errors.Is(res.err, context.DeadlineExceeded) {
				continue
			}
			if errors.Is(res.err, errRangeChanged) {
				firstErr = res.err
			} else if firstErr == nil || !errors.Is(firstErr, errRangeChanged) {
				firstErr = res.err
			}
		case <-ctx.Done():
			break collect
		}
	}
	// No job writes after this point: the file can be closed safely.
	wg.Wait()

	if ctx.Err() != nil {
		// Jobs that finished while we were unwinding may have fully written
		// their chunk: record those before saving the sidecar so a resume does
		// not repeat work that is already on disk.
		for {
			select {
			case res := <-results:
				if res.err == nil {
					t.setChunkDone(res.index)
				}
			default:
				_ = t.saveMeta()
				return ctx.Err()
			}
		}
	}
	if firstErr != nil {
		_ = t.saveMeta()
		return firstErr
	}
	for _, ok := range t.chunkBitmap() {
		if !ok {
			_ = t.saveMeta()
			return fmt.Errorf("download: transfer finished with incomplete chunks")
		}
	}
	if err := f.Sync(); err != nil {
		return err
	}
	removeMetaFile(t.metaPathValue())
	return nil
}

// downloadSequential is the fallback for servers without Range support.
func (r *Role) downloadSequential(ctx context.Context, t *Task, info *probeInfo) error {
	path := t.pathValue()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	removeMetaFile(t.metaPathValue())
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if !r.setStatus(t, StatusDownloading, nil) {
		return ctx.Err()
	}
	maxRetries, base := t.retryConfig()
	return retryDo(ctx, maxRetries+1, base, func() error {
		t.setDownloaded(0)
		if err := f.Truncate(0); err != nil {
			return err
		}
		return r.streamOnce(ctx, t, f)
	})
}

func (r *Role) streamOnce(ctx context.Context, t *Task, f *os.File) error {
	req, err := t.newRequest(ctx, http.MethodGet)
	if err != nil {
		return err
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &httpStatusError{code: resp.StatusCode, url: req.URL.String()}
	}
	if err := checkFinalURL(resp.Request.URL); err != nil {
		return err
	}
	written, err := t.copyBody(ctx, f, resp.Body, 0, -1)
	if err != nil {
		return err
	}
	if resp.ContentLength >= 0 && written != resp.ContentLength {
		return io.ErrUnexpectedEOF
	}
	t.setTotal(written)
	t.setDownloaded(written)
	return nil
}

func (t *Task) downloadChunk(ctx context.Context, f *os.File, c chunkSpec, etag, lm string) error {
	maxRetries, base := t.retryConfig()
	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		added, err := t.fetchChunk(ctx, f, c, etag, lm)
		if err == nil && added != c.size() {
			err = io.ErrUnexpectedEOF
		}
		if err == nil {
			return nil
		}
		// A partially written chunk is restarted from its beginning, so its
		// bytes must not stay counted.
		if added > 0 {
			t.downloaded.Add(-added)
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		if isPermanent(err) {
			return err
		}
		lastErr = err
		if attempt == maxRetries {
			break
		}
		t.addRetry()
		if err := sleepBackoff(ctx, attempt, base); err != nil {
			return err
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("download: chunk %d failed", c.index)
	}
	return lastErr
}

func (t *Task) fetchChunk(ctx context.Context, f *os.File, c chunkSpec, etag, lm string) (int64, error) {
	req, err := t.newRequest(ctx, http.MethodGet)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", c.start, c.end-1))
	validator := etag
	if strings.HasPrefix(validator, "W/") {
		// If-Range requires a strong validator.
		validator = ""
	}
	switch {
	case validator != "":
		req.Header.Set("If-Range", validator)
	case lm != "":
		req.Header.Set("If-Range", lm)
	}
	resp, err := t.role.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusPartialContent:
		if cr, ok := parseContentRange(resp.Header.Get("Content-Range")); ok {
			if cr.start != c.start || (cr.total >= 0 && cr.total != t.totalValue()) {
				return 0, errRangeChanged
			}
		}
	case http.StatusRequestedRangeNotSatisfiable:
		return 0, errRangeChanged
	case http.StatusOK:
		// If-Range rejected our validator (or the server ignored Range).
		return 0, errRangeChanged
	default:
		return 0, &httpStatusError{code: resp.StatusCode, url: req.URL.String()}
	}
	return t.copyBody(ctx, f, resp.Body, c.start, c.size())
}

func (t *Task) copyBody(ctx context.Context, f *os.File, body io.Reader, offset, want int64) (int64, error) {
	buf := make([]byte, copyBufferSize)
	var written int64
	for {
		if err := ctx.Err(); err != nil {
			return written, err
		}
		readSize := len(buf)
		if want >= 0 {
			remain := want - written
			if remain <= 0 {
				return written, nil
			}
			if int64(readSize) > remain {
				readSize = int(remain)
			}
		}
		n, rerr := body.Read(buf[:readSize])
		if n > 0 {
			if err := t.waitRate(ctx, n); err != nil {
				return written, err
			}
			if _, werr := f.WriteAt(buf[:n], offset+written); werr != nil {
				return written, werr
			}
			written += int64(n)
			t.downloaded.Add(int64(n))
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				if want >= 0 && written < want {
					return written, io.ErrUnexpectedEOF
				}
				return written, nil
			}
			return written, rerr
		}
	}
}

func (t *Task) newRequest(ctx context.Context, method string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, t.urlValue(), nil)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidURL, err)
	}
	// Byte ranges and sizes must refer to the stored representation.
	req.Header.Set("Accept-Encoding", "identity")
	if ua := t.userAgentValue(); ua != "" {
		req.Header.Set("User-Agent", ua)
	}
	return req, nil
}

func (t *Task) totalValue() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.total
}
