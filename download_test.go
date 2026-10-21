package download_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/xiaoLangZe/go_muyuan"
)

func testData(n int) []byte {
	b := make([]byte, n)
	r := rand.New(rand.NewSource(42))
	_, _ = r.Read(b)
	return b
}

type slowReadSeeker struct {
	r     io.ReadSeeker
	delay time.Duration
}

func (s *slowReadSeeker) Read(p []byte) (int, error) {
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	return s.r.Read(p)
}

func (s *slowReadSeeker) Seek(off int64, whence int) (int64, error) {
	return s.r.Seek(off, whence)
}

type rangeServer struct {
	data    []byte
	etag    string
	delay   time.Duration
	noRange bool

	mu        sync.Mutex
	requests  int
	rangeReqs int
}

func (s *rangeServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.requests++
	if r.Header.Get("Range") != "" {
		s.rangeReqs++
	}
	s.mu.Unlock()

	if s.noRange {
		w.Header().Set("Content-Length", fmt.Sprint(len(s.data)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(s.data)
		return
	}
	if s.etag != "" {
		w.Header().Set("ETag", `"`+s.etag+`"`)
	}
	w.Header().Set("Accept-Ranges", "bytes")
	mod := time.Unix(1700000000, 0)
	http.ServeContent(w, r, "file.bin", mod, &slowReadSeeker{r: bytes.NewReader(s.data), delay: s.delay})
}

func (s *rangeServer) stats() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requests, s.rangeReqs
}

func newRole(t *testing.T, opts ...download.Option) *download.Role {
	t.Helper()
	base := []download.Option{
		download.WithAllowPrivateHosts(true),
		download.WithProgressInterval(50 * time.Millisecond),
		download.WithRetryBase(20 * time.Millisecond),
	}
	r, err := download.New(append(base, opts...)...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		if err := r.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return r
}

func waitFor(t *testing.T, timeout time.Duration, what string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func waitTask(t *testing.T, r *download.Role, id download.TaskID) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := r.Wait(ctx, id); err != nil {
		info, _ := r.Get(id)
		t.Fatalf("Wait: %v (status=%s path=%s error=%q)", err, info.Status, info.Path, info.Error)
	}
}

func TestDownloadRangedCompletes(t *testing.T) {
	data := testData(3 << 20)
	srv := &rangeServer{data: data, etag: "v1"}
	ts := httptest.NewServer(srv)
	defer ts.Close()

	dir := t.TempDir()
	r := newRole(t, download.WithThreads(4))
	id, err := r.Add(ts.URL+"/file.bin", dir)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	waitTask(t, r, id)

	got, err := os.ReadFile(filepath.Join(dir, "file.bin"))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("downloaded content mismatch: got %d bytes want %d", len(got), len(data))
	}
	info, ok := r.Get(id)
	if !ok {
		t.Fatal("task missing after completion")
	}
	if info.Status != download.StatusCompleted {
		t.Fatalf("status = %s, want completed", info.Status)
	}
	if info.Downloaded != int64(len(data)) || info.TotalSize != int64(len(data)) {
		t.Fatalf("size accounting: downloaded=%d total=%d", info.Downloaded, info.TotalSize)
	}
	if info.ChunksTotal < 2 {
		t.Fatalf("expected multiple chunks, got %d", info.ChunksTotal)
	}
	if _, err := os.Stat(filepath.Join(dir, "file.bin.dltmp.json")); !os.IsNotExist(err) {
		t.Fatalf("sidecar not removed: %v", err)
	}
	_, ranges := srv.stats()
	if ranges < 2 {
		t.Fatalf("expected ranged requests, got %d", ranges)
	}
}

func TestSequentialFallbackWithoutRange(t *testing.T) {
	data := testData(1 << 20)
	srv := &rangeServer{data: data, noRange: true}
	ts := httptest.NewServer(srv)
	defer ts.Close()

	dir := t.TempDir()
	r := newRole(t)
	id, err := r.Add(ts.URL+"/plain.bin", dir)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	waitTask(t, r, id)

	got, err := os.ReadFile(filepath.Join(dir, "plain.bin"))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("sequential download content mismatch")
	}
	info, _ := r.Get(id)
	if info.Status != download.StatusCompleted || info.ChunksTotal != 0 {
		t.Fatalf("unexpected info: status=%s chunks=%d", info.Status, info.ChunksTotal)
	}
}

func TestHTTPErrorFailsTask(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusNotFound)
	}))
	defer ts.Close()

	r := newRole(t)
	id, err := r.Add(ts.URL+"/missing.bin", t.TempDir())
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := r.Wait(ctx, id); err == nil {
		t.Fatal("expected failure for 404")
	}
	info, _ := r.Get(id)
	if info.Status != download.StatusFailed || info.Error == "" {
		t.Fatalf("expected failed task with error, got status=%s error=%q", info.Status, info.Error)
	}
}

func TestPauseResume(t *testing.T) {
	data := testData(4 << 20)
	srv := &rangeServer{data: data, etag: "v1", delay: 15 * time.Millisecond}
	ts := httptest.NewServer(srv)
	defer ts.Close()

	dir := t.TempDir()
	r := newRole(t, download.WithThreads(2))
	id, err := r.Add(ts.URL+"/pausable.bin", dir)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	waitFor(t, 10*time.Second, "download progress", func() bool {
		info, ok := r.Get(id)
		return ok && info.Downloaded > 0
	})
	if err := r.Pause(id); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	waitFor(t, 10*time.Second, "paused state", func() bool {
		info, _ := r.Get(id)
		return info.Status == download.StatusPaused
	})
	if _, err := os.Stat(filepath.Join(dir, "pausable.bin.dltmp.json")); err != nil {
		t.Fatalf("sidecar should exist after pause: %v", err)
	}
	if err := r.Resume(id); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	waitTask(t, r, id)

	got, err := os.ReadFile(filepath.Join(dir, "pausable.bin"))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("content mismatch after resume")
	}
	if _, err := os.Stat(filepath.Join(dir, "pausable.bin.dltmp.json")); !os.IsNotExist(err) {
		t.Fatalf("sidecar should be removed after completion: %v", err)
	}
}

func TestCancelThenResume(t *testing.T) {
	data := testData(3 << 20)
	srv := &rangeServer{data: data, etag: "v1", delay: 10 * time.Millisecond}
	ts := httptest.NewServer(srv)
	defer ts.Close()

	dir := t.TempDir()
	r := newRole(t, download.WithThreads(2))
	id, err := r.Add(ts.URL+"/cancel.bin", dir)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	waitFor(t, 10*time.Second, "progress", func() bool {
		info, _ := r.Get(id)
		return info.Downloaded > 0
	})
	if err := r.Cancel(id); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	info, _ := r.Get(id)
	if info.Status != download.StatusCanceled {
		t.Fatalf("status = %s, want canceled", info.Status)
	}
	if err := r.Resume(id); err != nil {
		t.Fatalf("Resume after cancel: %v", err)
	}
	waitTask(t, r, id)
	got, _ := os.ReadFile(filepath.Join(dir, "cancel.bin"))
	if !bytes.Equal(got, data) {
		t.Fatal("content mismatch after cancel+resume")
	}
}

func TestDuplicateAndIdempotent(t *testing.T) {
	srv := &rangeServer{data: testData(2 << 20), etag: "v1", delay: 5 * time.Millisecond}
	ts := httptest.NewServer(srv)
	defer ts.Close()

	dir := t.TempDir()
	r := newRole(t)
	id, err := r.Add(ts.URL+"/dup.bin", dir)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	_, err = r.Add(ts.URL+"/dup.bin", dir)
	var dup *download.DuplicateTaskError
	if !errors.As(err, &dup) || dup.ID != id {
		t.Fatalf("expected DuplicateTaskError with id %s, got %v", id, err)
	}
	id2, err := r.AddWithOptions(ts.URL+"/dup.bin", dir, download.WithIdempotent(true))
	if err != nil || id2 != id {
		t.Fatalf("idempotent Add = (%s, %v), want (%s, nil)", id2, err, id)
	}
	waitTask(t, r, id)
}

func TestPathAndURLValidation(t *testing.T) {
	root := t.TempDir()
	r := newRole(t, download.WithRootDir(root))

	if _, err := r.Add("http://example.com/file.bin", ".."); !errors.Is(err, download.ErrPathEscape) {
		t.Fatalf("escape error = %v, want ErrPathEscape", err)
	}
	if _, err := r.Add("http://example.com/", root); !errors.Is(err, download.ErrInvalidFileName) {
		t.Fatalf("trailing slash error = %v, want ErrInvalidFileName", err)
	}
	if _, err := r.Add("ftp://example.com/file.bin", root); !errors.Is(err, download.ErrInvalidURL) {
		t.Fatalf("scheme error = %v, want ErrInvalidURL", err)
	}
	if _, err := r.AddWithOptions("http://example.com/file.bin", root, download.WithFileName("../evil")); !errors.Is(err, download.ErrInvalidFileName) {
		t.Fatalf("file name error = %v, want ErrInvalidFileName", err)
	}
}

func TestPrivateHostRejectedByDefault(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer ts.Close()

	r, err := download.New(download.WithThreads(2))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer r.Close()
	if _, err := r.Add(ts.URL+"/x.bin", t.TempDir()); !errors.Is(err, download.ErrPrivateHost) {
		t.Fatalf("error = %v, want ErrPrivateHost", err)
	}
	if _, err := r.Add("http://localhost:8080/x.bin", t.TempDir()); !errors.Is(err, download.ErrPrivateHost) {
		t.Fatalf("localhost error = %v, want ErrPrivateHost", err)
	}
}

func TestEventsSequenceAndCompletion(t *testing.T) {
	srv := &rangeServer{data: testData(2 << 20), etag: "v1", delay: 3 * time.Millisecond}
	ts := httptest.NewServer(srv)
	defer ts.Close()

	r := newRole(t, download.WithThreads(3), download.WithReplayBuffer(256),
		download.WithProgressInterval(5*time.Millisecond))
	ch, cancel := r.Subscribe()
	defer cancel()

	id, err := r.Add(ts.URL+"/events.bin", t.TempDir())
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	var events []download.Event
	timeout := time.After(30 * time.Second)
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				t.Fatal("subscription closed unexpectedly")
			}
			events = append(events, ev)
			if ev.Type == download.EventTaskState {
				info, ok := ev.Payload.(download.TaskInfo)
				if ok && info.ID == id && info.Status == download.StatusCompleted {
					goto done
				}
			}
		case <-timeout:
			t.Fatal("timed out waiting for completion event")
		}
	}
done:
	var lastSeq uint64
	types := map[download.EventType]bool{}
	for _, ev := range events {
		if ev.Seq < lastSeq {
			t.Fatalf("sequence went backwards: %d after %d", ev.Seq, lastSeq)
		}
		lastSeq = ev.Seq
		types[ev.Type] = true
	}
	for _, want := range []download.EventType{download.EventTaskAdded, download.EventTaskState, download.EventTaskProgress} {
		if !types[want] {
			t.Fatalf("event type %s missing from stream (%v)", want, types)
		}
	}

	snap, err := r.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	replay, replayCancel := r.Subscribe(download.WithSince(0), download.WithSubscribeBuffer(512))
	defer replayCancel()
	select {
	case ev, ok := <-replay:
		if !ok {
			t.Fatal("replay channel closed for fresh sequence")
		}
		if ev.Type != download.EventTaskAdded || ev.TaskID != id {
			t.Fatalf("first replay event = %s/%s, want task_added/%s", ev.Type, ev.TaskID, id)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no replay events")
	}
	if snap.Seq == 0 {
		t.Fatal("snapshot seq is zero")
	}
}

func TestSnapshotAndStats(t *testing.T) {
	srv := &rangeServer{data: testData(256 << 10), etag: "v1"}
	ts := httptest.NewServer(srv)
	defer ts.Close()

	r := newRole(t)
	dir := t.TempDir()
	var ids []download.TaskID
	for i := 0; i < 3; i++ {
		id, err := r.AddWithOptions(fmt.Sprintf("%s/f%d.bin", ts.URL, i), dir,
			download.WithPriority(i), download.WithTags(map[string]string{"group": "g"}))
		if err != nil {
			t.Fatalf("Add %d: %v", i, err)
		}
		ids = append(ids, id)
		waitTask(t, r, id)
	}

	snap, err := r.Snapshot(download.WithSnapshotSort(download.SortByPriority, true))
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if snap.Summary.Total != 3 || snap.Summary.Completed != 3 {
		t.Fatalf("summary = %+v", snap.Summary)
	}
	if len(snap.Tasks) != 3 {
		t.Fatalf("snapshot tasks = %d", len(snap.Tasks))
	}
	if snap.Tasks[0].Priority != 2 {
		t.Fatalf("sort/priority wrong: %+v", snap.Tasks)
	}

	light, err := r.Snapshot(download.WithSnapshotDetails(false))
	if err != nil {
		t.Fatalf("light Snapshot: %v", err)
	}
	if light.Tasks != nil {
		t.Fatal("light snapshot should omit tasks")
	}

	list, err := r.List(download.ListFilter{
		Tags:     map[string]string{"group": "g"},
		IDs:      ids,
		SortBy:   download.SortByPriority,
		SortDesc: true,
		Limit:    2,
	})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 2 || list[0].Priority != 2 {
		t.Fatalf("filtered list = %+v", list)
	}
	if _, err := r.List(download.ListFilter{Limit: -1}); !errors.Is(err, download.ErrInvalidFilter) {
		t.Fatalf("negative limit error = %v, want ErrInvalidFilter", err)
	}
	if all := r.ListAll(); len(all) != 3 {
		t.Fatalf("ListAll = %d tasks", len(all))
	}
	stats := r.Stats()
	if stats.Total != 3 || stats.Workers < 1 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestRemoveCleanupAndClosedRole(t *testing.T) {
	srv := &rangeServer{data: testData(2 << 20), etag: "v1", delay: 10 * time.Millisecond}
	ts := httptest.NewServer(srv)
	defer ts.Close()

	dir := t.TempDir()
	r := newRole(t, download.WithThreads(2))
	id, err := r.Add(ts.URL+"/removed.bin", dir)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	waitFor(t, 10*time.Second, "progress", func() bool {
		info, _ := r.Get(id)
		return info.Downloaded > 0
	})
	if err := r.Remove(id, true); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if _, ok := r.Get(id); ok {
		t.Fatal("task still present after Remove")
	}
	if _, err := os.Stat(filepath.Join(dir, "removed.bin")); !os.IsNotExist(err) {
		t.Fatalf("partial file should be removed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "removed.bin.dltmp.json")); !os.IsNotExist(err) {
		t.Fatalf("sidecar should be removed: %v", err)
	}

	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := r.Add(ts.URL+"/after.bin", dir); !errors.Is(err, download.ErrRoleClosed) {
		t.Fatalf("Add after Close = %v, want ErrRoleClosed", err)
	}
}

func TestSubscribeBackpressureKicksSlowConsumer(t *testing.T) {
	srv := &rangeServer{data: testData(128 << 10), etag: "v1"}
	ts := httptest.NewServer(srv)
	defer ts.Close()

	r := newRole(t, download.WithThreads(2), download.WithProgressInterval(10*time.Millisecond))
	ch, cancel := r.Subscribe(download.WithSubscribeBuffer(1))
	defer cancel()

	dir := t.TempDir()
	for i := 0; i < 8; i++ {
		if _, err := r.Add(fmt.Sprintf("%s/backpressure%d.bin", ts.URL, i), dir); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	// Do not read: state events must eventually kick the subscriber.
	time.Sleep(500 * time.Millisecond)
	received := 0
	deadline := time.After(5 * time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				if received > 2 {
					t.Fatalf("drained %d events, expected a kicked channel", received)
				}
				if r.Stats().DroppedEvents == 0 {
					t.Fatal("dropped event counter is zero after kick")
				}
				return
			}
			received++
		case <-deadline:
			t.Fatalf("slow subscriber was not disconnected (received %d buffered events)", received)
		}
	}
}

func TestWaitContextDoesNotCancelTask(t *testing.T) {
	srv := &rangeServer{data: testData(2 << 20), etag: "v1", delay: 15 * time.Millisecond}
	ts := httptest.NewServer(srv)
	defer ts.Close()

	dir := t.TempDir()
	r := newRole(t, download.WithThreads(2))
	id, err := r.Add(ts.URL+"/wait.bin", dir)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := r.Wait(ctx, id); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Wait = %v, want context deadline", err)
	}
	info, _ := r.Get(id)
	if info.Status == download.StatusCanceled || info.Status == download.StatusFailed {
		t.Fatalf("task stopped because the wait expired: %s", info.Status)
	}
	waitTask(t, r, id)
	got, _ := os.ReadFile(filepath.Join(dir, "wait.bin"))
	if !bytes.Equal(got, srv.data) {
		t.Fatal("content mismatch: task did not survive Wait cancellation")
	}
}

func TestUpdatePriorityAndRate(t *testing.T) {
	srv := &rangeServer{data: testData(256 << 10), etag: "v1"}
	ts := httptest.NewServer(srv)
	defer ts.Close()

	r := newRole(t)
	id, err := r.Add(ts.URL+"/update.bin", t.TempDir())
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	priority := 7
	rate := int64(0)
	if err := r.Update(id, download.TaskPatch{Priority: &priority, RateLimit: &rate}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	info, _ := r.Get(id)
	if info.Priority != 7 {
		t.Fatalf("priority = %d", info.Priority)
	}
	badRate := int64(-1)
	if err := r.Update(id, download.TaskPatch{RateLimit: &badRate}); !errors.Is(err, download.ErrInvalidPatch) {
		t.Fatalf("negative rate error = %v", err)
	}
	waitTask(t, r, id)
	// URL changes are only legal while Pending.
	newURL := ts.URL + "/other.bin"
	if err := r.Update(id, download.TaskPatch{URL: &newURL}); !errors.Is(err, download.ErrInvalidState) {
		t.Fatalf("post-completion URL update = %v, want ErrInvalidState", err)
	}
}

type mutableServer struct {
	mu   sync.Mutex
	data []byte
	etag string
}

func (s *mutableServer) set(data []byte, etag string) {
	s.mu.Lock()
	s.data, s.etag = data, etag
	s.mu.Unlock()
}

func (s *mutableServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	data, etag := s.data, s.etag
	s.mu.Unlock()
	w.Header().Set("ETag", `"`+etag+`"`)
	w.Header().Set("Accept-Ranges", "bytes")
	http.ServeContent(w, r, "file.bin", time.Unix(1700000000, 0),
		&slowReadSeeker{r: bytes.NewReader(data), delay: 20 * time.Millisecond})
}

func TestResumeRestartsWhenRemoteFileChanged(t *testing.T) {
	v1 := testData(4 << 20)
	v2 := testData(4 << 20)
	for i := range v2 {
		v2[i] ^= 0xff
	}
	srv := &mutableServer{data: v1, etag: "v1"}
	ts := httptest.NewServer(srv)
	defer ts.Close()

	dir := t.TempDir()
	r := newRole(t, download.WithThreads(2))
	id, err := r.Add(ts.URL+"/changing.bin", dir)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	waitFor(t, 10*time.Second, "progress", func() bool {
		info, _ := r.Get(id)
		return info.Downloaded > 0
	})
	if err := r.Pause(id); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	waitFor(t, 10*time.Second, "paused", func() bool {
		info, _ := r.Get(id)
		return info.Status == download.StatusPaused
	})
	srv.set(v2, "v2")
	if err := r.Resume(id); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	waitTask(t, r, id)

	got, err := os.ReadFile(filepath.Join(dir, "changing.bin"))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Equal(got, v2) {
		t.Fatal("resume mixed old and new remote content; expected a clean restart")
	}
}

func TestOverwritePolicies(t *testing.T) {
	data := testData(64 << 10)
	srv := &rangeServer{data: data, etag: "v1"}
	ts := httptest.NewServer(srv)
	defer ts.Close()

	dir := t.TempDir()
	target := filepath.Join(dir, "ow.bin")
	if err := os.WriteFile(target, []byte("old content"), 0o644); err != nil {
		t.Fatal(err)
	}
	r := newRole(t)

	if _, err := r.Add(ts.URL+"/ow.bin", dir); !errors.Is(err, download.ErrFileExists) {
		t.Fatalf("default policy error = %v, want ErrFileExists", err)
	}
	if _, err := r.AddWithOptions(ts.URL+"/ow.bin", dir,
		download.WithOverwritePolicy(download.OverwriteSkip)); !errors.Is(err, download.ErrFileExists) {
		t.Fatalf("skip policy error = %v, want ErrFileExists", err)
	}
	id, err := r.AddWithOptions(ts.URL+"/ow.bin", dir,
		download.WithOverwritePolicy(download.OverwriteReplace))
	if err != nil {
		t.Fatalf("replace Add: %v", err)
	}
	waitTask(t, r, id)
	got, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("replace policy did not refresh the file")
	}
	id2, err := r.AddWithOptions(ts.URL+"/ow.bin", dir,
		download.WithOverwritePolicy(download.OverwriteResume))
	if err != nil {
		t.Fatalf("resume policy Add: %v", err)
	}
	waitTask(t, r, id2)
}

func TestCloseCancelsActiveTasksAndSubscriptions(t *testing.T) {
	srv := &rangeServer{data: testData(3 << 20), etag: "v1", delay: 20 * time.Millisecond}
	ts := httptest.NewServer(srv)
	defer ts.Close()

	r, err := download.New(download.WithAllowPrivateHosts(true),
		download.WithThreads(2), download.WithProgressInterval(10*time.Millisecond))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ch, cancel := r.Subscribe()
	defer cancel()
	id, err := r.Add(ts.URL+"/close.bin", t.TempDir())
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	waitFor(t, 10*time.Second, "progress", func() bool {
		info, _ := r.Get(id)
		return info.Downloaded > 0
	})
	done := make(chan error, 1)
	go func() { done <- r.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close hung")
	}
	info, ok := r.Get(id)
	if !ok {
		t.Fatal("task record should survive Close")
	}
	if info.Status != download.StatusCanceled {
		t.Fatalf("status after Close = %s, want canceled", info.Status)
	}
	if err := r.Wait(context.Background(), id); !errors.Is(err, download.ErrTaskCanceled) {
		t.Fatalf("Wait after Close = %v, want ErrTaskCanceled", err)
	}
	for range ch {
		// Drain until the package closes the channel.
	}
	if _, ok := <-ch; ok {
		t.Fatal("subscription still open after Close")
	}
}

func TestWithSinceTooOldClosesChannel(t *testing.T) {
	srv := &rangeServer{data: testData(64 << 10), etag: "v1"}
	ts := httptest.NewServer(srv)
	defer ts.Close()

	r := newRole(t, download.WithReplayBuffer(4))
	dir := t.TempDir()
	for i := 0; i < 3; i++ {
		id, err := r.Add(fmt.Sprintf("%s/replay%d.bin", ts.URL, i), dir)
		if err != nil {
			t.Fatalf("Add: %v", err)
		}
		waitTask(t, r, id)
	}
	ch, cancel := r.Subscribe(download.WithSince(1))
	defer cancel()
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected an immediately closed channel for an evicted sequence")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Subscribe with stale sequence did not close the channel")
	}
}

func TestResumeIgnoresSidecarWithoutTargetFile(t *testing.T) {
	data := testData(2 << 20)
	srv := &rangeServer{data: data, etag: "v1", delay: 10 * time.Millisecond}
	ts := httptest.NewServer(srv)
	defer ts.Close()

	dir := t.TempDir()
	r := newRole(t, download.WithThreads(2))
	id, err := r.Add(ts.URL+"/gone.bin", dir)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	waitFor(t, 10*time.Second, "progress", func() bool {
		info, _ := r.Get(id)
		return info.Downloaded > 0
	})
	if err := r.Pause(id); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	waitFor(t, 10*time.Second, "paused", func() bool {
		info, _ := r.Get(id)
		return info.Status == download.StatusPaused
	})
	target := filepath.Join(dir, "gone.bin")
	waitFor(t, 5*time.Second, "target file handle release", func() bool {
		err := os.Remove(target)
		return err == nil || os.IsNotExist(err)
	})
	if err := r.Resume(id); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	waitTask(t, r, id)
	got, err := os.ReadFile(filepath.Join(dir, "gone.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("resume with a missing target file produced corrupt output")
	}
}
