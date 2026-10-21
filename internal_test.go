package download

import (
	"context"
	"testing"
	"time"
)

func TestPlanChunksBoundaries(t *testing.T) {
	cases := []struct {
		total    int64
		wantSize int64
	}{
		{0, 0},
		{1, minChunkSize},
		{10 << 20, minChunkSize},
		{1 << 30, 16 << 20},
		{200 << 20, 200 << 20 / 64},
	}
	for _, c := range cases {
		if got := planChunks(c.total); got != c.wantSize {
			t.Errorf("planChunks(%d) = %d, want %d", c.total, got, c.wantSize)
		}
	}
	chunks := buildChunks(10, 4)
	if len(chunks) != 3 || chunks[2].end != 10 || chunks[0].size() != 4 {
		t.Fatalf("unexpected chunks: %+v", chunks)
	}
}

func TestParseContentRange(t *testing.T) {
	cases := []struct {
		in string
		ok bool
		cr contentRange
	}{
		{"bytes 0-0/123", true, contentRange{start: 0, end: 0, total: 123}},
		{"bytes 100-199/1000", true, contentRange{start: 100, end: 199, total: 1000}},
		{"bytes */0", true, contentRange{start: -1, end: -1, total: 0}},
		{"bytes 5-4/10", false, contentRange{}},
		{"items 0-0/1", false, contentRange{}},
		{"garbage", false, contentRange{}},
	}
	for _, c := range cases {
		got, ok := parseContentRange(c.in)
		if ok != c.ok || (ok && got != c.cr) {
			t.Errorf("parseContentRange(%q) = (%+v, %v), want (%+v, %v)", c.in, got, ok, c.cr, c.ok)
		}
	}
}

func TestValidateFileName(t *testing.T) {
	good := []string{"file.bin", "a b.txt", "中文.zip", "v1.2.3.tar.gz"}
	for _, name := range good {
		if err := validateFileName(name); err != nil {
			t.Errorf("validateFileName(%q) = %v", name, err)
		}
	}
	bad := []string{"", ".", "..", "a/b", `a\b`, "a\x00b", "a\nb"}
	for _, name := range bad {
		if err := validateFileName(name); err == nil {
			t.Errorf("validateFileName(%q) accepted", name)
		}
	}
}

func TestWithinDir(t *testing.T) {
	if !withinDir("/tmp/root", "/tmp/root/a/b") {
		t.Error("nested path should be inside")
	}
	if withinDir("/tmp/root", "/tmp/root2/a") {
		t.Error("sibling prefix must not count as inside")
	}
	if withinDir("/tmp/root", "/tmp") {
		t.Error("parent path must not count as inside")
	}
}

func TestTransitionRules(t *testing.T) {
	legal := [][2]TaskStatus{
		{StatusPending, StatusProbing},
		{StatusProbing, StatusDownloading},
		{StatusProbing, StatusCompleted},
		{StatusDownloading, StatusPaused},
		{StatusPaused, StatusPending},
		{StatusCanceled, StatusPending},
		{StatusDownloading, StatusCompleted},
	}
	for _, pair := range legal {
		if !canTransition(pair[0], pair[1]) {
			t.Errorf("%s -> %s should be legal", pair[0], pair[1])
		}
	}
	illegal := [][2]TaskStatus{
		{StatusCompleted, StatusDownloading},
		{StatusPending, StatusCompleted},
		{StatusFailed, StatusPending},
		{StatusRemoved, StatusPending},
	}
	for _, pair := range illegal {
		if canTransition(pair[0], pair[1]) {
			t.Errorf("%s -> %s should be illegal", pair[0], pair[1])
		}
	}
}

func TestLimiterCancellation(t *testing.T) {
	l := newLimiter(1024)
	if err := l.waitN(context.Background(), 512); err != nil {
		t.Fatalf("waitN: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	if err := l.waitN(ctx, 1<<20); err == nil {
		t.Fatal("expected cancellation error")
	}
	if time.Since(start) > time.Second {
		t.Fatal("cancellation was not prompt")
	}
}
