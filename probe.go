package download

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// probeInfo describes the remote file's capabilities.
type probeInfo struct {
	finalURL      string
	total         int64
	supportsRange bool
	etag          string
	lastModified  string
}

// contentRange is the parsed Content-Range header.
type contentRange struct {
	start int64
	end   int64
	total int64 // -1 when unknown
}

func parseContentRange(v string) (contentRange, bool) {
	v = strings.TrimSpace(v)
	if !strings.HasPrefix(v, "bytes ") {
		return contentRange{}, false
	}
	rest := strings.TrimPrefix(v, "bytes ")
	slash := strings.IndexByte(rest, '/')
	if slash < 0 {
		return contentRange{}, false
	}
	rangePart := rest[:slash]
	totalPart := rest[slash+1:]
	total := int64(-1)
	if totalPart != "*" {
		n, err := strconv.ParseInt(totalPart, 10, 64)
		if err != nil || n < 0 {
			return contentRange{}, false
		}
		total = n
	}
	if rangePart == "*" {
		return contentRange{start: -1, end: -1, total: total}, true
	}
	dash := strings.IndexByte(rangePart, '-')
	if dash < 0 {
		return contentRange{}, false
	}
	start, err1 := strconv.ParseInt(rangePart[:dash], 10, 64)
	end, err2 := strconv.ParseInt(rangePart[dash+1:], 10, 64)
	if err1 != nil || err2 != nil || start < 0 || end < start {
		return contentRange{}, false
	}
	return contentRange{start: start, end: end, total: total}, true
}

func checkFinalURL(u *url.URL) error {
	if u == nil {
		return ErrInvalidURL
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return ErrInvalidURL
	}
	return nil
}

// probeOnce performs GET with Range: bytes=0-0. A ranged GET is more reliable
// than HEAD: it proves that ranges actually work and returns the real size.
func (r *Role) probeOnce(ctx context.Context, t *Task) (*probeInfo, error) {
	req, err := t.newRequest(ctx, http.MethodGet)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Range", "bytes=0-0")
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	// The probe body is one byte at most; drain defensively for connection reuse.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))

	if err := checkFinalURL(resp.Request.URL); err != nil {
		return nil, err
	}

	info := &probeInfo{
		finalURL:     resp.Request.URL.String(),
		etag:         resp.Header.Get("ETag"),
		lastModified: resp.Header.Get("Last-Modified"),
	}
	switch resp.StatusCode {
	case http.StatusPartialContent:
		cr, ok := parseContentRange(resp.Header.Get("Content-Range"))
		if ok && cr.total >= 0 {
			info.total = cr.total
			info.supportsRange = true
		} else if resp.ContentLength >= 0 {
			info.total = resp.ContentLength
		} else {
			info.total = 0
		}
	case http.StatusOK:
		// No range support (or a server that ignores Range): stream it once.
		info.supportsRange = false
		if resp.ContentLength > 0 {
			info.total = resp.ContentLength
		}
	case http.StatusRequestedRangeNotSatisfiable:
		cr, ok := parseContentRange(resp.Header.Get("Content-Range"))
		if !ok || cr.total != 0 {
			return nil, &httpStatusError{code: resp.StatusCode, url: resp.Request.URL.String()}
		}
		info.total = 0
		info.supportsRange = true
	default:
		return nil, &httpStatusError{code: resp.StatusCode, url: resp.Request.URL.String()}
	}
	return info, nil
}

func (r *Role) probeWithRetry(ctx context.Context, t *Task) (*probeInfo, error) {
	maxRetries, base := t.retryConfig()
	var info *probeInfo
	err := retryDo(ctx, maxRetries+1, base, func() error {
		p, err := r.probeOnce(ctx, t)
		if err != nil {
			return err
		}
		info = p
		return nil
	})
	if err != nil {
		return nil, err
	}
	return info, nil
}
