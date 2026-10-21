package download

import (
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"syscall"
	"time"
)

// userAgentTransport injects the configured User-Agent unless the request
// already carries one (per-task override), and re-checks the target host on
// every request so redirects cannot reach blocked addresses.
type userAgentTransport struct {
	base         http.RoundTripper
	ua           string
	blockPrivate bool
}

func (t *userAgentTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.blockPrivate {
		if err := checkRequestHost(req.URL); err != nil {
			return nil, err
		}
	}
	if t.ua == "" || req.Header.Get("User-Agent") != "" {
		return t.base.RoundTrip(req)
	}
	clone := req.Clone(req.Context())
	clone.Header.Set("User-Agent", t.ua)
	return t.base.RoundTrip(clone)
}

var blockedPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("10.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("127.0.0.0/8"),
	netip.MustParsePrefix("169.254.0.0/16"),
	netip.MustParsePrefix("172.16.0.0/12"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.168.0.0/16"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("224.0.0.0/4"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("::/128"),
	netip.MustParsePrefix("::1/128"),
	netip.MustParsePrefix("64:ff9b::/96"),
	netip.MustParsePrefix("fc00::/7"),
	netip.MustParsePrefix("fe80::/10"),
	netip.MustParsePrefix("ff00::/8"),
}

func isBlockedIP(ip netip.Addr) bool {
	if !ip.IsValid() {
		return true
	}
	ip = ip.Unmap()
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsUnspecified() || ip.IsMulticast() ||
		ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		ip.IsInterfaceLocalMulticast() {
		return true
	}
	for _, p := range blockedPrefixes {
		if p.Contains(ip) {
			return true
		}
	}
	return false
}

// blockPrivateControl is a Dialer.Control hook, so the decision is made on the
// resolved address and a DNS name cannot bypass the check.
func blockPrivateControl(_, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}
	if i := strings.IndexByte(host, '%'); i >= 0 {
		host = host[:i]
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return fmt.Errorf("%w: cannot parse %s", ErrPrivateHost, address)
	}
	if isBlockedIP(ip) {
		return fmt.Errorf("%w: %s", ErrPrivateHost, ip)
	}
	return nil
}

// checkRequestHost rejects literal loopback, private and reserved hosts. Names
// are resolved by the dialer, whose Control hook applies the same test to the
// chosen IP.
func checkRequestHost(u *url.URL) error {
	if u == nil {
		return ErrInvalidURL
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("%w: scheme must be http or https", ErrInvalidURL)
	}
	host := u.Hostname()
	if host == "" {
		return fmt.Errorf("%w: missing host", ErrInvalidURL)
	}
	if strings.EqualFold(host, "localhost") || strings.HasSuffix(strings.ToLower(host), ".localhost") {
		return fmt.Errorf("%w: %s", ErrPrivateHost, host)
	}
	if ip, err := netip.ParseAddr(host); err == nil && isBlockedIP(ip) {
		return fmt.Errorf("%w: %s", ErrPrivateHost, host)
	}
	return nil
}

// validateURL checks the scheme and, as a fast failure, literal hosts. Host
// names are re-checked on every dial after resolution.
func validateURL(raw string, allowPrivate bool) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidURL, err)
	}
	if allowPrivate {
		if u.Scheme != "http" && u.Scheme != "https" {
			return nil, fmt.Errorf("%w: scheme must be http or https", ErrInvalidURL)
		}
		if u.Hostname() == "" {
			return nil, fmt.Errorf("%w: missing host", ErrInvalidURL)
		}
		return u, nil
	}
	if err := checkRequestHost(u); err != nil {
		return nil, err
	}
	return u, nil
}

func buildTransport(cfg *roleConfig) (*http.Transport, error) {
	dialer := &net.Dialer{
		Timeout:   15 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	if !cfg.allowPrivate {
		dialer.Control = blockPrivateControl
	}
	tr := &http.Transport{
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   max(4, cfg.threads*2),
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
	}
	switch {
	case cfg.proxy != "":
		pu, err := url.Parse(cfg.proxy)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid proxy url: %v", ErrInvalidOption, err)
		}
		switch pu.Scheme {
		case "http", "https", "socks5", "socks5h":
		default:
			return nil, fmt.Errorf("%w: unsupported proxy scheme %q", ErrInvalidOption, pu.Scheme)
		}
		if pu.Host == "" {
			return nil, fmt.Errorf("%w: proxy url has no host", ErrInvalidOption)
		}
		tr.Proxy = http.ProxyURL(pu)
	case cfg.proxyFromEnv:
		tr.Proxy = http.ProxyFromEnvironment
	}
	return tr, nil
}

func newHTTPClient(cfg *roleConfig) (*http.Client, error) {
	tr, err := buildTransport(cfg)
	if err != nil {
		return nil, err
	}
	return &http.Client{
		Transport: &userAgentTransport{base: tr, ua: cfg.userAgent, blockPrivate: !cfg.allowPrivate},
	}, nil
}

// wrapCustomClient preserves the caller's client (timeouts, TLS config, jar)
// while adding UA injection and, where possible, the private-address guard.
func wrapCustomClient(hc *http.Client, cfg *roleConfig) *http.Client {
	clone := *hc
	base := clone.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	if tr, ok := base.(*http.Transport); ok && !cfg.allowPrivate {
		cloneTr := tr.Clone()
		dialer := &net.Dialer{
			Timeout:   15 * time.Second,
			KeepAlive: 30 * time.Second,
			Control:   blockPrivateControl,
		}
		cloneTr.DialContext = dialer.DialContext
		base = cloneTr
	}
	clone.Transport = &userAgentTransport{base: base, ua: cfg.userAgent, blockPrivate: !cfg.allowPrivate}
	return &clone
}
