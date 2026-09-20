package req

import (
	"context"
	"net/http"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/imroc/req/v3/pkg/altsvc"
)

// fakeH3Prober is a controllable stand-in for *http3.Transport: it never
// dials real QUIC connections, so tests can exercise the registry's own
// coordination logic without any network involved.
type fakeH3Prober struct {
	mu       sync.Mutex
	calls    []string
	addConn  func(ctx context.Context, hostname string) error
	response func(req *http.Request) (*http.Response, error)
}

func (f *fakeH3Prober) AddConn(ctx context.Context, hostname string) error {
	f.mu.Lock()
	f.calls = append(f.calls, hostname)
	f.mu.Unlock()
	return f.addConn(ctx, hostname)
}

func (f *fakeH3Prober) RoundTrip(req *http.Request) (*http.Response, error) {
	return f.response(req)
}

func (f *fakeH3Prober) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func testH3Entries(host string, expire time.Time) []*altsvc.AltSvc {
	return []*altsvc.AltSvc{{Protocol: "h3", Host: host, Port: "443", Expire: expire}}
}

func mustParseURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("failed to parse url %q: %v", raw, err)
	}
	return u
}

// waitUntil polls cond until it returns true or the deadline passes,
// avoiding a fixed sleep for what is otherwise inherently async setup
// (the registry processes channel messages on its own goroutine).
func waitUntil(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("condition not met before deadline")
}

func TestAltSvcRegistryDiscoverSeedsBeforeDedupingDuplicates(t *testing.T) {
	prober := &fakeH3Prober{addConn: func(ctx context.Context, hostname string) error {
		<-ctx.Done() // never resolves on its own; only the caller's timeout ends it
		return ctx.Err()
	}}
	r := newAltSvcRegistry(prober)
	defer r.close()

	u := mustParseURL(t, "https://example.test/asset")
	entries := testH3Entries("example.test", time.Now().Add(time.Hour))

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.discover("https://example.test:443", u, entries)
		}()
	}
	wg.Wait()

	waitUntil(t, func() bool { return prober.callCount() >= 1 })
	time.Sleep(20 * time.Millisecond) // let any accidental duplicate dispatch land if it were going to
	if got := prober.callCount(); got != 1 {
		t.Fatalf("AddConn called %d times for 20 concurrent discoveries of the same host, want exactly 1", got)
	}
}

func TestAltSvcRegistryLoadNeverBlocksOnInFlightProbe(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	prober := &fakeH3Prober{addConn: func(ctx context.Context, hostname string) error {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return ctx.Err()
	}}
	r := newAltSvcRegistry(prober)
	defer r.close()

	u := mustParseURL(t, "https://example.test/asset")
	r.discover("https://example.test:443", u, testH3Entries("example.test", time.Now().Add(time.Hour)))
	waitUntil(t, func() bool { return prober.callCount() >= 1 }) // probe is now stuck on release

	start := time.Now()
	_, ok := r.load("https://example.test:443")
	elapsed := time.Since(start)

	if ok {
		t.Fatal("load() returned ok=true for a host whose probe hasn't succeeded yet")
	}
	if elapsed > 50*time.Millisecond {
		t.Fatalf("load() took %v; it must never wait on an in-flight probe", elapsed)
	}
}

func TestAltSvcRegistrySuccessIsUsableViaLoad(t *testing.T) {
	prober := &fakeH3Prober{
		addConn: func(ctx context.Context, hostname string) error { return nil },
		response: func(req *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Body: http.NoBody, Request: req}, nil
		},
	}
	r := newAltSvcRegistry(prober)
	defer r.close()

	addr := "https://example.test:443"
	u := mustParseURL(t, "https://example.test/asset")
	r.discover(addr, u, testH3Entries("example.test", time.Now().Add(time.Hour)))

	waitUntil(t, func() bool { _, ok := r.load(addr); return ok })
	entry, ok := r.load(addr)
	if !ok {
		t.Fatal("load() never became ready after a successful probe")
	}
	req, _ := http.NewRequest(http.MethodGet, u.String(), nil)
	resp, err := entry.transport.RoundTrip(req)
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("RoundTrip via the confirmed entry failed: resp=%v err=%v", resp, err)
	}
}

func TestAltSvcRegistryExhaustionRemovesHostForFutureRediscovery(t *testing.T) {
	prober := &fakeH3Prober{addConn: func(ctx context.Context, hostname string) error {
		return context.DeadlineExceeded // every attempt fails
	}}
	r := newAltSvcRegistry(prober)
	defer r.close()

	addr := "https://example.test:443"
	u := mustParseURL(t, "https://example.test/asset")
	entries := testH3Entries("example.test", time.Now().Add(time.Hour)) // exactly one candidate

	r.discover(addr, u, entries)
	waitUntil(t, func() bool {
		_, exists := r.hosts.Load(addr)
		return prober.callCount() >= 1 && !exists
	})

	if _, exists := r.hosts.Load(addr); exists {
		t.Fatal("host entry should be removed once every alt-svc candidate has failed")
	}

	// A later response can still trigger a fresh attempt; the old code's
	// leftover dead entry would have permanently blocked this.
	r.discover(addr, u, entries)
	waitUntil(t, func() bool { return prober.callCount() >= 2 })
}
