package req

import (
	"context"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/imroc/req/v3/internal/altsvcutil"
	"github.com/imroc/req/v3/pkg/altsvc"
)

// h3Prober is the slice of *http3.Transport the registry actually needs:
// AddConn to probe a candidate, RoundTrip to serve confirmed requests.
// Defined as an interface, rather than depending on *http3.Transport
// directly, so tests can substitute a fake instead of dialing real QUIC
// connections.
type h3Prober interface {
	AddConn(ctx context.Context, hostname string) error
	http.RoundTripper
}

const (
	// proberDialTimeout bounds a single AddConn attempt. It is deliberately
	// short and independent of any request-facing timeout: a probe's only
	// job is "can I connect", and a slow or dead candidate must not tie up
	// a worker for anywhere near as long as a real request is allowed to run.
	proberDialTimeout = 10 * time.Second

	proberWorkerCount = 8
	registryChanSize  = 64
	evictorPeriod     = 1 * time.Minute
)

// altSvcEntry is the only thing the critical path ever reads. A nil
// transport means "known about, not ready to use", every read path must
// fall back to normal H2/H1 in that case rather than wait for it to become
// ready.
type altSvcEntry struct {
	transport http.RoundTripper
	url       *url.URL
	entries   []*altsvc.AltSvc
	index     int
	expire    time.Time // zero while unconfirmed; the entry's own advertised max-age once confirmed
}

type altSvcRegisterMsg struct {
	addr    string
	url     *url.URL
	entries []*altsvc.AltSvc
}

type altSvcErrorMsg struct {
	addr string
}

type altSvcProbeJob struct {
	addr    string
	url     *url.URL
	entries []*altsvc.AltSvc
	index   int
}

type altSvcProbeResult struct {
	addr      string
	url       *url.URL
	entries   []*altsvc.AltSvc
	index     int
	transport http.RoundTripper
	err       error
}

// altSvcRegistry owns H3 discovery for a Transport. hosts is a sync.Map so
// every consumer request can read it lock-free; the registry goroutine is
// the only thing that ever writes to it, so there's no contention to
// reason about on the write side either, just one goroutine processing one
// message at a time. A fixed pool of prober workers does the actual
// AddConn attempts, each bounded by proberDialTimeout, so a burst of dead
// hosts can occupy the pool for at most that long, never for the length
// of a real request's own timeout.
type altSvcRegistry struct {
	hosts sync.Map // addr string -> *altSvcEntry

	register chan altSvcRegisterMsg
	errs     chan altSvcErrorMsg
	probes   chan altSvcProbeJob
	results  chan altSvcProbeResult
	stop     chan struct{}

	t3 h3Prober
}

func newAltSvcRegistry(t3 h3Prober) *altSvcRegistry {
	r := &altSvcRegistry{
		register: make(chan altSvcRegisterMsg, registryChanSize),
		errs:     make(chan altSvcErrorMsg, registryChanSize),
		probes:   make(chan altSvcProbeJob, registryChanSize),
		results:  make(chan altSvcProbeResult, registryChanSize),
		stop:     make(chan struct{}),
		t3:       t3,
	}
	for i := 0; i < proberWorkerCount; i++ {
		go r.proberWorker()
	}
	go r.run()
	go r.evictLoop()
	return r
}

func (r *altSvcRegistry) close() {
	close(r.stop)
}

// discover is called from handleAltSvc, on whichever request's own
// goroutine just saw a fresh alt-svc header. It never blocks: the send is
// buffered, and even a full buffer only means one message is dropped, not
// that the calling request waits on anything.
func (r *altSvcRegistry) discover(addr string, u *url.URL, entries []*altsvc.AltSvc) {
	select {
	case r.register <- altSvcRegisterMsg{addr: addr, url: u, entries: entries}:
	default:
	}
}

// reportError is called from checkAltSvc when a real request's own attempt
// against an already-confirmed transport failed. Same non-blocking
// contract as discover.
func (r *altSvcRegistry) reportError(addr string) {
	select {
	case r.errs <- altSvcErrorMsg{addr: addr}:
	default:
	}
}

func (r *altSvcRegistry) run() {
	for {
		select {
		case <-r.stop:
			return

		case msg := <-r.register:
			if _, exists := r.hosts.Load(msg.addr); exists {
				continue // already tracked, in flight, confirmed, or mid-retry; nothing to do
			}
			r.hosts.Store(msg.addr, &altSvcEntry{url: msg.url, entries: msg.entries})
			r.dispatchProbe(msg.addr, msg.url, msg.entries, 0)

		case msg := <-r.errs:
			v, ok := r.hosts.Load(msg.addr)
			if !ok {
				continue
			}
			cur := v.(*altSvcEntry)
			r.handleFailure(altSvcProbeResult{addr: msg.addr, url: cur.url, entries: cur.entries, index: cur.index})

		case res := <-r.results:
			if res.err != nil {
				r.handleFailure(res)
				continue
			}
			r.hosts.Store(res.addr, &altSvcEntry{
				transport: res.transport,
				url:       res.url,
				entries:   res.entries,
				index:     res.index,
				expire:    res.entries[res.index].Expire,
			})
		}
	}
}

// handleFailure advances to the next alt-svc candidate and reprobes it, or,
// if none are left, removes the host entirely. Removing rather than
// leaving an inert "gave up" entry behind is what lets a future response
// carrying another alt-svc header start a fresh attempt: the old code left
// a dead entry in place forever, which permanently blocked ever
// rediscovering H3 for that host again, even if the underlying issue was
// transient.
func (r *altSvcRegistry) handleFailure(res altSvcProbeResult) {
	next := res.index + 1
	if next >= len(res.entries) {
		r.hosts.Delete(res.addr)
		return
	}
	r.hosts.Store(res.addr, &altSvcEntry{url: res.url, entries: res.entries})
	r.dispatchProbe(res.addr, res.url, res.entries, next)
}

func (r *altSvcRegistry) dispatchProbe(addr string, u *url.URL, entries []*altsvc.AltSvc, index int) {
	select {
	case r.probes <- altSvcProbeJob{addr: addr, url: u, entries: entries, index: index}:
	case <-r.stop:
	}
}

func (r *altSvcRegistry) proberWorker() {
	for {
		select {
		case <-r.stop:
			return
		case job := <-r.probes:
			r.probe(job)
		}
	}
}

// probe assumes job.entries[job.index] is an "h3" entry; handleAltSvc only
// ever stores entries that already passed that filter.
func (r *altSvcRegistry) probe(job altSvcProbeJob) {
	entry := job.entries[job.index]
	ctx, cancel := context.WithTimeout(context.Background(), proberDialTimeout)
	defer cancel()
	hostname := altsvcutil.ConvertURL(entry, job.url).Host
	err := r.t3.AddConn(ctx, hostname)
	r.results <- altSvcProbeResult{
		addr:      job.addr,
		url:       job.url,
		entries:   job.entries,
		index:     job.index,
		transport: r.t3,
		err:       err,
	}
}

func (r *altSvcRegistry) evictLoop() {
	ticker := time.NewTicker(evictorPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-r.stop:
			return
		case <-ticker.C:
			now := time.Now()
			r.hosts.Range(func(key, value any) bool {
				entry := value.(*altSvcEntry)
				if entry.transport != nil && !entry.expire.IsZero() && now.After(entry.expire) {
					r.hosts.Delete(key)
				}
				return true
			})
		}
	}
}

// load is the entire critical-path read: one lock-free Load, nothing else.
func (r *altSvcRegistry) load(addr string) (*altSvcEntry, bool) {
	v, ok := r.hosts.Load(addr)
	if !ok {
		return nil, false
	}
	entry := v.(*altSvcEntry)
	if entry.transport == nil {
		return nil, false // in flight, not ready; caller falls back to H2/H1
	}
	return entry, true
}
