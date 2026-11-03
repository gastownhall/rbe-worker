package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
)

// --- fixtures ---------------------------------------------------------------

func blob(n int, seed int64) []byte {
	b := make([]byte, n)
	r := rand.New(rand.NewSource(seed))
	// Half random, half zeros: compressible, like a binary.
	r.Read(b[:n/2])
	return b
}

func digestOf(b []byte) (string, int64) {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:]), int64(len(b))
}

func zstdOf(t *testing.T, b []byte) []byte {
	t.Helper()
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBetterCompression))
	if err != nil {
		t.Fatal(err)
	}
	return enc.EncodeAll(b, nil)
}

func readResource(hash string, size int64) string {
	return fmt.Sprintf("oss/compressed-blobs/zstd/%s/%d", hash, size)
}

func readReq(resource string, offset, limit int64) []byte {
	b := protowire.AppendTag(nil, 1, protowire.BytesType)
	b = protowire.AppendString(b, resource)
	if offset != 0 {
		b = protowire.AppendVarint(protowire.AppendTag(b, 2, protowire.VarintType), uint64(offset))
	}
	if limit != 0 {
		b = protowire.AppendVarint(protowire.AppendTag(b, 3, protowire.VarintType), uint64(limit))
	}
	return b
}

func respData(t *testing.T, msgs [][]byte) []byte {
	t.Helper()
	var out []byte
	for _, m := range msgs {
		for len(m) > 0 {
			num, typ, n := protowire.ConsumeTag(m)
			if n < 0 {
				t.Fatalf("bad ReadResponse")
			}
			m = m[n:]
			if num == 10 && typ == protowire.BytesType {
				v, n := protowire.ConsumeBytes(m)
				out = append(out, v...)
				m = m[n:]
				continue
			}
			m = m[protowire.ConsumeFieldValue(num, typ, m):]
		}
	}
	return out
}

func readResp(data []byte) []byte {
	return protowire.AppendBytes(protowire.AppendTag(nil, 10, protowire.BytesType), data)
}

// upstream is a fake zread host: Read answers "UPSTREAM <resource>"
// (or the status in fail), anything else echoes its request messages.
type upstream struct {
	mu    sync.Mutex
	calls []string
	fail  map[string]error
}

func (u *upstream) handle(_ any, ss grpc.ServerStream) error {
	method, _ := grpc.MethodFromServerStream(ss)
	var reqs [][]byte
	for {
		f := &frame{}
		if err := ss.RecvMsg(f); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		reqs = append(reqs, f.b)
	}
	u.mu.Lock()
	u.calls = append(u.calls, method)
	err := u.fail[method]
	u.mu.Unlock()
	if err != nil {
		return err
	}
	if method == readMethod {
		r, _ := parseReadRequest(reqs[0])
		return ss.SendMsg(&frame{b: readResp([]byte("UPSTREAM " + r.resource))})
	}
	for _, r := range reqs {
		if err := ss.SendMsg(&frame{b: r}); err != nil {
			return err
		}
	}
	return nil
}

func (u *upstream) count(method string) int {
	u.mu.Lock()
	defer u.mu.Unlock()
	n := 0
	for _, c := range u.calls {
		if c == method {
			n++
		}
	}
	return n
}

// r2 is a fake hot tier: objects by name, or a behaviour.
type r2 struct {
	mu      sync.Mutex
	objects map[string][]byte
	gets    map[string]int
	mode    map[string]string // name -> "slow", "500", "403", "cut"
	delay   time.Duration
}

func (r *r2) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	name := strings.TrimPrefix(req.URL.Path, "/h/oss/")
	r.mu.Lock()
	r.gets[name]++
	obj, ok := r.objects[name]
	mode := r.mode[name]
	delay := r.delay
	r.mu.Unlock()
	switch mode {
	case "500":
		http.Error(w, "boom", 500)
		return
	case "403":
		http.Error(w, "waf", 403)
		return
	case "slow":
		select {
		case <-time.After(delay):
		case <-req.Context().Done():
			return
		}
	}
	if !ok {
		http.NotFound(w, req)
		return
	}
	if mode == "cut" {
		// Promise the whole object, send half, drop the connection.
		w.Header().Set("Content-Length", fmt.Sprint(len(obj)))
		w.WriteHeader(200)
		w.Write(obj[:len(obj)/2])
		w.(http.Flusher).Flush()
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			conn.Close()
		}
		return
	}
	w.Header().Set("Content-Type", "application/zstd")
	w.Write(obj)
}

func (r *r2) put(name string, b []byte) {
	r.mu.Lock()
	r.objects[name] = b
	r.mu.Unlock()
}

func (r *r2) getCount(name string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.gets[name]
}

// promoter is a fake core promoter: answer(req) decides.
type promoter struct {
	mu     sync.Mutex
	reqs   []map[string]any
	answer func(req map[string]any) (int, string, any)
}

func (p *promoter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var req map[string]any
	json.NewDecoder(r.Body).Decode(&req)
	p.mu.Lock()
	p.reqs = append(p.reqs, req)
	p.mu.Unlock()
	code, ctype, body := p.answer(req)
	w.Header().Set("Content-Type", ctype)
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(body)
}

func (p *promoter) requests() []map[string]any {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]map[string]any(nil), p.reqs...)
}

type rig struct {
	up   *upstream
	r2   *r2
	prom *promoter
	srv  *server
	conn *grpc.ClientConn
}

func newRig(t *testing.T, tweak func(*config)) *rig {
	t.Helper()
	rg := &rig{
		up: &upstream{fail: map[string]error{}},
		r2: &r2{objects: map[string][]byte{}, gets: map[string]int{}, mode: map[string]string{}},
		prom: &promoter{answer: func(map[string]any) (int, string, any) {
			return 503, "application/json", map[string]string{"state": "disabled"}
		}},
	}
	// Upstream.
	ul, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ug := grpc.NewServer(grpc.ForceServerCodec(rawCodec{}), grpc.UnknownServiceHandler(rg.up.handle))
	go ug.Serve(ul)
	t.Cleanup(ug.Stop)
	r2srv := httptest.NewServer(rg.r2)
	t.Cleanup(r2srv.Close)
	psrv := httptest.NewServer(rg.prom)
	t.Cleanup(psrv.Close)
	cfg := config{
		upstream: ul.Addr().String(), hot: r2srv.URL + "/h/oss", promote: psrv.URL + promotePath,
		tmp: t.TempDir(), min: 1024, max: 1 << 30, inflight: 4, conns: 2, r2Fails: 5, promoteFails: 3,
		r2Timeout: 2 * time.Second, r2MBps: 50, promoteWaitMax: 5 * time.Second, breakerOpen: time.Minute,
	}
	if tweak != nil {
		tweak(&cfg)
	}
	var ups []grpc.ClientConnInterface
	for i := 0; i < cfg.conns; i++ {
		cc, err := grpc.NewClient("passthrough:///"+cfg.upstream, grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.ForceCodec(rawCodec{})))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { cc.Close() })
		ups = append(ups, cc)
	}
	rg.srv = newServer(cfg, ups, r2srv.Client(), psrv.Client())
	rg.srv.statfsFree = func(string) (uint64, error) { return 1 << 50, nil }
	sl, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := rg.srv.grpcServer()
	go g.Serve(sl)
	t.Cleanup(g.Stop)
	rg.conn, err = grpc.NewClient("passthrough:///"+sl.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(rawCodec{}), grpc.MaxCallRecvMsgSize(maxMsg)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rg.conn.Close() })
	return rg
}

// call sends one request message on method and returns the response messages.
func (rg *rig) call(t *testing.T, method string, req []byte) ([][]byte, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cs, err := rg.conn.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true, ClientStreams: true}, method)
	if err != nil {
		t.Fatal(err)
	}
	if err := cs.SendMsg(&frame{b: req}); err != nil {
		t.Fatal(err)
	}
	cs.CloseSend()
	var out [][]byte
	for {
		f := &frame{}
		if err := cs.RecvMsg(f); err != nil {
			if err == io.EOF {
				return out, nil
			}
			return out, err
		}
		out = append(out, f.b)
	}
}

func (rg *rig) read(t *testing.T, resource string, offset, limit int64) []byte {
	t.Helper()
	msgs, err := rg.call(t, readMethod, readReq(resource, offset, limit))
	if err != nil {
		t.Fatalf("Read %s: %v", resource, err)
	}
	return respData(t, msgs)
}

func upstreamAnswer(resource string) []byte { return []byte("UPSTREAM " + resource) }

// --- behaviour -------------------------------------------------------------

func TestHitServesTheVerifiedObject(t *testing.T) {
	rg := newRig(t, nil)
	b := blob(300<<10, 1)
	h, n := digestOf(b)
	z := zstdOf(t, b)
	rg.r2.put(objectName(h, n), z)
	got := rg.read(t, readResource(h, n), 0, 0)
	if !bytes.Equal(got, z) {
		t.Fatalf("got %d bytes, want the object's %d compressed bytes", len(got), len(z))
	}
	if rg.up.count(readMethod) != 0 {
		t.Error("a hit reached the upstream")
	}
	dec, _ := zstd.NewReader(nil)
	if raw, err := dec.DecodeAll(got, nil); err != nil || !bytes.Equal(raw, b) {
		t.Error("served bytes do not decode to the blob")
	}
	if r := rg.srv.report(); !strings.Contains(r, "r2_hits=1 ") || !strings.Contains(r, fmt.Sprintf("raw_bytes=%d ", n)) {
		t.Errorf("report %q", r)
	}
}

func TestLegacyAndSha256ResourceForms(t *testing.T) {
	rg := newRig(t, nil)
	b := blob(64<<10, 2)
	h, n := digestOf(b)
	z := zstdOf(t, b)
	rg.r2.put(objectName(h, n), z)
	for _, res := range []string{
		fmt.Sprintf("oss-fork/compressed-blobs/zstd/sha256/%s/%d", h, n),
		fmt.Sprintf("compressed-blobs/zstd/%s/%d", h, n),
	} {
		if got := rg.read(t, res, 0, 0); !bytes.Equal(got, z) {
			t.Errorf("%s: not served from R2", res)
		}
	}
}

func Test404PromoteThenHit(t *testing.T) {
	rg := newRig(t, nil)
	b := blob(200<<10, 3)
	h, n := digestOf(b)
	z := zstdOf(t, b)
	rg.prom.answer = func(req map[string]any) (int, string, any) {
		rg.r2.put(objectName(h, n), z)
		return 200, "application/json", map[string]string{"key": "h/oss/" + objectName(h, n)}
	}
	got := rg.read(t, readResource(h, n), 0, 0)
	if !bytes.Equal(got, z) {
		t.Fatal("not served from R2 after the promotion")
	}
	reqs := rg.prom.requests()
	if len(reqs) != 1 || reqs[0]["hash"] != h || reqs[0]["size"].(float64) != float64(n) || reqs[0]["seen_404"] != true {
		t.Errorf("promote requests %v", reqs)
	}
	if w := reqs[0]["wait_ms"].(float64); w < 2000 || w > 5000 {
		t.Errorf("wait_ms %v, want 2 s + size/40e3 capped at -promote-wait-max", w)
	}
	if rg.r2.getCount(objectName(h, n)) != 2 {
		t.Errorf("R2 GETs %d, want 2 (404, then the object)", rg.r2.getCount(objectName(h, n)))
	}
	if r := rg.srv.report(); !strings.Contains(r, "promoted=1 ") || !strings.Contains(r, "r2_404=1 ") {
		t.Errorf("report %q", r)
	}
}

func Test404PromoteRefusedFallsBack(t *testing.T) {
	for _, c := range []struct {
		name  string
		code  int
		ctype string
		body  any
	}{
		{"budget", 429, "application/json", map[string]string{"state": "budget"}},
		{"pending", 202, "application/json", map[string]string{"state": "pending"}},
		{"disabled", 503, "application/json", map[string]string{"state": "disabled"}},
		{"not-in-tier", 404, "application/json", map[string]string{}},
		// Caddy's answer where the route is not deployed: 200, gRPC refusal.
		{"caddy-refusal", 200, "application/grpc", map[string]string{}},
		// A 200 for another key is no promotion of this one.
		{"wrong-key", 200, "application/json", map[string]string{"key": "h/oss/other-1.zst"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			rg := newRig(t, nil)
			b := blob(100<<10, 4)
			h, n := digestOf(b)
			rg.prom.answer = func(map[string]any) (int, string, any) { return c.code, c.ctype, c.body }
			res := readResource(h, n)
			if got := rg.read(t, res, 0, 0); !bytes.Equal(got, upstreamAnswer(res)) {
				t.Fatalf("got %q, want the upstream's answer", got)
			}
			if rg.r2.getCount(objectName(h, n)) != 1 {
				t.Errorf("R2 GETs %d, want 1 (no second GET without a promotion)", rg.r2.getCount(objectName(h, n)))
			}
		})
	}
}

func TestCorruptObjectFallsBackAndIsReported(t *testing.T) {
	rg := newRig(t, nil)
	b := blob(256<<10, 5)
	h, n := digestOf(b)
	bad := append([]byte(nil), b...)
	bad[len(bad)-1] ^= 0xff // same size, other content
	rg.r2.put(objectName(h, n), zstdOf(t, bad))
	rg.prom.answer = func(map[string]any) (int, string, any) {
		return 200, "application/json", map[string]string{"key": "h/oss/" + objectName(h, n)}
	}
	res := readResource(h, n)
	// Not one R2 byte before verification: the answer is the upstream's alone.
	if got := rg.read(t, res, 0, 0); !bytes.Equal(got, upstreamAnswer(res)) {
		t.Fatalf("got %d bytes, want the upstream's answer alone", len(got))
	}
	// The second read never tries R2 for this key.
	if got := rg.read(t, res, 0, 0); !bytes.Equal(got, upstreamAnswer(res)) {
		t.Fatal("second read not from the upstream")
	}
	if g := rg.r2.getCount(objectName(h, n)); g != 1 {
		t.Errorf("R2 GETs %d, want 1 (a key that failed is never read again)", g)
	}
	r := rg.srv.report() // waits for the background report
	if !strings.Contains(r, "verify_fail=1 ") {
		t.Errorf("report %q", r)
	}
	reqs := rg.prom.requests()
	if len(reqs) != 1 || reqs[0]["bad"] != true || reqs[0]["hash"] != h {
		t.Errorf("want one bad=true report to the promoter, got %v", reqs)
	}
}

func TestWrongDigestObjectFallsBack(t *testing.T) {
	rg := newRig(t, nil)
	b := blob(128<<10, 6)
	h, n := digestOf(b)
	other := blob(64<<10, 7) // a valid object of another blob (stale or swapped)
	rg.r2.put(objectName(h, n), zstdOf(t, other))
	res := readResource(h, n)
	if got := rg.read(t, res, 0, 0); !bytes.Equal(got, upstreamAnswer(res)) {
		t.Fatal("a wrong object was served")
	}
	if r := rg.srv.report(); !strings.Contains(r, "verify_fail=1 ") {
		t.Errorf("report %q", r)
	}
}

func TestLongerObjectFallsBack(t *testing.T) {
	rg := newRig(t, nil)
	b := blob(128<<10, 8)
	h, n := digestOf(b)
	rg.r2.put(objectName(h, n), zstdOf(t, append(append([]byte(nil), b...), 0)))
	res := readResource(h, n)
	if got := rg.read(t, res, 0, 0); !bytes.Equal(got, upstreamAnswer(res)) {
		t.Fatal("an object decoding past the size was served")
	}
}

func TestTrailingGarbageFallsBack(t *testing.T) {
	rg := newRig(t, nil)
	b := blob(128<<10, 9)
	h, n := digestOf(b)
	rg.r2.put(objectName(h, n), append(zstdOf(t, b), []byte("tail")...))
	res := readResource(h, n)
	if got := rg.read(t, res, 0, 0); !bytes.Equal(got, upstreamAnswer(res)) {
		t.Fatal("an object with bytes past its frames was served")
	}
}

func TestTruncatedFramesFallBack(t *testing.T) {
	rg := newRig(t, nil)
	b := blob(512<<10, 10)
	h, n := digestOf(b)
	z := zstdOf(t, b)
	rg.r2.put(objectName(h, n), z[:len(z)*2/3]) // a whole HTTP body, a partial object
	res := readResource(h, n)
	if got := rg.read(t, res, 0, 0); !bytes.Equal(got, upstreamAnswer(res)) {
		t.Fatal("a partial object was served")
	}
	if r := rg.srv.report(); !strings.Contains(r, "verify_fail=1 ") {
		t.Errorf("report %q", r)
	}
}

func TestConnectionCutMidObjectFallsBack(t *testing.T) {
	rg := newRig(t, nil)
	b := blob(512<<10, 11)
	h, n := digestOf(b)
	rg.r2.put(objectName(h, n), zstdOf(t, b))
	rg.r2.mode[objectName(h, n)] = "cut"
	res := readResource(h, n)
	if got := rg.read(t, res, 0, 0); !bytes.Equal(got, upstreamAnswer(res)) {
		t.Fatal("a cut transfer was served")
	}
	r := rg.srv.report()
	if !strings.Contains(r, "r2_errors=1 ") || !strings.Contains(r, "verify_fail=0 ") {
		t.Errorf("a cut transfer is an R2 error, not a verification failure: %q", r)
	}
}

func TestSlowR2FallsBackWithinTheTimeLimit(t *testing.T) {
	rg := newRig(t, func(c *config) { c.r2Timeout = 300 * time.Millisecond; c.r2MBps = 1e6 })
	b := blob(64<<10, 12)
	h, n := digestOf(b)
	rg.r2.put(objectName(h, n), zstdOf(t, b))
	rg.r2.mode[objectName(h, n)] = "slow"
	rg.r2.delay = 10 * time.Second
	res := readResource(h, n)
	t0 := time.Now()
	if got := rg.read(t, res, 0, 0); !bytes.Equal(got, upstreamAnswer(res)) {
		t.Fatal("not the upstream's answer")
	}
	if d := time.Since(t0); d > 3*time.Second {
		t.Errorf("slow R2 held the read %v (limit 300 ms + size / rate)", d)
	}
}

func TestR2BreakerBypassesR2(t *testing.T) {
	rg := newRig(t, func(c *config) { c.r2Fails = 3 })
	var res []string
	for i := 0; i < 6; i++ {
		b := blob(32<<10, int64(100+i))
		h, n := digestOf(b)
		rg.r2.mode[objectName(h, n)] = "500"
		res = append(res, readResource(h, n))
	}
	for _, r := range res {
		if got := rg.read(t, r, 0, 0); !bytes.Equal(got, upstreamAnswer(r)) {
			t.Fatal("not the upstream's answer")
		}
	}
	total := 0
	rg.r2.mu.Lock()
	for _, g := range rg.r2.gets {
		total += g
	}
	rg.r2.mu.Unlock()
	if total != 3 {
		t.Errorf("R2 GETs %d, want 3 (then the breaker bypasses R2)", total)
	}
	if r := rg.srv.report(); !strings.Contains(r, "r2_breaker_opened=1") || !strings.Contains(r, "bypassed=3 ") {
		t.Errorf("report %q", r)
	}
}

func TestWAFRefusalIsAnR2ErrorNotAPromotion(t *testing.T) {
	rg := newRig(t, nil)
	b := blob(32<<10, 13)
	h, n := digestOf(b)
	rg.r2.mode[objectName(h, n)] = "403"
	res := readResource(h, n)
	if got := rg.read(t, res, 0, 0); !bytes.Equal(got, upstreamAnswer(res)) {
		t.Fatal("not the upstream's answer")
	}
	if len(rg.prom.requests()) != 0 {
		t.Error("a 403 (WAF) asked the core to promote")
	}
}

func TestPromoteBreaker(t *testing.T) {
	rg := newRig(t, func(c *config) { c.promoteFails = 2 })
	rg.prom.answer = func(map[string]any) (int, string, any) { return 502, "text/plain", "bad gateway" }
	for i := 0; i < 4; i++ {
		b := blob(32<<10, int64(200+i))
		h, n := digestOf(b)
		res := readResource(h, n)
		if got := rg.read(t, res, 0, 0); !bytes.Equal(got, upstreamAnswer(res)) {
			t.Fatal("not the upstream's answer")
		}
	}
	if got := len(rg.prom.requests()); got != 2 {
		t.Errorf("promote requests %d, want 2 (then the breaker bypasses promotion)", got)
	}
}

func TestPromoterDownFallsBack(t *testing.T) {
	rg := newRig(t, func(c *config) { c.promote = "http://127.0.0.1:1" + promotePath })
	b := blob(32<<10, 14)
	h, n := digestOf(b)
	res := readResource(h, n)
	if got := rg.read(t, res, 0, 0); !bytes.Equal(got, upstreamAnswer(res)) {
		t.Fatal("not the upstream's answer")
	}
}

func TestReadsThatNeverTouchR2(t *testing.T) {
	rg := newRig(t, nil)
	b := blob(64<<10, 15)
	h, n := digestOf(b)
	rg.r2.put(objectName(h, n), zstdOf(t, b))
	small := blob(100, 16)
	sh, sn := digestOf(small)
	for _, c := range []struct {
		name, res     string
		offset, limit int64
	}{
		{"offset", readResource(h, n), 1, 0},
		{"limit", readResource(h, n), 0, 10},
		{"identity", fmt.Sprintf("oss/blobs/%s/%d", h, n), 0, 0},
		{"small", readResource(sh, sn), 0, 0},
		{"other-compressor", fmt.Sprintf("oss/compressed-blobs/deflate/%s/%d", h, n), 0, 0},
		{"bad-hash", fmt.Sprintf("oss/compressed-blobs/zstd/%s/%d", strings.ToUpper(h), n), 0, 0},
	} {
		if got := rg.read(t, c.res, c.offset, c.limit); !bytes.Equal(got, upstreamAnswer(c.res)) {
			t.Errorf("%s: got %q, want the upstream's answer", c.name, got)
		}
	}
	if g := rg.r2.getCount(objectName(h, n)); g != 0 {
		t.Errorf("R2 GETs %d, want 0", g)
	}
}

func TestOversizeIsNotReadFromR2(t *testing.T) {
	rg := newRig(t, func(c *config) { c.max = 32 << 10 })
	b := blob(64<<10, 17)
	h, n := digestOf(b)
	rg.r2.put(objectName(h, n), zstdOf(t, b))
	res := readResource(h, n)
	if got := rg.read(t, res, 0, 0); !bytes.Equal(got, upstreamAnswer(res)) {
		t.Fatal("a blob above -max came from R2")
	}
}

func TestLowDiskBypassesR2(t *testing.T) {
	rg := newRig(t, nil)
	rg.srv.statfsFree = func(string) (uint64, error) { return 1 << 20, nil }
	b := blob(64<<10, 18)
	h, n := digestOf(b)
	rg.r2.put(objectName(h, n), zstdOf(t, b))
	res := readResource(h, n)
	if got := rg.read(t, res, 0, 0); !bytes.Equal(got, upstreamAnswer(res)) {
		t.Fatal("R2 used with no disk to verify on")
	}
}

func TestFindMissingBlobsPassesThroughUnchanged(t *testing.T) {
	rg := newRig(t, nil)
	req := []byte{0x0a, 0x03, 'o', 's', 's', 0x12, 0x02, 0x0a, 0x00}
	msgs, err := rg.call(t, "/build.bazel.remote.execution.v2.ContentAddressableStorage/FindMissingBlobs", req)
	if err != nil || len(msgs) != 1 || !bytes.Equal(msgs[0], req) {
		t.Fatalf("FindMissingBlobs relayed as %v, %v; want the upstream's echo", msgs, err)
	}
}

func TestUpstreamStatusIsRelayed(t *testing.T) {
	rg := newRig(t, nil)
	rg.up.fail[readMethod] = status.Error(codes.NotFound, "no such blob")
	b := blob(100, 19)
	h, n := digestOf(b)
	_, err := rg.call(t, readMethod, readReq(readResource(h, n), 0, 0))
	if status.Code(err) != codes.NotFound || !strings.Contains(err.Error(), "no such blob") {
		t.Fatalf("got %v, want the upstream's NotFound", err)
	}
}

func TestRepeatDigests(t *testing.T) {
	rg := newRig(t, nil)
	b := blob(64<<10, 20)
	h, n := digestOf(b)
	rg.r2.put(objectName(h, n), zstdOf(t, b))
	for i := 0; i < 3; i++ {
		rg.read(t, readResource(h, n), 0, 0)
	}
	if r := rg.srv.report(); !strings.Contains(r, "repeat_digests=2 ") || !strings.Contains(r, fmt.Sprintf("repeat_bytes=%d", 2*n)) {
		t.Errorf("report %q", r)
	}
}

// The core compresses with the zstd CLI (rbe-hot.py: zstd -q -9 -T2 -c):
// its multithreaded output must verify here like the library's.
func TestZstdCLIObjectVerifies(t *testing.T) {
	if _, err := exec.LookPath("zstd"); err != nil {
		t.Skip("no zstd CLI")
	}
	rg := newRig(t, nil)
	b := blob(3<<20, 21)
	h, n := digestOf(b)
	cmd := exec.Command("zstd", "-q", "-9", "-T2", "-c")
	cmd.Stdin = bytes.NewReader(b)
	z, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	rg.r2.put(objectName(h, n), z)
	if got := rg.read(t, readResource(h, n), 0, 0); !bytes.Equal(got, z) {
		t.Fatal("the CLI's object was not served")
	}
}

// --- parsing and flags ----------------------------------------------------

func TestParseCompressed(t *testing.T) {
	h := strings.Repeat("ab", 32)
	for name, want := range map[string]bool{
		"oss/compressed-blobs/zstd/" + h + "/5":        true,
		"oss/compressed-blobs/zstd/sha256/" + h + "/5": true,
		"compressed-blobs/zstd/" + h + "/5":            true,
		"a/b/compressed-blobs/zstd/" + h + "/5":        true,
		"oss/compressed-blobs/zstd/" + h + "/05":       false,
		"oss/compressed-blobs/zstd/" + h + "/-1":       false,
		"oss/compressed-blobs/zstd/" + h[:63] + "/5":   false,
		"oss/compressed-blobs/zstd/" + h + "/5/x":      false,
		"oss/compressed-blobs/zstd/blake3/" + h + "/5": false,
		"oss/blobs/" + h + "/5":                        false,
	} {
		if _, ok := parseCompressed(name); ok != want {
			t.Errorf("parseCompressed(%q) = %v, want %v", name, ok, want)
		}
	}
}

func TestParseFlags(t *testing.T) {
	base := []string{"-upstream", "rbe-fork-zread.ops.gascity.com:8444", "-cert", "c", "-key", "k", "-tmp", "/tmp/x",
		"-hot", "https://rbe-warm.ops.gascity.com/h/fork", "-promote", "https://rbe-fork-zread.ops.gascity.com:8444" + promotePath}
	if _, err := parseFlags(base); err != nil {
		t.Fatalf("valid flags refused: %v", err)
	}
	for name, extra := range map[string][]string{
		"non-loopback listen": {"-listen", "0.0.0.0:50090"},
		"public listen":       {"-listen", "10.0.0.1:50090"},
		"http hot":            {"-hot", "http://rbe-warm.ops.gascity.com/h/fork"},
		"hot outside h/":      {"-hot", "https://rbe-warm.ops.gascity.com/chunks"},
		"promote path":        {"-promote", "https://rbe-fork-zread.ops.gascity.com:8444/x"},
		"min above max":       {"-min", "10", "-max", "5"},
	} {
		if _, err := parseFlags(append(append([]string(nil), base...), extra...)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
