package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/klauspost/compress/zstd"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/encoding/protowire"
)

const (
	readMethod  = "/google.bytestream.ByteStream/Read"
	promotePath = "/rbe.hot.v1.Hot/Promote"
	maxMsg      = 64 << 20
	chunk       = 1 << 20
	// The decoder's window cap: zstd -9 uses 4 MiB at most, so a bigger one
	// is not the core's object (and could make the decoder allocate).
	maxWindow = 128 << 20
)

// rawCodec passes messages through as bytes: the proxy never decodes what it
// relays, so an upstream answer reaches NativeLink exactly as sent.
type rawCodec struct{}

type frame struct{ b []byte }

func (rawCodec) Marshal(v any) ([]byte, error) {
	f, ok := v.(*frame)
	if !ok {
		return nil, fmt.Errorf("rawCodec: %T", v)
	}
	return f.b, nil
}

func (rawCodec) Unmarshal(data []byte, v any) error {
	f, ok := v.(*frame)
	if !ok {
		return fmt.Errorf("rawCodec: %T", v)
	}
	f.b = append(f.b[:0], data...)
	return nil
}

func (rawCodec) Name() string { return "proto" }

// breaker opens after limit consecutive failures and stays open for d.
type breaker struct {
	mu         sync.Mutex
	fails, max int
	d          time.Duration
	until      time.Time
	opened     int
	now        func() time.Time
}

func (b *breaker) allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return !b.now().Before(b.until)
}

func (b *breaker) fail() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.fails++
	if b.fails >= b.max {
		b.until, b.fails = b.now().Add(b.d), 0
		b.opened++
	}
}

func (b *breaker) ok() {
	b.mu.Lock()
	b.fails = 0
	b.mu.Unlock()
}

type stats struct {
	reads, big, r2Hits, r2Bytes, rawBytes, promoted, r2NotFound, r2Errors  atomic.Int64
	notFoundAfterPromote, busy, localErrors                                atomic.Int64
	fallbacks, verifyFail, bypassed, coreBytes, repeatDigests, repeatBytes atomic.Int64
}

type server struct {
	cfg        config
	up         []grpc.ClientConnInterface
	next       atomic.Uint64
	r2c, pc    *http.Client
	sem        chan struct{}
	r2, prom   *breaker
	st         stats
	mu         sync.Mutex
	bad        map[string]bool // keys whose object failed verification: never read again
	seen       map[string]bool // big digests read (repeat_*)
	waits      []float64       // promotion waits, seconds (bounded)
	reportWG   sync.WaitGroup
	statfsFree func(dir string) (uint64, error)
}

func newServer(cfg config, up []grpc.ClientConnInterface, r2c, pc *http.Client) *server {
	now := time.Now
	return &server{
		cfg: cfg, up: up, r2c: r2c, pc: pc,
		sem:        make(chan struct{}, cfg.inflight),
		r2:         &breaker{max: cfg.r2Fails, d: cfg.breakerOpen, now: now},
		prom:       &breaker{max: cfg.promoteFails, d: cfg.breakerOpen, now: now},
		bad:        map[string]bool{},
		seen:       map[string]bool{},
		statfsFree: statfsFree,
	}
}

func statfsFree(dir string) (uint64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, err
	}
	return st.Bavail * uint64(st.Bsize), nil
}

func (s *server) grpcServer(extra ...grpc.ServerOption) *grpc.Server {
	return grpc.NewServer(append([]grpc.ServerOption{
		// NativeLink's GrpcStore pings every 30 s, idle or not
		// (tls_utils.rs: keep_alive_while_idle). grpc-go's default policy
		// (5 min, no pings without a stream) answers that with GOAWAY
		// too_many_pings after about 2 idle minutes.
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 5 * time.Second, PermitWithoutStream: true}),
		grpc.ForceServerCodec(rawCodec{}),
		grpc.UnknownServiceHandler(s.handle),
		grpc.MaxRecvMsgSize(maxMsg), grpc.MaxSendMsgSize(maxMsg),
		grpc.InitialWindowSize(8 << 20), grpc.InitialConnWindowSize(64 << 20),
	}, extra...)...)
}

// handle serves every method: Read may come from R2, the rest is relayed.
func (s *server) handle(_ any, ss grpc.ServerStream) error {
	method, ok := grpc.MethodFromServerStream(ss)
	if !ok {
		return errors.New("rbe-hotread: no method")
	}
	if method == readMethod {
		return s.read(ss)
	}
	return s.proxy(ss, method, nil, nil)
}

// digest is a compressed read's blob.
type digest struct {
	hash, key string
	size      int64
}

type readRequest struct {
	resource      string
	offset, limit int64
}

func parseReadRequest(b []byte) (readRequest, error) {
	var r readRequest
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return r, protowire.ParseError(n)
		}
		b = b[n:]
		switch {
		case num == 1 && typ == protowire.BytesType:
			v, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return r, protowire.ParseError(n)
			}
			r.resource, b = string(v), b[n:]
		case (num == 2 || num == 3) && typ == protowire.VarintType:
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return r, protowire.ParseError(n)
			}
			if num == 2 {
				r.offset = int64(v)
			} else {
				r.limit = int64(v)
			}
			b = b[n:]
		default:
			n := protowire.ConsumeFieldValue(num, typ, b)
			if n < 0 {
				return r, protowire.ParseError(n)
			}
			b = b[n:]
		}
	}
	return r, nil
}

// parseCompressed: {instance/}compressed-blobs/zstd/{sha256/}HASH/SIZE.
func parseCompressed(name string) (digest, bool) {
	p := strings.Split(name, "/")
	i := 0
	for i < len(p) && p[i] != "compressed-blobs" {
		i++
	}
	p = p[min(i, len(p)):]
	if len(p) == 5 && p[2] == "sha256" {
		p = append(p[:2], p[3:]...)
	}
	if len(p) != 4 || p[0] != "compressed-blobs" || p[1] != "zstd" || !isHex64(p[2]) {
		return digest{}, false
	}
	size, err := strconv.ParseInt(p[3], 10, 64)
	if err != nil || size < 0 || p[3] != strconv.FormatInt(size, 10) {
		return digest{}, false
	}
	return digest{hash: p[2], size: size, key: fmt.Sprintf("%s-%d", p[2], size)}, true
}

func isHex64(h string) bool {
	if len(h) != 64 {
		return false
	}
	for _, c := range h {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func (s *server) read(ss grpc.ServerStream) error {
	first := &frame{}
	if err := ss.RecvMsg(first); err != nil {
		return err
	}
	s.st.reads.Add(1)
	req, err := parseReadRequest(first.b)
	d, ok := parseCompressed(req.resource)
	if err == nil && ok && req.offset == 0 && req.limit == 0 && d.size >= s.cfg.min && d.size <= s.cfg.max {
		s.st.big.Add(1)
		s.noteRepeat(d)
		served, err := s.tryHot(ss, d)
		if served {
			return err
		}
		s.st.fallbacks.Add(1)
	}
	return s.proxy(ss, readMethod, first, &s.st.coreBytes)
}

func (s *server) noteRepeat(d digest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.seen[d.key] {
		s.st.repeatDigests.Add(1)
		s.st.repeatBytes.Add(d.size)
	}
	s.seen[d.key] = true
}

type result int

const (
	resOK result = iota
	resNotFound
	resR2Error
	resVerifyFail
	resLocal
)

// fileWriter keeps the temp file's first write error apart from the
// decoder's: a full disk is no verification failure.
type fileWriter struct {
	f   *os.File
	err error
}

func (w *fileWriter) Write(p []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	n, err := w.f.Write(p)
	w.err = err
	return n, err
}

// tryHot serves d from R2 when it can. served false: nothing was sent, the
// caller proxies; served true: the response went out (err: sending failed).
func (s *server) tryHot(ss grpc.ServerStream, d digest) (served bool, err error) {
	s.mu.Lock()
	bad := s.bad[d.key]
	s.mu.Unlock()
	if bad || !s.r2.allow() {
		s.st.bypassed.Add(1)
		return false, nil
	}
	if free, err := s.statfsFree(s.cfg.tmp); err != nil || free < uint64(2*d.size)+10<<30 {
		s.st.bypassed.Add(1)
		return false, nil
	}
	ctx := ss.Context()
	// A free fetch slot within -slot-wait, else today's path at once.
	wait := time.NewTimer(s.cfg.slotWait)
	select {
	case s.sem <- struct{}{}:
		wait.Stop()
	case <-wait.C:
		s.st.busy.Add(1)
		return false, nil
	case <-ctx.Done():
		wait.Stop()
		return false, nil
	}
	defer func() { <-s.sem }()
	f, zn, res := s.fetch(ctx, d)
	if res == resNotFound && s.promoteOnce(ctx, d, false) {
		f, zn, res = s.fetch(ctx, d)
		if res == resNotFound {
			// The core said it is in R2 and R2 says it is not: a stale
			// (edge-cached) 404 or a lost upload. Should stay about 0.
			s.st.notFoundAfterPromote.Add(1)
		}
	}
	switch res {
	case resOK:
	case resNotFound:
		return false, nil
	case resVerifyFail:
		s.markBad(d)
		return false, nil
	default:
		return false, nil
	}
	defer f.Close()
	buf := make([]byte, chunk)
	for {
		n, rerr := f.Read(buf)
		if n > 0 {
			msg := protowire.AppendBytes(protowire.AppendTag(make([]byte, 0, n+8), 10, protowire.BytesType), buf[:n])
			if err := ss.SendMsg(&frame{b: msg}); err != nil {
				return true, err
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			// The verified file on this VM's disk failed to read back, after
			// the first bytes went out: nothing to fall back to. NativeLink
			// sees an error and retries the fetch.
			return true, rerr
		}
	}
	s.st.r2Hits.Add(1)
	s.st.r2Bytes.Add(zn)
	s.st.rawBytes.Add(d.size)
	return true, nil
}

// markBad: this VM never reads the key from R2 again, and tells the core
// (once, in the background) so it republishes from its own verified copy.
func (s *server) markBad(d digest) {
	s.st.verifyFail.Add(1)
	s.mu.Lock()
	first := !s.bad[d.key]
	s.bad[d.key] = true
	s.mu.Unlock()
	if first {
		s.reportWG.Add(1)
		go func() {
			defer s.reportWG.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			s.promoteOnce(ctx, d, true)
		}()
	}
}

// maxCompressed bounds an object's size: zstd's own worst case is under
// size + size/128 + 64 KiB.
func maxCompressed(size int64) int64 { return size + size/128 + 64<<10 }

type bodyReader struct {
	r   io.Reader
	err error
	n   int64
}

func (b *bodyReader) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	b.n += int64(n)
	if err != nil && err != io.EOF {
		b.err = err
	}
	return n, err
}

type hashCounter struct {
	h   io.Writer
	n   int64
	max int64
}

var errTooLong = errors.New("decoded past the digest's size")

func (h *hashCounter) Write(p []byte) (int, error) {
	if h.n+int64(len(p)) > h.max {
		return 0, errTooLong
	}
	h.n += int64(len(p))
	return h.h.Write(p)
}

// fetch GETs d's object into an unlinked temp file, decoding and hashing it
// as it arrives. resOK: the file (at offset 0) holds the compressed bytes,
// and they decode to exactly d.
func (s *server) fetch(ctx context.Context, d digest) (*os.File, int64, result) {
	timeout := s.cfg.r2Timeout + time.Duration(float64(d.size)/(s.cfg.r2MBps*1e6)*float64(time.Second))
	parent := ctx
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	// A failure after NativeLink gave up on the read is no R2 error.
	r2Error := func() {
		if parent.Err() == nil {
			s.r2Error()
		}
	}
	// A query string of its own on every GET: Cloudflare's cache keys on it,
	// so no edge copy (a 404 from before the promotion above all) answers
	// this read; R2 ignores it (the B3 pre-check confirms both).
	var q [8]byte
	rand.Read(q[:])
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		s.cfg.hot+"/"+objectName(d.hash, d.size)+"?r="+hex.EncodeToString(q[:]), nil)
	if err != nil {
		r2Error()
		return nil, 0, resR2Error
	}
	resp, err := s.r2c.Do(req)
	if err != nil {
		r2Error()
		return nil, 0, resR2Error
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusNotFound:
		s.r2.ok()
		s.st.r2NotFound.Add(1)
		return nil, 0, resNotFound
	case resp.StatusCode != http.StatusOK:
		r2Error()
		return nil, 0, resR2Error
	}
	zmax := maxCompressed(d.size)
	if resp.ContentLength > zmax {
		return s.verifyFailed(nil)
	}
	f, err := os.CreateTemp(s.cfg.tmp, d.key+".*.zst")
	if err != nil {
		s.st.localErrors.Add(1)
		return nil, 0, resLocal // this VM's disk: not R2's fault, no breaker
	}
	os.Remove(f.Name()) // the fd alone: nothing is left behind, whatever happens
	body := &bodyReader{r: io.LimitReader(resp.Body, zmax+1)}
	fw := &fileWriter{f: f}
	tee := io.TeeReader(body, fw)
	dec, err := zstd.NewReader(tee, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxWindow(maxWindow),
		zstd.WithDecoderMaxMemory(uint64(d.size)+maxWindow))
	if err != nil {
		f.Close()
		return nil, 0, resR2Error
	}
	h := sha256.New()
	hc := &hashCounter{h: h, max: d.size}
	_, derr := io.Copy(hc, dec)
	dec.Close()
	// Whatever follows the frames is not the core's object either.
	extra, _ := io.Copy(io.Discard, tee)
	switch {
	case fw.err != nil:
		// This VM's disk (full, an IO error): neither R2's fault nor the
		// object's. No breaker, no bad mark: today's path for this read.
		f.Close()
		s.st.localErrors.Add(1)
		return nil, 0, resLocal
	case body.err != nil || ctx.Err() != nil:
		// Truncated, reset or slow: R2's (or the path's) fault.
		f.Close()
		r2Error()
		return nil, 0, resR2Error
	case derr != nil || extra > 0 || body.n > zmax || hc.n != d.size || hex.EncodeToString(h.Sum(nil)) != d.hash:
		return s.verifyFailed(f)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		f.Close()
		return nil, 0, resR2Error
	}
	s.r2.ok()
	return f, body.n, resOK
}

func (s *server) r2Error() {
	s.st.r2Errors.Add(1)
	s.r2.fail()
}

// verifyFailed: the object is not the digest's. It counts against R2 too:
// a run of them means R2 serves something else than the core wrote.
func (s *server) verifyFailed(f *os.File) (*os.File, int64, result) {
	if f != nil {
		f.Close()
	}
	s.r2.fail()
	return nil, 0, resVerifyFail
}

// promoteOnce asks the core to publish d (bad: its object failed
// verification here). true: the core says it is in R2 now.
func (s *server) promoteOnce(ctx context.Context, d digest, bad bool) bool {
	if s.cfg.promote == "" || !s.prom.allow() {
		return false
	}
	wait := min(s.cfg.promoteWaitMax, 2*time.Second+time.Duration(float64(d.size)/40e3)*time.Millisecond)
	if bad {
		wait = 0
	}
	body, _ := json.Marshal(map[string]any{"hash": d.hash, "size": d.size, "wait_ms": wait.Milliseconds(), "seen_404": true, "bad": bad})
	ctx, cancel := context.WithTimeout(ctx, wait+10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.cfg.promote, bytes.NewReader(body))
	if err != nil {
		return false
	}
	req.Header.Set("Content-Type", "application/json")
	t0 := time.Now()
	resp, err := s.pc.Do(req)
	if err != nil {
		s.prom.fail()
		return false
	}
	defer resp.Body.Close()
	var ans struct {
		Key   string `json:"key"`
		State string `json:"state"`
	}
	jsonBody := strings.HasPrefix(resp.Header.Get("Content-Type"), "application/json")
	if jsonBody {
		_ = json.NewDecoder(io.LimitReader(resp.Body, 4096)).Decode(&ans)
	}
	switch {
	case resp.StatusCode == http.StatusOK && jsonBody && strings.HasSuffix(ans.Key, "/"+objectName(d.hash, d.size)):
		s.prom.ok()
		if !bad {
			s.st.promoted.Add(1)
			s.mu.Lock()
			if len(s.waits) < 100000 {
				s.waits = append(s.waits, time.Since(t0).Seconds())
			}
			s.mu.Unlock()
		}
		return true
	case jsonBody && (resp.StatusCode == http.StatusAccepted || resp.StatusCode == http.StatusNotFound ||
		resp.StatusCode == http.StatusRequestEntityTooLarge || resp.StatusCode == http.StatusTooManyRequests ||
		resp.StatusCode == http.StatusServiceUnavailable):
		// An answer: pending, not in the core's tier, out of range, over
		// budget, switched off. Today's path for this read.
		s.prom.ok()
		return false
	default:
		// Anything else (a 5xx, Caddy's gRPC refusal when the route is not
		// deployed, a body that is not the promoter's): an error.
		s.prom.fail()
		return false
	}
}

// proxy relays one call to the upstream, message for message. first: the
// request message already read (Read). count: where to add the response bytes.
func (s *server) proxy(ss grpc.ServerStream, method string, first *frame, count *atomic.Int64) error {
	ctx, cancel := context.WithCancel(ss.Context())
	defer cancel()
	up := s.up[s.next.Add(1)%uint64(len(s.up))]
	cs, err := up.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true, ClientStreams: true}, method)
	if err != nil {
		return err
	}
	go func() {
		if first != nil {
			if err := cs.SendMsg(first); err != nil {
				return // the upstream ended the call: RecvMsg below has its status
			}
		}
		for {
			f := &frame{}
			if err := ss.RecvMsg(f); err != nil {
				if err == io.EOF {
					cs.CloseSend()
				} else {
					cancel()
				}
				return
			}
			if err := cs.SendMsg(f); err != nil {
				return
			}
		}
	}()
	for {
		f := &frame{}
		err := cs.RecvMsg(f)
		if err == io.EOF {
			ss.SetTrailer(cs.Trailer())
			return nil
		}
		if err != nil {
			ss.SetTrailer(cs.Trailer())
			return err
		}
		if count != nil {
			count.Add(int64(len(f.b)))
		}
		if err := ss.SendMsg(f); err != nil {
			return err
		}
	}
}

func (s *server) report() string {
	// Background bad reports get 2 s more at most: the line goes out first.
	done := make(chan struct{})
	go func() { s.reportWG.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
	}
	s.mu.Lock()
	w := append([]float64(nil), s.waits...)
	s.mu.Unlock()
	p50 := "-"
	if len(w) > 0 {
		sort.Float64s(w)
		p50 = fmt.Sprintf("%.1f", w[len(w)/2])
	}
	st := &s.st
	return fmt.Sprintf("rbe-hot: reads=%d big=%d r2_hits=%d r2_bytes=%d raw_bytes=%d promoted=%d promote_wait_s_p50=%s"+
		" r2_404=%d r2_errors=%d fallbacks=%d verify_fail=%d bypassed=%d core_bytes=%d repeat_digests=%d repeat_bytes=%d"+
		" r2_breaker_opened=%d promote_breaker_opened=%d not_found_after_promote=%d busy=%d local_errors=%d",
		st.reads.Load(), st.big.Load(), st.r2Hits.Load(), st.r2Bytes.Load(), st.rawBytes.Load(), st.promoted.Load(), p50,
		st.r2NotFound.Load(), st.r2Errors.Load(), st.fallbacks.Load(), st.verifyFail.Load(), st.bypassed.Load(),
		st.coreBytes.Load(), st.repeatDigests.Load(), st.repeatBytes.Load(), s.r2.openedCount(), s.prom.openedCount(),
		st.notFoundAfterPromote.Load(), st.busy.Load(), st.localErrors.Load())
}

func (b *breaker) openedCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.opened
}
