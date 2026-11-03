// Command rbe-hotread fronts a pool worker's REMOTE_READ store on loopback
// (infra README "Hot blobs on R2"; design B2). NativeLink's REMOTE_READ
// GrpcStore talks to it instead of the zread host; it serves a big
// compressed ByteStream.Read from the farm's hot tier on R2 (free egress)
// and everything else, and every failure, from the zread host exactly as
// before:
//
//   - ByteStream.Read of compressed-blobs/zstd, offset 0, no limit, size in
//     [-min, -max], R2 not bypassed: GET -hot/<hash>-<size>.zst into an
//     unlinked temp file under -tmp, decoding it as it arrives; its
//     decompressed size and SHA-256 must equal the digest before NativeLink
//     sees one byte. Then the compressed bytes go to NativeLink, which
//     decodes and verifies them again. On a 404 it asks the core once
//     (POST -promote, the worker certificate) and GETs again on a 200.
//   - every other call (FindMissingBlobs, identity and small reads, offset
//     reads, a 404 the core did not promote, a 403, a timeout, a bad or
//     truncated object): proxied to -upstream (mTLS, the worker
//     certificate) message for message, so NativeLink sees today's answers.
//
// No new trust: an R2 object is only ever a candidate. A wrong one costs a
// fallback (and is never tried again by this VM), never a wrong blob.
// Breakers: -r2-fails consecutive R2 errors bypass R2 for -breaker-open;
// -promote-fails promote errors bypass promotion for as long.
//
// At SIGTERM it prints one line (the baseline's "rbe-hot:" input) and exits:
//
//	rbe-hot: reads=N big=N r2_hits=N r2_bytes=N raw_bytes=N promoted=N
//	  promote_wait_s_p50=X r2_404=N r2_errors=N fallbacks=N verify_fail=N
//	  bypassed=N core_bytes=N repeat_digests=N repeat_bytes=N
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type config struct {
	listen, upstream, cert, key, hot, promote, tmp string
	min, max                                       int64
	inflight, conns, r2Fails, promoteFails         int
	r2Timeout, promoteWaitMax, breakerOpen         time.Duration
	r2MBps                                         float64
}

// The hot tier's base URL: https, a host, and the tier's own prefix (h/oss
// or h/fork) alone. blacksmith-worker.sh checks the prefix against the tier.
var hotURL = regexp.MustCompile(`^https://[A-Za-z0-9.-]+/h/(oss|fork)$`)

func parseFlags(args []string) (config, error) {
	var c config
	fs := flag.NewFlagSet("rbe-hotread", flag.ContinueOnError)
	fs.StringVar(&c.listen, "listen", "127.0.0.1:50090", "loopback address NativeLink's REMOTE_READ dials (grpc://, h2c)")
	fs.StringVar(&c.upstream, "upstream", "", "zread host:port (grpcs, the worker certificate)")
	fs.StringVar(&c.cert, "cert", "", "worker certificate (PEM)")
	fs.StringVar(&c.key, "key", "", "worker key (PEM)")
	fs.StringVar(&c.hot, "hot", "", "hot tier base URL, https://HOST/h/<tier>")
	fs.StringVar(&c.promote, "promote", "", "promote URL, https://<zread host:port>/rbe.hot.v1.Hot/Promote (empty: never promote)")
	fs.StringVar(&c.tmp, "tmp", "", "directory for objects being verified (on the CAS's disk)")
	fs.Int64Var(&c.min, "min", 64<<20, "smallest blob (bytes) read from R2")
	fs.Int64Var(&c.max, "max", 4<<30, "largest blob (bytes) read from R2")
	fs.IntVar(&c.inflight, "inflight", 16, "R2 fetches at once (more wait)")
	fs.IntVar(&c.conns, "upstream-conns", 4, "HTTP/2 connections to the upstream")
	fs.IntVar(&c.r2Fails, "r2-fails", 5, "consecutive R2 errors (not 404s) that bypass R2 for -breaker-open")
	fs.IntVar(&c.promoteFails, "promote-fails", 3, "consecutive promote errors that bypass promotion for -breaker-open")
	fs.DurationVar(&c.r2Timeout, "r2-timeout", 10*time.Second, "an R2 GET's time limit, plus size / -r2-mbps")
	fs.Float64Var(&c.r2MBps, "r2-mbps", 50, "slowest R2 rate (MB/s) a GET may take")
	fs.DurationVar(&c.promoteWaitMax, "promote-wait-max", 30*time.Second, "longest wait for a promotion")
	fs.DurationVar(&c.breakerOpen, "breaker-open", 5*time.Minute, "how long a breaker bypasses R2 or promotion")
	if err := fs.Parse(args); err != nil {
		return c, err
	}
	host, _, err := net.SplitHostPort(c.listen)
	if err != nil {
		return c, fmt.Errorf("-listen %q: %v", c.listen, err)
	}
	if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
		return c, fmt.Errorf("-listen %q: loopback only (actions must not reach it)", c.listen)
	}
	if _, _, err := net.SplitHostPort(c.upstream); err != nil {
		return c, fmt.Errorf("-upstream %q: %v", c.upstream, err)
	}
	if !hotURL.MatchString(c.hot) {
		return c, fmt.Errorf("-hot %q: want https://HOST/h/oss or /h/fork", c.hot)
	}
	if c.promote != "" {
		u, err := url.Parse(c.promote)
		if err != nil || u.Scheme != "https" || u.Path != promotePath {
			return c, fmt.Errorf("-promote %q: want https://HOST:PORT%s", c.promote, promotePath)
		}
	}
	if c.tmp == "" || c.cert == "" || c.key == "" {
		return c, errors.New("-tmp, -cert and -key are required")
	}
	if c.min < 1 || c.max < c.min || c.inflight < 1 || c.conns < 1 || c.r2Fails < 1 || c.promoteFails < 1 || c.r2MBps <= 0 {
		return c, errors.New("-min, -max, -inflight, -upstream-conns, -r2-fails, -promote-fails or -r2-mbps out of range")
	}
	return c, nil
}

func main() {
	cfg, err := parseFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "rbe-hotread:", err)
		os.Exit(2)
	}
	cert, err := tls.LoadX509KeyPair(cfg.cert, cfg.key)
	if err != nil {
		fmt.Fprintln(os.Stderr, "rbe-hotread: worker certificate:", err)
		os.Exit(2)
	}
	upHost, _, _ := net.SplitHostPort(cfg.upstream)
	tlsCfg := &tls.Config{Certificates: []tls.Certificate{cert}, ServerName: upHost, MinVersion: tls.VersionTLS12}
	conns, err := dialUpstream(cfg.upstream, credentials.NewTLS(tlsCfg), cfg.conns)
	if err != nil {
		fmt.Fprintln(os.Stderr, "rbe-hotread: upstream:", err)
		os.Exit(2)
	}
	if err := os.MkdirAll(cfg.tmp, 0o700); err != nil {
		fmt.Fprintln(os.Stderr, "rbe-hotread:", err)
		os.Exit(2)
	}
	s := newServer(cfg, conns, r2Client(), promoteClient(tlsCfg))
	lis, err := net.Listen("tcp", cfg.listen)
	if err != nil {
		fmt.Fprintln(os.Stderr, "rbe-hotread:", err)
		os.Exit(2)
	}
	g := s.grpcServer()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sig
		done := make(chan struct{})
		go func() { g.GracefulStop(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			g.Stop()
		}
	}()
	fmt.Fprintf(os.Stderr, "rbe-hotread: listening on %s, upstream %s, hot %s, min %d\n", cfg.listen, cfg.upstream, cfg.hot, cfg.min)
	if err := g.Serve(lis); err != nil {
		fmt.Fprintln(os.Stderr, "rbe-hotread:", err)
	}
	fmt.Println(s.report())
}

// r2Client reads the hot tier anonymously over IPv4 (the WAF allow-list is
// Blacksmith's IPv4 ranges), with no proxy from the environment.
func r2Client() *http.Client {
	d := &net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}
	return &http.Client{
		Transport: &http.Transport{
			Proxy:               nil,
			DialContext:         func(ctx context.Context, _, addr string) (net.Conn, error) { return d.DialContext(ctx, "tcp4", addr) },
			TLSHandshakeTimeout: 5 * time.Second,
			ForceAttemptHTTP2:   true,
			MaxIdleConnsPerHost: 16,
			IdleConnTimeout:     90 * time.Second,
			DisableCompression:  true,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

// promoteClient POSTs to the zread host with the worker certificate. HTTP/2:
// the fork edge (:8444) speaks nothing else.
func promoteClient(tlsCfg *tls.Config) *http.Client {
	c := tlsCfg.Clone()
	c.ServerName = ""
	c.NextProtos = []string{"h2", "http/1.1"}
	return &http.Client{
		Transport: &http.Transport{
			Proxy:               nil,
			TLSClientConfig:     c,
			TLSHandshakeTimeout: 5 * time.Second,
			ForceAttemptHTTP2:   true,
			MaxIdleConnsPerHost: 2,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

func dialUpstream(target string, creds credentials.TransportCredentials, n int) ([]grpc.ClientConnInterface, error) {
	var out []grpc.ClientConnInterface
	for i := 0; i < n; i++ {
		cc, err := grpc.NewClient("dns:///"+target,
			grpc.WithTransportCredentials(creds),
			grpc.WithDefaultCallOptions(grpc.ForceCodec(rawCodec{}),
				grpc.MaxCallRecvMsgSize(maxMsg), grpc.MaxCallSendMsgSize(maxMsg)),
			grpc.WithInitialWindowSize(8<<20), grpc.WithInitialConnWindowSize(64<<20),
			grpc.WithUserAgent("rbe-hotread"))
		if err != nil {
			return nil, err
		}
		out = append(out, cc)
	}
	return out, nil
}

// objectName is a digest's object under the tier's prefix.
func objectName(hash string, size int64) string { return fmt.Sprintf("%s-%d.zst", hash, size) }
