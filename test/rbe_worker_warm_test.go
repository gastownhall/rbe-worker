package scripts_test

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

const warmCAS = "worker/warm-cas"

// warmMember is one tar member of a warm-set chunk.
type warmMember struct {
	name     string
	data     []byte
	typeflag byte
	link     string
}

// incompressible returns n pseudo-random bytes: a chunk that zstd cannot shrink.
func incompressible(n int) []byte {
	b := make([]byte, n)
	rand.New(rand.NewSource(int64(n))).Read(b)
	return b
}

func blobName(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]) + "-" + strconv.Itoa(len(data))
}

func zstdChunk(t *testing.T, members []warmMember) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, m := range members {
		h := &tar.Header{Name: m.name, Mode: 0o444, Size: int64(len(m.data)), Typeflag: m.typeflag, Linkname: m.link}
		if m.typeflag != tar.TypeReg {
			h.Size = 0
		}
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if m.typeflag == tar.TypeReg {
			if _, err := tw.Write(m.data); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("zstd", "-q", "-c")
	cmd.Stdin = &buf
	out, err := cmd.Output()
	if err != nil {
		t.Skipf("zstd: %v (the runner image has it)", err)
	}
	return out
}

// warmServer serves current.json and the chunks of one set, slowly if delay > 0,
// and records every path requested.
func warmServer(t *testing.T, manifest any, chunks map[string][]byte, delay time.Duration) (*httptest.Server, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var seen []string
	man, _ := json.Marshal(manifest)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.URL.Path)
		mu.Unlock()
		if r.URL.Path == "/w/current.json" {
			w.Write(man)
			return
		}
		body, ok := chunks[strings.TrimPrefix(r.URL.Path, "/w/chunks/")]
		if !ok {
			http.NotFound(w, r)
			return
		}
		for len(body) > 0 {
			n := min(len(body), 4096)
			if _, err := w.Write(body[:n]); err != nil {
				return
			}
			body = body[n:]
			if delay > 0 {
				w.(http.Flusher).Flush()
				time.Sleep(delay)
			}
		}
	}))
	t.Cleanup(srv.Close)
	return srv, func() []string { mu.Lock(); defer mu.Unlock(); return append([]string(nil), seen...) }
}

func runWarmCAS(t *testing.T, url, store string, deadline time.Time) (string, time.Duration) {
	t.Helper()
	return runWarmCASWith(t, nil, url, store, deadline)
}

// warmEnv is warm-cas's environment in these tests: no free-space margin, so
// the result does not depend on the runner's disk.
func warmEnv() []string { return append(os.Environ(), "RBE_WARM_FREE_MARGIN=0") }

// runWarmCASWith runs warm-cas under prefix (e.g. prlimit) when it is given.
func runWarmCASWith(t *testing.T, prefix []string, url, store string, deadline time.Time) (string, time.Duration) {
	t.Helper()
	start := time.Now()
	args := append(append([]string(nil), prefix...), "python3", filepath.Join(repoRoot(t), warmCAS), url, store, strconv.FormatInt(deadline.Unix(), 10))
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Env = warmEnv()
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("warm-cas must exit 0 when it cannot warm: %v\n%s", err, out)
	}
	return string(out), time.Since(start)
}

func TestWarmCASPlacesOnlyVerifiedBlobs(t *testing.T) {
	good1, good2, other := bytes.Repeat([]byte("a"), 70000), bytes.Repeat([]byte("b"), 90000), []byte("not what the name says")
	set := "0123456789abcdef0123456789abcdef"
	chunk := zstdChunk(t, []warmMember{
		{name: blobName(good1), data: good1, typeflag: tar.TypeReg},
		{name: blobName(bytes.Repeat([]byte("z"), len(other))), data: other, typeflag: tar.TypeReg}, // content != name
		{name: blobName(good2), typeflag: tar.TypeSymlink, link: "/etc/passwd"},
		{name: "../../escape-1", data: good2, typeflag: tar.TypeReg},
		{name: blobName(good2), typeflag: tar.TypeLink, link: "x"},
		{name: "d2", typeflag: tar.TypeDir},
		{name: blobName(good2), data: good2, typeflag: tar.TypeReg},
	})
	man := map[string]any{"v": 1, "set": set, "chunks": []map[string]any{{"name": set + "/00.tar.zst", "raw_bytes": 160000, "zbytes": len(chunk)}}}
	srv, _ := warmServer(t, man, map[string][]byte{set + "/00.tar.zst": chunk}, 0)
	store := t.TempDir()
	out, _ := runWarmCAS(t, srv.URL+"/w", store, time.Now().Add(30*time.Second))
	if !regexp.MustCompile(`(?m)^rbe-warm: set=` + set + ` status=partial blobs=2 bytes=160000 rejected=1 skipped=4 chunks=1/1 missing=0 stopped=0 `).MatchString(out) {
		t.Fatalf("warm-cas output:\n%s", out)
	}
	entries, _ := os.ReadDir(filepath.Join(store, "content", "d2"))
	var names []string
	for _, e := range entries {
		info, _ := e.Info()
		if !info.Mode().IsRegular() || info.Mode().Perm() != 0o444 {
			t.Errorf("%s: mode %v, want a regular 0444 file (the filesystem store's own mode)", e.Name(), info.Mode())
		}
		names = append(names, e.Name())
	}
	want := []string{blobName(good1) + "-0", blobName(good2) + "-0"}
	if strings.Join(names, ",") != strings.Join(want, ",") && strings.Join(names, ",") != want[1]+","+want[0] {
		t.Errorf("content/d2 = %v, want %v (generation 0: NativeLink writes 1 and up)", names, want)
	}
	if left, _ := os.ReadDir(filepath.Join(store, "tmp", "d2")); len(left) != 0 {
		t.Errorf("staging left behind: %v", left)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(store), "escape-1")); err == nil {
		t.Error("a '..' member was extracted outside the store")
	}
	// Idempotent: a second run places nothing new (the two placed blobs and
	// the four members that are not blobs are skipped).
	out, _ = runWarmCAS(t, srv.URL+"/w", store, time.Now().Add(30*time.Second))
	if !strings.Contains(out, " blobs=2 ") || !strings.Contains(out, " skipped=6 ") {
		t.Errorf("second run:\n%s", out)
	}
}

func TestWarmCASRefusesABadManifest(t *testing.T) {
	set := "0123456789abcdef0123456789abcdef"
	for name, chunkName := range map[string]string{"traversal": "../../etc/passwd", "other set": "fedcba9876543210fedcba9876543210/00.tar.zst"} {
		t.Run(name, func(t *testing.T) {
			man := map[string]any{"v": 1, "set": set, "chunks": []map[string]any{{"name": chunkName, "raw_bytes": 1, "zbytes": 1}}}
			srv, seen := warmServer(t, man, nil, 0)
			out, _ := runWarmCAS(t, srv.URL+"/w", t.TempDir(), time.Now().Add(30*time.Second))
			if !strings.Contains(out, "rbe-warm: set=- status=error reason=ValueError") {
				t.Errorf("output:\n%s", out)
			}
			if got := seen(); len(got) != 1 {
				t.Errorf("fetched %v; a bad manifest must stop before any chunk", got)
			}
		})
	}
}

func TestWarmCASStopsAtItsDeadline(t *testing.T) {
	set := "0123456789abcdef0123456789abcdef"
	blob := incompressible(1 << 20) // so the slow server really is slow
	chunk := zstdChunk(t, []warmMember{{name: blobName(blob), data: blob, typeflag: tar.TypeReg}})
	man := map[string]any{"v": 1, "set": set, "chunks": []map[string]any{{"name": set + "/00.tar.zst", "raw_bytes": len(blob), "zbytes": len(chunk)}}}
	srv, _ := warmServer(t, man, map[string][]byte{set + "/00.tar.zst": chunk}, 2*time.Second)
	store := t.TempDir()
	out, took := runWarmCAS(t, srv.URL+"/w", store, time.Now().Add(2*time.Second))
	if !strings.Contains(out, "status=timeout") || took > 6*time.Second {
		t.Errorf("after %v:\n%s", took, out)
	}
	if left, _ := os.ReadDir(filepath.Join(store, "tmp", "d2")); len(left) != 0 {
		t.Errorf("staging left behind: %v", left)
	}
}

// blacksmith-worker.sh sends SIGTERM once isolation is ready (plus
// RBE_WARM_GRACE): warm-cas reports what it placed and exits at once.
func TestWarmCASStopsAtSIGTERM(t *testing.T) {
	set := "0123456789abcdef0123456789abcdef"
	blob := incompressible(1 << 20) // so the slow server really is slow
	chunk := zstdChunk(t, []warmMember{{name: blobName(blob), data: blob, typeflag: tar.TypeReg}})
	man := map[string]any{"v": 1, "set": set, "chunks": []map[string]any{{"name": set + "/00.tar.zst", "raw_bytes": len(blob), "zbytes": len(chunk)}}}
	srv, _ := warmServer(t, man, map[string][]byte{set + "/00.tar.zst": chunk}, 2*time.Second)
	store := t.TempDir()
	cmd := exec.Command("python3", filepath.Join(repoRoot(t), warmCAS), srv.URL+"/w", store, strconv.FormatInt(time.Now().Add(60*time.Second).Unix(), 10))
	cmd.Env = warmEnv()
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	start := time.Now()
	cmd.Process.Signal(syscall.SIGTERM)
	if err := cmd.Wait(); err != nil {
		t.Fatalf("exit after SIGTERM: %v\n%s", err, out.String())
	}
	if took := time.Since(start); took > 3*time.Second || !strings.Contains(out.String(), "status=timeout") {
		t.Errorf("SIGTERM to exit %v:\n%s", took, out.String())
	}
	if left, _ := os.ReadDir(filepath.Join(store, "tmp", "d2")); len(left) != 0 {
		t.Errorf("staging left behind: %v", left)
	}
}

func TestWarmCASUnreachableCostsOnlyTheWarmStart(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()
	out, _ := runWarmCAS(t, url+"/w", t.TempDir(), time.Now().Add(10*time.Second))
	if !strings.Contains(out, "rbe-warm: set=- status=error reason=CalledProcessError") {
		t.Errorf("output:\n%s", out)
	}
}

// The worker script wires warm-cas the one safe way: pool mode only, before
// NativeLink starts, bounded, its own session killed whole, and no line that
// the scaler's isolation rollback could mistake for an isolation phase.
func TestBlacksmithWorkerWarmWiring(t *testing.T) {
	s := readFile(t, repoRoot(t), rbeWorkerScript)
	for _, want := range []string{
		`if [ -n "${RBE_WARM_URL:-}" ] && [ "$WORKER_MODE" = pool ]; then`,
		`"$HERE/warm-cas" "$RBE_WARM_URL" "$STORE" "$warm_end" >"$RUNNER_TEMP/rbe-warm.out" 2>&1 &`,
		`kill -TERM "$warm_pid" 2>/dev/null || true`,
		`warm_sid=$(ps -o sid= -p "$warm_pid" | tr -d ' ' || true)`,
		`if [ "$warm_sid" = "$warm_pid" ]; then`,
		`env -i PATH="$PATH" HOME="$HOME" RUST_LOG="$NL_RUST_LOG" "$NL_BIN_DIR/nativelink"`,
	} {
		if !strings.Contains(s, want) {
			t.Errorf("%s: missing %q", rbeWorkerScript, want)
		}
	}
	wait, start := strings.Index(s, `pkill -KILL -s "$warm_sid"`), strings.Index(s, `"$NL_BIN_DIR/nativelink" "$ROOT/worker.json"`)
	if wait < 0 || start < 0 || wait > start {
		t.Errorf("the warm wait must come before NativeLink starts")
	}
	for _, ln := range strings.Split(s, "\n") {
		if strings.Contains(ln, "rbe-warm") && regexp.MustCompile(`(^|\s)isolation: `).MatchString(ln) {
			t.Errorf("warm line looks like an isolation phase: %q", ln)
		}
	}
	w := readFile(t, repoRoot(t), warmCAS)
	if strings.Contains(w, "extractall") {
		t.Error("warm-cas must never extract by path")
	}
	// The warm domain's allow-list is IPv4 (infra tofu zone-waf.tf): a fetch
	// over IPv6 would be refused at the edge.
	if !strings.Contains(w, `"curl", "-fsS", "--ipv4",`) {
		t.Error("warm-cas must fetch over IPv4")
	}
}

// A write can be short (a full disk, a file size limit): warm-cas must never
// place a truncated file under a verified name. Under prlimit --fsize the
// second blob's write is cut at the limit; the chunk stops there (stopped=1,
// not an integrity reject), and only the blob that fitted is placed.
func TestWarmCASShortWriteNeverPlacesATruncatedBlob(t *testing.T) {
	prlimit, err := exec.LookPath("prlimit")
	if err != nil {
		t.Skip("prlimit (util-linux, on the runner image) is not installed")
	}
	set := "0123456789abcdef0123456789abcdef"
	small, big, after := bytes.Repeat([]byte("s"), 70000), bytes.Repeat([]byte("b"), 150000), bytes.Repeat([]byte("t"), 80000)
	chunk := zstdChunk(t, []warmMember{
		{name: blobName(small), data: small, typeflag: tar.TypeReg},
		{name: blobName(big), data: big, typeflag: tar.TypeReg},
		{name: blobName(after), data: after, typeflag: tar.TypeReg},
	})
	man := map[string]any{"v": 1, "set": set, "chunks": []map[string]any{{"name": set + "/00.tar.zst", "raw_bytes": 300000, "zbytes": len(chunk)}}}
	srv, _ := warmServer(t, man, map[string][]byte{set + "/00.tar.zst": chunk}, 0)
	store := t.TempDir()
	out, _ := runWarmCASWith(t, []string{prlimit, "--fsize=100000"}, srv.URL+"/w", store, time.Now().Add(30*time.Second))
	if !strings.Contains(out, " status=partial blobs=1 bytes=70000 rejected=0 ") || !strings.Contains(out, " stopped=1 ") {
		t.Errorf("warm-cas output:\n%s", out)
	}
	d2 := filepath.Join(store, "content", "d2")
	entries, _ := os.ReadDir(d2)
	for _, e := range entries {
		info, _ := e.Info()
		size, _ := strconv.ParseInt(strings.Split(e.Name(), "-")[1], 10, 64)
		if info.Size() != size {
			t.Errorf("%s holds %d bytes, its name says %d", e.Name(), info.Size(), size)
		}
	}
	if len(entries) != 1 || entries[0].Name() != blobName(small)+"-0" {
		t.Errorf("content/d2 = %v, want only %s-0", entries, blobName(small))
	}
	if left, _ := os.ReadDir(filepath.Join(store, "tmp", "d2")); len(left) != 0 {
		t.Errorf("staging left behind: %v", left)
	}
}

// A chunk may not place more bytes than the manifest declared for it.
func TestWarmCASKeepsToTheManifestsBytes(t *testing.T) {
	set := "0123456789abcdef0123456789abcdef"
	a, b := bytes.Repeat([]byte("a"), 70000), bytes.Repeat([]byte("b"), 90000)
	chunk := zstdChunk(t, []warmMember{{name: blobName(a), data: a, typeflag: tar.TypeReg}, {name: blobName(b), data: b, typeflag: tar.TypeReg}})
	man := map[string]any{"v": 1, "set": set, "chunks": []map[string]any{{"name": set + "/00.tar.zst", "raw_bytes": 70000, "zbytes": len(chunk)}}}
	srv, _ := warmServer(t, man, map[string][]byte{set + "/00.tar.zst": chunk}, 0)
	out, _ := runWarmCAS(t, srv.URL+"/w", t.TempDir(), time.Now().Add(30*time.Second))
	if !strings.Contains(out, " status=partial blobs=1 bytes=70000 rejected=1 ") {
		t.Errorf("warm-cas output:\n%s", out)
	}
}

// A chunk that cannot be fetched (an expired chunk's 404) is missing, not an
// integrity reject: W2's rejected=0 gate measures integrity alone.
func TestWarmCASCountsAMissingChunkApart(t *testing.T) {
	set := "0123456789abcdef0123456789abcdef"
	a := bytes.Repeat([]byte("a"), 70000)
	chunk := zstdChunk(t, []warmMember{{name: blobName(a), data: a, typeflag: tar.TypeReg}})
	man := map[string]any{"v": 1, "set": set, "chunks": []map[string]any{
		{"name": set + "/00.tar.zst", "raw_bytes": 70000, "zbytes": len(chunk)},
		{"name": set + "/01.tar.zst", "raw_bytes": 70000, "zbytes": 100},
	}}
	srv, _ := warmServer(t, man, map[string][]byte{set + "/00.tar.zst": chunk}, 0)
	out, _ := runWarmCAS(t, srv.URL+"/w", t.TempDir(), time.Now().Add(30*time.Second))
	if !strings.Contains(out, " status=partial blobs=1 bytes=70000 rejected=0 skipped=0 chunks=2/2 missing=1 stopped=0 ") {
		t.Errorf("warm-cas output:\n%s", out)
	}
}

// The script's warm wait, run as written: a warm-cas that ignores SIGTERM and
// keeps a forked child, or one that exits at SIGTERM but leaves its child
// behind. Either way registration waits at most RBE_WARM_GRACE plus the 10 s
// report window, and nothing of the warm session survives.
func TestBlacksmithWorkerWarmWaitIsBounded(t *testing.T) {
	block := regexp.MustCompile(`(?s)\nif \[ -n "\$\{warm_pid:-\}" \]; then\n\t# Isolation is ready.*?\nfi\n`).FindString(readFile(t, repoRoot(t), rbeWorkerScript))
	if block == "" {
		t.Fatalf("%s: no warm wait block", rbeWorkerScript)
	}
	for _, c := range []struct {
		name       string
		leaderTerm string // what the fake warm-cas does at SIGTERM
	}{
		{"ignores SIGTERM", "signal.SIG_IGN"},
		{"exits at SIGTERM, child stays", "lambda *_: os._exit(0)"},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			fake := filepath.Join(dir, "fake-warm-cas")
			// The child ignores SIGTERM too and records its pid.
			prog := "import os, signal, sys, time\nos.setsid()\n" +
				"if os.fork() == 0:\n    signal.signal(signal.SIGTERM, signal.SIG_IGN)\n" +
				"    open(sys.argv[1], 'w').write(str(os.getpid()))\n    time.sleep(600)\n    os._exit(0)\n" +
				"signal.signal(signal.SIGTERM, " + c.leaderTerm + ")\ntime.sleep(600)\n"
			if err := os.WriteFile(fake, []byte(prog), 0o755); err != nil {
				t.Fatal(err)
			}
			childFile := filepath.Join(dir, "child.pid")
			script := "set -euo pipefail\nRUNNER_TEMP=" + dir + "\nRBE_WARM_GRACE=1\n" +
				"python3 " + fake + " " + childFile + " >\"$RUNNER_TEMP/rbe-warm.out\" 2>&1 &\nwarm_pid=$!\n" +
				"warm_end=$(($(date +%s) + 75))\n" +
				"for _ in $(seq 50); do [ -s " + childFile + " ] && break; sleep 0.1; done\n" +
				"echo \"leader=$warm_pid\"\nSECONDS=0" + block + "echo \"waited=$SECONDS\"\n"
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			start := time.Now()
			out, err := exec.CommandContext(ctx, "bash", "-c", script).CombinedOutput()
			took := time.Since(start)
			if err != nil {
				t.Fatalf("wait block: %v\n%s", err, out)
			}
			// Grace 1 s plus 10 s, with one 0.5 s poll of slack each.
			if took > 13*time.Second {
				t.Errorf("registration waited %v, want at most RBE_WARM_GRACE + 10 s\n%s", took, out)
			}
			if !strings.Contains(string(out), "rbe-warm: status=error reason=no-report") {
				t.Errorf("no report line:\n%s", out)
			}
			leader := regexp.MustCompile(`leader=(\d+)`).FindStringSubmatch(string(out))
			child, _ := os.ReadFile(childFile)
			if leader == nil || len(child) == 0 {
				t.Fatalf("leader %v, child %q\n%s", leader, child, out)
			}
			for _, pid := range []string{leader[1], string(child)} {
				deadline := time.Now().Add(3 * time.Second)
				for alive(pid) && time.Now().Before(deadline) {
					time.Sleep(50 * time.Millisecond)
				}
				if alive(pid) {
					t.Errorf("pid %s of the warm session is still running", pid)
				}
			}
			if s, _ := exec.Command("pgrep", "-s", leader[1]).Output(); len(s) != 0 {
				t.Errorf("session %s still has members: %s", leader[1], s)
			}
		})
	}
}

// alive: the process exists and is not a zombie.
func alive(pid string) bool {
	b, err := os.ReadFile("/proc/" + strings.TrimSpace(pid) + "/stat")
	if err != nil {
		return false
	}
	f := strings.Fields(string(b[strings.LastIndexByte(string(b), ')')+1:]))
	return len(f) > 0 && f[0] != "Z" && f[0] != "X"
}
