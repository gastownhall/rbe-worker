package scripts_test

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
	start := time.Now()
	cmd := exec.Command("python3", filepath.Join(repoRoot(t), warmCAS), url, store, strconv.FormatInt(deadline.Unix(), 10))
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
	man := map[string]any{"v": 1, "set": set, "chunks": []map[string]any{{"name": set + "/00.tar.zst", "raw_bytes": 1, "zbytes": len(chunk)}}}
	srv, _ := warmServer(t, man, map[string][]byte{set + "/00.tar.zst": chunk}, 0)
	store := t.TempDir()
	out, _ := runWarmCAS(t, srv.URL+"/w", store, time.Now().Add(30*time.Second))
	if !regexp.MustCompile(`(?m)^rbe-warm: set=` + set + ` status=partial blobs=2 bytes=160000 rejected=1 skipped=4 chunks=1/1 `).MatchString(out) {
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
	blob := bytes.Repeat([]byte("c"), 1<<20)
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
	blob := bytes.Repeat([]byte("d"), 1<<20)
	chunk := zstdChunk(t, []warmMember{{name: blobName(blob), data: blob, typeflag: tar.TypeReg}})
	man := map[string]any{"v": 1, "set": set, "chunks": []map[string]any{{"name": set + "/00.tar.zst", "raw_bytes": len(blob), "zbytes": len(chunk)}}}
	srv, _ := warmServer(t, man, map[string][]byte{set + "/00.tar.zst": chunk}, 2*time.Second)
	store := t.TempDir()
	cmd := exec.Command("python3", filepath.Join(repoRoot(t), warmCAS), srv.URL+"/w", store, strconv.FormatInt(time.Now().Add(60*time.Second).Unix(), 10))
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
		`[ "$(ps -o sid= -p "$warm_pid" | tr -d ' ')" = "$warm_pid" ]`,
		`env -i PATH="$PATH" HOME="$HOME" RUST_LOG="$NL_RUST_LOG" "$NL_BIN_DIR/nativelink"`,
	} {
		if !strings.Contains(s, want) {
			t.Errorf("%s: missing %q", rbeWorkerScript, want)
		}
	}
	wait, start := strings.Index(s, "pkill -KILL -s"), strings.Index(s, `"$NL_BIN_DIR/nativelink" "$ROOT/worker.json"`)
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
