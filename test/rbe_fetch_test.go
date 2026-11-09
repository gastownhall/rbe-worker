package scripts_test

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
)

const rbeFetch = "worker/rbe-fetch"

// fetchServer answers each path with the next status of its script (the last
// one repeats); 200 serves body. It counts the requests per path.
type fetchServer struct {
	*httptest.Server
	mu   sync.Mutex
	hits map[string]int
}

func newFetchServer(t *testing.T, body []byte, statuses map[string][]int) *fetchServer {
	t.Helper()
	fs := &fetchServer{hits: map[string]int{}}
	fs.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fs.mu.Lock()
		n := fs.hits[r.URL.Path]
		fs.hits[r.URL.Path]++
		fs.mu.Unlock()
		script, ok := statuses[r.URL.Path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		code := script[min(n, len(script)-1)]
		if code != http.StatusOK {
			w.WriteHeader(code)
			return
		}
		w.Write(body)
	}))
	t.Cleanup(fs.Close)
	return fs
}

func (fs *fetchServer) count(path string) int {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.hits[path]
}

func sha256Hex(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

// runFetch runs rbe-fetch for artifact "nl" into a fresh directory with
// one-second retry waits; mirror is RBE_DIST_MIRROR ("" unsets it).
func runFetch(t *testing.T, mirror, sum, mirrorPath, url string, extraEnv ...string) (out, file string, err error) {
	t.Helper()
	file = filepath.Join(t.TempDir(), "nl.tgz")
	cmd := exec.Command("bash", filepath.Join(repoRoot(t), rbeFetch), "nl", file, sum, "20", mirrorPath, url)
	cmd.Env = append(os.Environ(), "RBE_FETCH_RETRY_DELAY=1", "RBE_DIST_MIRROR="+mirror)
	cmd.Env = append(cmd.Env, extraEnv...)
	b, err := cmd.CombinedOutput()
	return string(b), file, err
}

func wantFile(t *testing.T, file string, body []byte) {
	t.Helper()
	got, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("no output file: %v", err)
	}
	if string(got) != string(body) {
		t.Fatalf("output file holds %q, want %q", got, body)
	}
	if _, err := os.Stat(file + ".part"); err == nil {
		t.Errorf("%s.part left behind", file)
	}
}

func wantLine(t *testing.T, out, re string) {
	t.Helper()
	if !regexp.MustCompile(`(?m)^rbe-fetch: name=nl ` + re).MatchString(out) {
		t.Fatalf("no line matching %q in:\n%s", re, out)
	}
}

// A 504 from upstream (the 2026-11-08 GitHub release outage) is retried.
func TestRBEFetchRetriesUpstream5xx(t *testing.T) {
	body := []byte("nativelink tarball")
	srv := newFetchServer(t, body, map[string][]int{"/up/nl.tgz": {504, 502, 200}})
	out, file, err := runFetch(t, "", sha256Hex(body), "-", srv.URL+"/up/nl.tgz")
	if err != nil {
		t.Fatalf("rbe-fetch: %v\n%s", err, out)
	}
	wantFile(t, file, body)
	wantLine(t, out, `status=ok source=upstream attempts=3 bytes=18 seconds=[0-9]+ mirror=off$`)
	if n := srv.count("/up/nl.tgz"); n != 3 {
		t.Errorf("upstream requests = %d, want 3", n)
	}
}

// The mirror comes first, and a good mirror byte stream never reaches upstream.
func TestRBEFetchMirrorFirst(t *testing.T) {
	body := []byte("nativelink tarball")
	srv := newFetchServer(t, body, map[string][]int{"/dist/nativelink/1.7.1/nl.tgz": {200}, "/up/nl.tgz": {200}})
	out, file, err := runFetch(t, srv.URL+"/dist", sha256Hex(body), "nativelink/1.7.1/nl.tgz", srv.URL+"/up/nl.tgz")
	if err != nil {
		t.Fatalf("rbe-fetch: %v\n%s", err, out)
	}
	wantFile(t, file, body)
	wantLine(t, out, `status=ok source=mirror attempts=1 bytes=18 seconds=[0-9]+ mirror=hit$`)
	if n := srv.count("/up/nl.tgz"); n != 0 {
		t.Errorf("upstream requested %d times after a mirror hit", n)
	}
	// A transient mirror error is retried on the mirror.
	srv = newFetchServer(t, body, map[string][]int{"/dist/nativelink/1.7.1/nl.tgz": {503, 200}, "/up/nl.tgz": {200}})
	out, _, err = runFetch(t, srv.URL+"/dist/", sha256Hex(body), "nativelink/1.7.1/nl.tgz", srv.URL+"/up/nl.tgz")
	if err != nil {
		t.Fatalf("rbe-fetch: %v\n%s", err, out)
	}
	wantLine(t, out, `status=ok source=mirror attempts=2 .* mirror=hit$`)
}

// A mirror miss (404, not retried) or a mirror outage falls through to upstream.
func TestRBEFetchMirrorMissFallsBackToUpstream(t *testing.T) {
	body := []byte("nativelink tarball")
	for name, mirror := range map[string][]int{"404": {404}, "5xx": {500}} {
		t.Run(name, func(t *testing.T) {
			srv := newFetchServer(t, body, map[string][]int{"/dist/nl/1/nl.tgz": mirror, "/up/nl.tgz": {200}})
			out, file, err := runFetch(t, srv.URL+"/dist", sha256Hex(body), "nl/1/nl.tgz", srv.URL+"/up/nl.tgz")
			if err != nil {
				t.Fatalf("rbe-fetch: %v\n%s", err, out)
			}
			wantFile(t, file, body)
			wantLine(t, out, `status=ok source=upstream attempts=1 .* mirror=miss$`)
			want := 3 // transient: --retry 2
			if name == "404" {
				want = 1
			}
			if n := srv.count("/dist/nl/1/nl.tgz"); n != want {
				t.Errorf("mirror requests = %d, want %d", n, want)
			}
		})
	}
}

// The mirror is untrusted transport: bytes that fail the pinned sha256 are
// thrown away, with a warning, and upstream's are used.
func TestRBEFetchMirrorChecksumMismatchFallsBackToUpstream(t *testing.T) {
	good, bad := []byte("nativelink tarball"), []byte("tampered tarball!!")
	mirror := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write(bad) }))
	t.Cleanup(mirror.Close)
	up := newFetchServer(t, good, map[string][]int{"/up/nl.tgz": {200}})
	out, file, err := runFetch(t, mirror.URL, sha256Hex(good), "nl/1/nl.tgz", up.URL+"/up/nl.tgz")
	if err != nil {
		t.Fatalf("rbe-fetch: %v\n%s", err, out)
	}
	wantFile(t, file, good)
	wantLine(t, out, `status=ok source=upstream attempts=1 .* mirror=bad-sha256$`)
	if !strings.Contains(out, "::warning title=rbe fetch::nl from RBE_DIST_MIRROR failed its pinned sha256") ||
		!strings.Contains(out, "sha256="+sha256Hex(bad)+" want="+sha256Hex(good)) {
		t.Errorf("no mismatch warning naming both sums:\n%s", out)
	}
}

// Total failure exits non-zero, so the worker's setup fails and the pool
// breaker sees the outage; nothing is left at OUT. An upstream byte that
// fails the checksum is a failure too.
func TestRBEFetchTotalFailureExitsNonZero(t *testing.T) {
	body := []byte("nativelink tarball")
	t.Run("outage", func(t *testing.T) {
		srv := newFetchServer(t, body, map[string][]int{"/dist/nl/1/nl.tgz": {503}, "/up/nl.tgz": {504}})
		out, file, err := runFetch(t, srv.URL+"/dist", sha256Hex(body), "nl/1/nl.tgz", srv.URL+"/up/nl.tgz")
		if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() != 1 {
			t.Fatalf("rbe-fetch: %v, want exit 1\n%s", err, out)
		}
		wantLine(t, out, `status=failed source=- attempts=6 bytes=0 seconds=[0-9]+ mirror=miss$`)
		if n := srv.count("/up/nl.tgz"); n != 6 {
			t.Errorf("upstream requests = %d, want 6 (--retry 5)", n)
		}
		for _, f := range []string{file, file + ".part", file + ".err"} {
			if _, err := os.Stat(f); err == nil {
				t.Errorf("%s left behind after a failure", f)
			}
		}
	})
	t.Run("upstream checksum", func(t *testing.T) {
		srv := newFetchServer(t, []byte("not it"), map[string][]int{"/up/nl.tgz": {200}})
		out, file, err := runFetch(t, "", sha256Hex(body), "-", srv.URL+"/up/nl.tgz")
		if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() != 1 {
			t.Fatalf("rbe-fetch: %v, want exit 1\n%s", err, out)
		}
		wantLine(t, out, `status=failed `)
		if _, err := os.Stat(file); err == nil {
			t.Error("a file that failed its checksum was left at OUT")
		}
	})
}

// An unpinned lookup (go.dev's checksum JSON, SHA256 -) never asks the mirror:
// there is nothing to check its bytes against. A mirror that is not an http(s)
// URL is ignored with a warning.
func TestRBEFetchUnpinnedSkipsMirror(t *testing.T) {
	body := []byte(`[]`)
	srv := newFetchServer(t, body, map[string][]int{"/dl/": {200}, "/dist/go/x": {200}})
	out, file, err := runFetch(t, srv.URL+"/dist", "-", "go/x", srv.URL+"/dl/?mode=json&include=all")
	if err != nil {
		t.Fatalf("rbe-fetch: %v\n%s", err, out)
	}
	wantFile(t, file, body)
	wantLine(t, out, `status=ok source=upstream attempts=1 .* mirror=off$`)
	if n := srv.count("/dist/go/x"); n != 0 {
		t.Errorf("mirror asked %d times for an unpinned file", n)
	}
	out, _, err = runFetch(t, "file:///etc", sha256Hex(body), "go/x", srv.URL+"/dl/")
	if err != nil || !strings.Contains(out, "::warning title=rbe fetch::RBE_DIST_MIRROR unusable") {
		t.Errorf("a file:// mirror: %v\n%s", err, out)
	}
	for _, args := range [][]string{
		{"nl", "o", "abc", "20", "-", srv.URL},            // not a sha256
		{"nl", "o", "-", "20", "../etc/passwd", srv.URL},  // traversal
		{"nl", "o", "-", "0", "-", srv.URL},               // max time
		{"nl", "o", "-", "20", "-", "file:///etc/passwd"}, // upstream scheme
		{"NL", "o", "-", "20", "-", srv.URL},              // name
		{"nl", "o", "-", "20", "-"},                       // arity
	} {
		cmd := exec.Command("bash", append([]string{filepath.Join(repoRoot(t), rbeFetch)}, args...)...)
		cmd.Dir = t.TempDir()
		if b, err := cmd.CombinedOutput(); err == nil || err.(*exec.ExitError).ExitCode() != 2 {
			t.Errorf("rbe-fetch %v: %v, want exit 2\n%s", args, err, b)
		}
	}
}

// curl before 7.71 has no --retry-all-errors: rbe-fetch leaves it out (curl
// --retry still retries 5xx) rather than failing on an unknown option.
func TestRBEFetchOldCurlWithoutRetryAllErrors(t *testing.T) {
	realCurl, err := exec.LookPath("curl")
	if err != nil {
		t.Skip("no curl")
	}
	for _, tc := range []struct{ version, want string }{{"7.68.0", "absent"}, {"8.5.0", "present"}} {
		bin := t.TempDir()
		log := filepath.Join(bin, "args")
		wrapper := "#!/bin/sh\nif [ \"$1\" = --version ]; then echo 'curl " + tc.version + " (x86_64-pc-linux-gnu)'; exit 0; fi\n" +
			"echo \"$*\" >>" + log + "\nexec " + realCurl + " \"$@\"\n"
		if err := os.WriteFile(filepath.Join(bin, "curl"), []byte(wrapper), 0o755); err != nil {
			t.Fatal(err)
		}
		body := []byte("x")
		srv := newFetchServer(t, body, map[string][]int{"/up": {503, 200}})
		out, _, err := runFetch(t, "", sha256Hex(body), "-", srv.URL+"/up", "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"))
		if err != nil {
			t.Fatalf("curl %s: %v\n%s", tc.version, err, out)
		}
		wantLine(t, out, `status=ok source=upstream attempts=2 `)
		args, _ := os.ReadFile(log)
		if got := map[bool]string{true: "present", false: "absent"}[strings.Contains(string(args), "--retry-all-errors")]; got != tc.want {
			t.Errorf("curl %s: --retry-all-errors %s, want %s (%s)", tc.version, got, tc.want, args)
		}
		if !strings.Contains(string(args), "--retry 5 --retry-delay 1") || !strings.Contains(string(args), "--connect-timeout 10 --max-time 20") {
			t.Errorf("curl %s args: %s", tc.version, args)
		}
	}
}

// Every setup download of blacksmith-worker.sh goes through rbe-fetch with
// its pin: no bare curl of a release URL is left, and each artifact has its
// mirror key under <name>/<version>/.
func TestRBEWorkerSetupDownloadsUseRBEFetch(t *testing.T) {
	root := repoRoot(t)
	script := readFile(t, root, rbeWorkerScript)
	for _, line := range strings.Split(script, "\n") {
		if regexp.MustCompile(`^\s*curl\b.*(github\.com|go\.dev)`).MatchString(line) {
			t.Errorf("a setup download bypasses rbe-fetch: %s", line)
		}
	}
	for _, want := range []string{
		`"$HERE/rbe-fetch" go-checksums "$RUNNER_TEMP/go-dl.json" - 60 - "https://go.dev/dl/?mode=json&include=all"`,
		`"$HERE/rbe-fetch" go "$RUNNER_TEMP/go.tgz" "$go_sum" 300 "go/${GO_VERSION}/go${GO_VERSION}.linux-amd64.tar.gz"`,
		`"$HERE/rbe-fetch" dolt "$RUNNER_TEMP/dolt.tgz" "$DOLT_SHA256" 180 "dolt/${DOLT_VERSION}/dolt-linux-amd64.tar.gz"`,
		`"$HERE/rbe-fetch" nativelink "$RUNNER_TEMP/nl.tgz" "$NL_SHA256" 180`,
		`"nativelink/${NL_VERSION}/nativelink-${NL_VERSION}-x86_64-unknown-linux-musl.tar.gz"`,
		// Only https:// reaches rbe-fetch from the workflow.
		`[[ ! $RBE_DIST_MIRROR =~ ^https://[A-Za-z0-9.-]+(/[A-Za-z0-9._/-]*)?$ ]]`,
	} {
		if !strings.Contains(script, want) {
			t.Errorf("%s: missing %s", rbeWorkerScript, want)
		}
	}
	// The pinned Go checksum is the product's go.mod version's (gascity pins
	// it; a version without a pin falls back to go.dev's JSON).
	if !regexp.MustCompile(`(?m)^[0-9]+\.[0-9]+\.[0-9]+\) GO_SHA256=[0-9a-f]{64} ;;$`).MatchString(script) {
		t.Errorf("%s: no pinned GO_SHA256", rbeWorkerScript)
	}
	ci := readFile(t, root, ".github/workflows/ci.yml")
	if !strings.Contains(ci, "worker/rbe-fetch") {
		t.Error("ci.yml's shellcheck does not cover worker/rbe-fetch")
	}
	if info, err := os.Stat(filepath.Join(root, rbeFetch)); err != nil || info.Mode().Perm()&0o111 == 0 {
		t.Errorf("%s must be executable: %v", rbeFetch, err)
	}
}
