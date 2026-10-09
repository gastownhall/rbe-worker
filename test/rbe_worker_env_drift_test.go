package scripts_test

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
)

// rbe-west's oss and oss-fork schedulers match worker-env exactly, so a pin
// that no live worker advertises is a queue that never drains while the pool
// scaler keeps dispatching Blacksmith workers that cannot take it. These tests
// pin the guards (worker/worker-env-drift) that make that loud and early:
//
//   - a worker whose host is not the pinned one still registers, advertising
//     what it measured: the pools are shared, and actions that send no
//     worker-env still run there, while those carrying the pin (gascity's
//     and beads') never match it;
//   - its run's measurement step fails with the diff and the manifest and pin
//     to commit in the step summary, and a job beside the worker opens or
//     updates the pin's drift issue while the worker serves (the farm caps
//     the pools while that issue is open);
//   - while that issue is open, remote CI on that pin fails at once instead
//     of queueing;
//   - a scheduled canary measures the Blacksmith image against the pin;
//   - a change that moves the pin, or touches the worker host's definition,
//     is measured on the Blacksmith image inside the required bazel job, and
//     a moved pin skips the remote suite (no worker serves it before merge).

const (
	rbeWorkerEnvDrift     = "worker/worker-env-drift"
	rbeWorkerEnvCanary    = ".github/workflows/rbe-worker-env-canary.yml"
	rbeWorkerEnvDriftDir  = "${{ runner.temp }}/worker-env-drift"
	rbeWorkerEnvDriftName = "worker-env-drift"
	rbeWorkerEnvLabel     = "rbe-worker-env-drift"
	rbeWorkerEnvReportGrp = "rbe-worker-env-drift-report"
	ghPinnedActionRE      = `^[a-z0-9-]+/[a-z0-9-]+(/[a-z0-9-]+)?@[0-9a-f]{40}$`
)

// ghStub is a gh for worker-env-drift: issue list answers with
// $GH_ISSUES (a JSON array of {title,url}) through the --jq it is given, api
// answers commits with $GH_COMMITS, contents?ref=SHA with $GH_FILES/SHA and
// a run attempt's jobs with $GH_JOBS through the --jq, issue view with
// $GH_VIEW. Every call is logged to $GH_LOG, one per line. GH_FAIL=1 fails
// every call. Like gh, it exits with its writer's status: a writer killed by
// SIGPIPE fails the call.
const ghStub = `#!/bin/sh
printf '%s\n' "$*" >>"$GH_LOG"
[ "${GH_FAIL:-}" != 1 ] || { echo "gh: HTTP 502" >&2; exit 1; }
jq=
prev=
for a; do
	[ "$prev" = --jq ] && jq=$a
	prev=$a
done
case "$1 $2" in
"issue list") jq -r "$jq" "$GH_ISSUES" ;;
"issue view") cat "$GH_VIEW" ;;
api*)
	for a; do
		case "$a" in
		*commits\?*) exec cat "$GH_COMMITS" ;;
		*contents/*ref=*) exec cat "$GH_FILES/${a##*ref=}" ;;
		*/jobs\?*) exec jq -r "$jq" "$GH_JOBS" ;;
		esac
	done
	;;
esac
`

type driftEnv struct {
	t       *testing.T
	root    string // the repo: the script under test
	dir     string // cwd: platforms/BUILD.bazel and tools/rbe/worker-env.txt
	pin     string
	ghLog   string
	issues  string
	summary string
	output  string
	extra   []string
}

func sha256Pin(b string) string {
	sum := sha256.Sum256([]byte(b))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func buildWithPin(pin string) string {
	return "platform(\n    name = \"rbe_worker\",\n    exec_properties = {\n        \"worker-env\": \"" + pin + "\",\n    },\n)\n"
}

func writeDriftFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

// newDriftEnv lays out a checkout whose pin is sha256(manifest), a stub gh
// and the GitHub Actions files.
func newDriftEnv(t *testing.T, manifest string) *driftEnv {
	t.Helper()
	e := &driftEnv{t: t, root: repoRoot(t), dir: t.TempDir(), pin: sha256Pin(manifest)}
	writeDriftFile(t, filepath.Join(e.dir, "tools/rbe/worker-env.txt"), manifest)
	writeDriftFile(t, filepath.Join(e.dir, "platforms/BUILD.bazel"), buildWithPin(e.pin))
	bin := filepath.Join(e.dir, "bin")
	writeDriftFile(t, filepath.Join(bin, "gh"), ghStub)
	if err := os.Chmod(filepath.Join(bin, "gh"), 0o755); err != nil {
		t.Fatal(err)
	}
	e.ghLog = filepath.Join(e.dir, "gh.log")
	e.issues = filepath.Join(e.dir, "issues.json")
	e.summary = filepath.Join(e.dir, "summary.md")
	e.output = filepath.Join(e.dir, "output")
	e.setIssues()
	return e
}

// setIssues sets the open drift issues: title, url pairs.
func (e *driftEnv) setIssues(titleURL ...string) {
	var items []string
	for i := 0; i+1 < len(titleURL); i += 2 {
		items = append(items, `{"title":"`+titleURL[i]+`","url":"`+titleURL[i+1]+`"}`)
	}
	writeDriftFile(e.t, e.issues, "["+strings.Join(items, ",")+"]\n")
}

func (e *driftEnv) run(args ...string) (string, error) {
	e.t.Helper()
	env := append([]string{
		"PATH=" + filepath.Join(e.dir, "bin") + string(os.PathListSeparator) + "/usr/bin" + string(os.PathListSeparator) + "/bin",
		"GITHUB_REPOSITORY=acme/repo", "GITHUB_RUN_ID=42", "RUNNER_NAME=runner-1", "RUNNER_TEMP=" + e.dir,
		"GITHUB_STEP_SUMMARY=" + e.summary, "GITHUB_OUTPUT=" + e.output,
		"GH_LOG=" + e.ghLog, "GH_ISSUES=" + e.issues,
	}, e.extra...)
	out, stderr, err := runRBEScript(e.dir, env, filepath.Join(e.root, rbeWorkerEnvDrift), args...)
	return out + stderr, err
}

func (e *driftEnv) read(path string) string {
	b, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		e.t.Fatal(err)
	}
	return string(b)
}

func driftTitle(pin string) string { return "rbe worker-env drift: " + pin }

func TestRBEWorkerEnvDriftCheck(t *testing.T) {
	pinned := "arch x86_64\npkg tmux 3.4\n"
	e := newDriftEnv(t, pinned)
	measured := filepath.Join(e.dir, "measured.txt")
	writeDriftFile(t, measured, pinned)
	out, err := e.run("check", measured)
	if err != nil || !strings.Contains(out, "this host is the pinned one ("+e.pin+")") {
		t.Fatalf("check of the pinned host: %v\n%s", err, out)
	}
	if _, err := os.Stat(filepath.Join(e.dir, "worker-env-drift")); !os.IsNotExist(err) {
		t.Errorf("check of the pinned host left a drift directory (%v)", err)
	}
	if s := e.read(e.summary); s != "" {
		t.Errorf("check of the pinned host wrote a step summary:\n%s", s)
	}

	drifted := "arch x86_64\npkg tmux 3.5\n"
	got := sha256Pin(drifted)
	writeDriftFile(t, measured, drifted)
	raw := filepath.Join(e.dir, "measured.raw.txt")
	writeDriftFile(t, raw, "arch x86_64\npkg tmux 3.5-1ubuntu0.1\n")
	out, err = e.run("check", measured, raw)
	if err == nil {
		t.Fatalf("check of a drifted host succeeded:\n%s", out)
	}
	if !strings.Contains(out, "::error title=rbe worker-env drift::this host measures worker-env="+got+", gascity requests "+e.pin) {
		t.Errorf("check output has no ::error naming both hashes:\n%s", out)
	}
	summary := e.read(e.summary)
	for _, want := range []string{
		"### rbe worker-env drift",
		"-pkg tmux 3.4\n+pkg tmux 3.5\n",
		"        \"worker-env\": \"" + got + "\",\n",
		"```\n" + drifted + "```",
		// The script's own hardcoded message text still names its pre-S2 path
		// (worker content is unmodified by the S2 move): match it as committed.
		"<summary>installed versions (tools/rbe/worker-env --raw; not hashed)</summary>\n\n```\narch x86_64\npkg tmux 3.5-1ubuntu0.1\n```",
		"runner-1 in https://github.com/acme/repo/actions/runs/42",
	} {
		if !strings.Contains(summary, want) {
			t.Errorf("step summary missing %q:\n%s", want, summary)
		}
	}
	dir := filepath.Join(e.dir, "worker-env-drift")
	for name, want := range map[string]string{
		"worker-env.txt":     drifted,
		"worker-env.raw.txt": "arch x86_64\npkg tmux 3.5-1ubuntu0.1\n",
		"pinned-pin":         e.pin + "\n",
		"measured-pin":       got + "\n",
	} {
		if b := e.read(filepath.Join(dir, name)); b != want {
			t.Errorf("drift dir %s = %q, want %q", name, b, want)
		}
	}

	// Without a raw listing (or an empty one) the report has none, and a
	// stale one from an earlier check is gone.
	writeDriftFile(t, raw, "")
	if out, err := e.run("check", measured, raw); err == nil {
		t.Fatalf("check of a drifted host succeeded:\n%s", out)
	}
	if _, err := os.Stat(filepath.Join(dir, "worker-env.raw.txt")); !os.IsNotExist(err) {
		t.Errorf("check without a raw listing kept worker-env.raw.txt (%v)", err)
	}
	if r := e.read(filepath.Join(dir, "report.md")); strings.Contains(r, "installed versions") {
		t.Errorf("report without a raw listing has one:\n%s", r)
	}
}

// TestRBEWorkerEnvDriftAwait: the watch beside a serving worker reports
// drift once the worker's drift upload ran, nothing if it was skipped (the
// host matched) or the worker job ended without it, and gives up with a
// warning at its deadline; a GitHub error is a retry, never a failure.
func TestRBEWorkerEnvDriftAwait(t *testing.T) {
	const job, step = "rbe pool worker (X)", "Upload the worker-env drift"
	e := newDriftEnv(t, "a\n")
	jobs := filepath.Join(e.dir, "jobs.json")
	for _, tc := range []struct {
		name, jobs, drift, say string
		extra                  []string
	}{
		{
			name: "uploaded", drift: "true", say: "completed (success)",
			jobs: `{"jobs":[{"name":"other","status":"completed","steps":[]},{"name":"` + job + `","status":"in_progress","steps":[{"name":"Measure","status":"completed","conclusion":"failure"},{"name":"` + step + `","status":"completed","conclusion":"success"}]}]}`,
		},
		{
			name: "host matched", drift: "false", say: "completed (skipped)",
			jobs: `{"jobs":[{"name":"` + job + `","status":"in_progress","steps":[{"name":"` + step + `","status":"completed","conclusion":"skipped"}]}]}`,
		},
		{
			name: "worker job ended first", drift: "false", say: "completed without",
			jobs: `{"jobs":[{"name":"` + job + `","status":"completed","steps":[{"name":"Set up job","status":"completed","conclusion":"failure"}]}]}`,
		},
		{
			name: "still provisioning at the deadline", drift: "false", say: "::warning title=rbe worker-env await::",
			jobs:  `{"jobs":[{"name":"` + job + `","status":"in_progress","steps":[{"name":"` + step + `","status":"pending","conclusion":null}]}]}`,
			extra: []string{"WORKER_ENV_AWAIT_SECONDS=0"},
		},
		{
			name: "worker job not listed yet", drift: "false", say: "::warning title=rbe worker-env await::",
			jobs: `{"jobs":[]}`, extra: []string{"WORKER_ENV_AWAIT_SECONDS=0"},
		},
		{
			name: "GitHub down", drift: "false", say: "::warning title=rbe worker-env await::",
			jobs: `{"jobs":[]}`, extra: []string{"WORKER_ENV_AWAIT_SECONDS=0", "GH_FAIL=1"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			writeDriftFile(t, jobs, tc.jobs)
			_ = os.Remove(e.output)
			_ = os.Remove(e.ghLog)
			e.extra = append([]string{"GH_JOBS=" + jobs, "GITHUB_RUN_ATTEMPT=3"}, tc.extra...)
			out, err := e.run("await", job, step)
			if err != nil {
				t.Fatalf("await: %v\n%s", err, out)
			}
			if got := strings.TrimSpace(e.read(e.output)); got != "drift="+tc.drift {
				t.Errorf("await output %q, want drift=%s\n%s", got, tc.drift, out)
			}
			if !strings.Contains(out, tc.say) {
				t.Errorf("await says %q, want %q", out, tc.say)
			}
			if log := e.read(e.ghLog); !strings.Contains(log, "api repos/acme/repo/actions/runs/42/attempts/3/jobs?per_page=100") {
				t.Errorf("await did not ask for this run attempt's jobs:\n%s", log)
			}
		})
	}
}

func (e *driftEnv) driftDir(measured string) (string, string) {
	e.t.Helper()
	path := filepath.Join(e.dir, "measured.txt")
	writeDriftFile(e.t, path, measured)
	if out, err := e.run("check", path); err == nil {
		e.t.Fatalf("check of a drifted host succeeded:\n%s", out)
	}
	_ = os.Remove(e.summary)
	return filepath.Join(e.dir, "worker-env-drift"), sha256Pin(measured)
}

// TestRBEWorkerEnvDriftReport: the first drift of a pin opens its issue
// (labeled, titled with the pin, the manifest in the body); a new
// measurement is added once; a host whose measurement is a previous pin (a
// stale image after a re-pin) opens nothing, so it cannot block the new pin.
func TestRBEWorkerEnvDriftReport(t *testing.T) {
	e := newDriftEnv(t, "a\n")
	commits := filepath.Join(e.dir, "commits")
	files := filepath.Join(e.dir, "files")
	view := filepath.Join(e.dir, "view")
	writeDriftFile(t, commits, "c1\nc0\n")
	writeDriftFile(t, filepath.Join(files, "c1"), buildWithPin(e.pin))
	writeDriftFile(t, filepath.Join(files, "c0"), buildWithPin(sha256Pin("old\n")))
	writeDriftFile(t, view, "")
	e.extra = []string{"GH_COMMITS=" + commits, "GH_FILES=" + files, "GH_VIEW=" + view, "DEFAULT_BRANCH=main"}

	// Nothing recorded (the worker failed for another reason): no GitHub call.
	if out, err := e.run("report", filepath.Join(e.dir, "nothing")); err != nil || e.read(e.ghLog) != "" {
		t.Fatalf("report without drift: %v, gh calls %q\n%s", err, e.read(e.ghLog), out)
	}

	dir, got := e.driftDir("new\n")
	out, err := e.run("report", dir)
	if err != nil {
		t.Fatalf("report: %v\n%s", err, out)
	}
	log := e.read(e.ghLog)
	if !strings.Contains(log, "api repos/acme/repo/commits?path=platforms/BUILD.bazel&sha=main&per_page=20") {
		t.Errorf("report did not look up the previous pins:\n%s", log)
	}
	if !strings.Contains(log, "label create "+rbeWorkerEnvLabel+" -R acme/repo --force") {
		t.Errorf("report did not ensure the label:\n%s", log)
	}
	create := regexp.MustCompile(`(?m)^issue create -R acme/repo --title ` + regexp.QuoteMeta(driftTitle(e.pin)) + ` --label ` + rbeWorkerEnvLabel + ` --body-file (\S+)$`).FindStringSubmatch(log)
	if create == nil {
		t.Fatalf("report did not open the pin's issue:\n%s", log)
	}
	if body := e.read(create[1]); !strings.Contains(body, "```\nnew\n```") || !strings.Contains(body, `"worker-env": "`+got+`",`) {
		t.Errorf("issue body lacks the manifest or the pin line:\n%s", body)
	}

	// The issue is open: a new measurement is a comment, a known one nothing.
	e.setIssues(driftTitle(e.pin), "https://x/issues/9")
	_ = os.Remove(e.ghLog)
	if out, err := e.run("report", dir); err != nil || !strings.Contains(e.read(e.ghLog), "issue comment https://x/issues/9 -R acme/repo --body-file") {
		t.Errorf("report of a new measurement: %v, gh calls:\n%s\n%s", err, e.read(e.ghLog), out)
	}
	writeDriftFile(t, view, "earlier report\nworker-env="+got+"\n")
	_ = os.Remove(e.ghLog)
	if out, err := e.run("report", dir); err != nil || strings.Contains(e.read(e.ghLog), "issue comment") || strings.Contains(e.read(e.ghLog), "issue create") {
		t.Errorf("report of a known measurement: %v, gh calls:\n%s\n%s", err, e.read(e.ghLog), out)
	}

	// A stale host: it measures the pin of an earlier default-branch commit.
	e.setIssues()
	dir, _ = e.driftDir("old\n")
	_ = os.Remove(e.ghLog)
	out, err = e.run("report", dir)
	if err != nil || !strings.Contains(out, "::warning title=rbe worker-env stale host::") {
		t.Errorf("report of a stale host: %v\n%s", err, out)
	}
	if log := e.read(e.ghLog); strings.Contains(log, "issue create") || strings.Contains(log, "label create") {
		t.Errorf("report of a stale host touched issues:\n%s", log)
	}
}

// TestRBEWorkerEnvDriftResolve: once the default branch pins another host,
// the old pins' issues close; the current pin's stays, with a warning.
func TestRBEWorkerEnvDriftResolve(t *testing.T) {
	e := newDriftEnv(t, "a\n")
	e.setIssues(driftTitle(sha256Pin("old\n")), "https://x/issues/1", driftTitle(e.pin), "https://x/issues/2")
	out, err := e.run("resolve")
	if err != nil {
		t.Fatalf("resolve: %v\n%s", err, out)
	}
	log := e.read(e.ghLog)
	if !strings.Contains(log, "issue close https://x/issues/1 -R acme/repo --comment Superseded") {
		t.Errorf("resolve did not close the superseded issue:\n%s", log)
	}
	if strings.Contains(log, "issues/2 ") || !strings.Contains(out, "::warning title=rbe worker-env drift::this host matches the pin "+e.pin+", but https://x/issues/2 is open") {
		t.Errorf("resolve must leave the current pin's issue open and warn:\n%s\n%s", log, out)
	}
}

// TestRBEWorkerScriptMeasureMode: measure mode needs no certificate and
// stops after the check, failing on drift; the other modes go on to
// NativeLink whatever the check says.
func TestRBEWorkerScriptMeasureMode(t *testing.T) {
	script := readFile(t, repoRoot(t), rbeWorkerScript)
	at := 0
	for _, want := range []string{
		"WORKER_MODE=${WORKER_MODE:-run}\n",
		"[ \"$WORKER_MODE\" = measure ] || : \"${RBE_WORKER_TLS_CERT:?}\" \"${RBE_WORKER_TLS_KEY:?}\" \"${RBE_WEST_HOST:?}\" \"${WORKER_NAME:?}\"\n",
		"measure) ;;\n",
		`*) echo "WORKER_MODE must be run, pool or measure" >&2; exit 2 ;;`,
		"\n\"$HERE/worker-env\" >\"$RUNNER_TEMP/worker-env.txt\"\n",
		"\nif ! (cd \"$RBE_PRODUCT_ROOT\" && \"$HERE/worker-env-drift\" check \"$RUNNER_TEMP/worker-env.txt\" \"$RUNNER_TEMP/worker-env.raw.txt\"); then\n\t[ \"$WORKER_MODE\" != measure ] || exit 3\n",
		"\n[ \"$WORKER_MODE\" != measure ] || exit 0\n",
		"/nativelink-${NL_VERSION}-x86_64-unknown-linux-musl.tar.gz",
	} {
		i := strings.Index(script[at:], want)
		if i < 0 {
			t.Fatalf("%s: %q missing or out of order", rbeWorkerScript, want)
		}
		at += i + len(want)
	}
}

// workerStubs is a host for blacksmith-worker.sh with nothing real behind
// it: sudo and apt-get do nothing, downloads are empty files whose checksums
// pass, the NativeLink archive unpacks to $NL_STUB, sleep returns once
// NativeLink has started, and dolt, go, uname and dpkg-query describe a host
// that is not the pinned one.
var workerStubs = map[string]string{
	"sudo": "echo \"sudo $*\" >>\"$STUB_LOG\"\n",
	"curl": `out=
prev=
for a; do [ "$prev" = -o ] && out=$a; prev=$a; done
if [ -n "$out" ]; then echo stub >"$out"; else echo '[]'; fi
`,
	"sha256sum": `if [ "${1:-}" = -c ]; then cat >/dev/null; exit 0; fi
for p in /usr/bin/sha256sum /bin/sha256sum; do [ -x "$p" ] && exec "$p" "$@"; done
exit 127
`,
	"tar": `dir=
prev=
for a; do [ "$prev" = -C ] && dir=$a; prev=$a; done
for a; do
	case "$a" in *nl.tgz) cp "$NL_STUB" "$dir/nativelink"; chmod +x "$dir/nativelink"; exit 0 ;; esac
done
for p in /usr/bin/tar /bin/tar; do [ -x "$p" ] && exec "$p" "$@"; done
exit 127
`,
	"sleep": `i=0
while [ ! -e "$NL_CAPTURE" ] && [ $i -lt 200 ]; do
	for p in /usr/bin/sleep /bin/sleep; do [ -x "$p" ] && { "$p" 0.05; break; }; done
	i=$((i + 1))
done
`,
	"uname":      "echo x86_64\n",
	"dolt":       "echo 'dolt version 2.1.8'\n",
	"go":         "echo 'go version go0.0.0-drifted linux/amd64'\n",
	"dpkg-query": "for p; do :; done\nprintf 'installed 9.9-drifted-%s\\n' \"$p\"\n",
}

// TestRBEWorkerRegistersOnDrift runs blacksmith-worker.sh on a host whose
// measurement is not the pin. In pool mode it still starts NativeLink, with
// worker.json advertising the measured hash (so actions without worker-env
// run on it and those that carry the pin never do), and
// leaves the drift report. In measure mode the same host fails (exit 3) and
// starts nothing.
func TestRBEWorkerRegistersOnDrift(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("blacksmith-worker.sh runs only on Linux pool hosts (sha256sum, nproc, GNU xargs)")
	}
	root := repoRoot(t)
	dir := t.TempDir()
	stubs := filepath.Join(dir, "bin")
	for name, body := range workerStubs {
		writeDriftFile(t, filepath.Join(stubs, name), "#!/bin/sh\n"+body)
		if err := os.Chmod(filepath.Join(stubs, name), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	nlStub := filepath.Join(dir, "nativelink-stub")
	writeDriftFile(t, nlStub, "#!/bin/sh\ncp \"$1\" \"$NL_CAPTURE.tmp\" && mv -f \"$NL_CAPTURE.tmp\" \"$NL_CAPTURE\"\n")
	osRelease := filepath.Join(dir, "os-release")
	writeDriftFile(t, osRelease, "ID=ubuntu\nVERSION_ID=\"24.04\"\n")
	path := stubs + string(os.PathListSeparator) + "/usr/bin" + string(os.PathListSeparator) + "/bin"

	run := func(mode string) (tmp, capture, out string, err error) {
		tmp = filepath.Join(dir, mode)
		capture = filepath.Join(tmp, "nativelink-started-with.json")
		if err := os.MkdirAll(tmp, 0o755); err != nil {
			t.Fatal(err)
		}
		env := []string{
			"PATH=" + path, "WORKER_ENV_PATH=" + path, "WORKER_ENV_OS_RELEASE=" + osRelease,
			"HOME=" + tmp, "GOTOOLCHAIN=local", "RUNNER_TEMP=" + tmp, "RUNNER_NAME=runner-1", "GITHUB_REPOSITORY=acme/repo", "GITHUB_RUN_ID=42",
			"GITHUB_STEP_SUMMARY=" + filepath.Join(tmp, "summary.md"),
			"STUB_LOG=" + filepath.Join(tmp, "stub.log"), "NL_STUB=" + nlStub, "NL_CAPTURE=" + capture,
			"WORKER_MODE=" + mode, "POOL_IDLE_MINUTES=1", "RBE_ACTION_ISOLATION=0",
		}
		// Measure mode runs with WORKER_MODE alone (the canary, the bazel
		// jobs): no certificate, no farm host, no worker name.
		if mode != "measure" {
			env = append(env, "RBE_WORKER_TLS_CERT=eA==", "RBE_WORKER_TLS_KEY=eA==", "RBE_WEST_HOST=rbe.invalid", "WORKER_NAME=w-1")
		}
		stdout, stderr, err := runRBEScript(root, env, filepath.Join(root, rbeWorkerScript))
		return tmp, capture, stdout + stderr, err
	}

	tmp, capture, out, err := run("pool")
	if err != nil {
		t.Fatalf("pool worker on a drifted host: %v\n%s", err, out)
	}
	measured, err := os.ReadFile(filepath.Join(tmp, "worker-env.txt"))
	if err != nil {
		t.Fatal(err)
	}
	got := sha256Pin(string(measured))
	pin := rbeWorkerPlatformExecProperties(t, readFile(t, root, rbeWorkerPlatformBuild))[rbeWorkerEnvProperty]
	if got == pin {
		t.Fatalf("the stub host measures the pin %s; the test needs a drifted one", pin)
	}
	if !strings.Contains(out, "worker-env: registering anyway with worker-env="+got) {
		t.Errorf("pool worker does not say it registers with the measured hash:\n%s", out)
	}
	started, err := os.ReadFile(capture)
	if err != nil {
		t.Fatalf("NativeLink never started on the drifted host: %v\n%s", err, out)
	}
	if err := checkWorkerJSONAdvertises(started, got); err != nil {
		t.Errorf("NativeLink's worker.json: %v\n%s", err, started)
	}
	if m := regexp.MustCompile(`"OSFamily"|"ISA"`).FindAllString(string(started), -1); len(m) != 2 {
		t.Errorf("worker.json lost its other platform properties: %s", started)
	}
	if b := readDriftOut(t, filepath.Join(tmp, "worker-env-drift", "measured-pin")); b != got+"\n" {
		t.Errorf("drift report measured-pin %q, want %s", b, got)
	}
	if s := readDriftOut(t, filepath.Join(tmp, "summary.md")); !strings.Contains(s, "### rbe worker-env drift") {
		t.Errorf("no drift report in the step summary:\n%s", s)
	}

	_, capture, out, err = run("measure")
	if code := exitCode(err); code != 3 {
		t.Errorf("measure on a drifted host: exit %d (%v), want 3\n%s", code, err, out)
	}
	if _, err := os.Stat(capture); !os.IsNotExist(err) {
		t.Errorf("measure mode started NativeLink (%v)", err)
	}
}

// pipeOverflow is more than any pipe buffer holds (64 KiB on Linux, 16-64
// KiB on macOS): a writer of this much into a reader that stops early is
// killed by SIGPIPE every time, never only when the race goes that way.
const pipeOverflow = 1 << 20

// defaultSIGPIPE gives the scripts this test runs the default SIGPIPE, as
// on GitHub runners, even when the test inherited it ignored (some agent
// sandboxes do): a signal ignored at exec stays ignored, and an ignored
// SIGPIPE turns the kill into an EPIPE that some writers (jq) shrug off.
// A handled signal is reset to its default at exec.
func defaultSIGPIPE(t *testing.T) {
	t.Helper()
	signal.Notify(make(chan os.Signal, 1), syscall.SIGPIPE)
	t.Cleanup(func() { signal.Reset(syscall.SIGPIPE) })
}

// TestRBEWorkerEnvDriftSummaryDrainsStdin: summary always reads its input,
// with or without a step summary file. A summary that returned unread let
// `echo ... | summary` die of SIGPIPE under pipefail, so preflight exited 141
// instead of 1, now and then (the hook passes /dev/null to keep it away).
func TestRBEWorkerEnvDriftSummaryDrainsStdin(t *testing.T) {
	defaultSIGPIPE(t)
	root := repoRoot(t)
	for _, tc := range []struct {
		name string
		env  []string
	}{
		{name: "unset"},
		{name: "empty", env: []string{"GITHUB_STEP_SUMMARY="}},
		{name: "file", env: []string{"GITHUB_STEP_SUMMARY=" + filepath.Join(t.TempDir(), "summary.md")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			script := `. "$1" && head -c ` + strconv.Itoa(pipeOverflow) + ` /dev/zero | summary`
			// bash -c SCRIPT $0 $1: SCRIPT sources $1, the script under test.
			stdout, stderr, err := runRBEScript(t.TempDir(), append([]string{"PATH=/usr/bin:/bin"}, tc.env...),
				"-c", script, "bash", filepath.Join(root, rbeWorkerEnvDrift))
			if err != nil {
				t.Fatalf("writer | summary: exit %d (%v), want 0 (141 is SIGPIPE: summary left its input unread)\n%s%s", exitCode(err), err, stdout, stderr)
			}
			for _, kv := range tc.env {
				if path, ok := strings.CutPrefix(kv, "GITHUB_STEP_SUMMARY="); ok && path != "" {
					if fi, err := os.Stat(path); err != nil || fi.Size() != pipeOverflow {
						t.Errorf("step summary %s: %v, want %d bytes", path, err, pipeOverflow)
					}
				}
			}
		})
	}
}

// TestRBEWorkerEnvDriftReadsWholePipes: no pipe in worker-env-drift ends in a
// reader that stops early (grep -q, head -n 1). Under pipefail the writer's
// SIGPIPE fails the pipeline, so a found match reads as no match: preflight
// let a remote run on a drifted pin queue, report re-commented a known
// measurement, and await missed a finished upload.
func TestRBEWorkerEnvDriftReadsWholePipes(t *testing.T) {
	const job, step = "rbe pool worker (X)", "Upload the worker-env drift"
	defaultSIGPIPE(t)
	e := newDriftEnv(t, "a\n")

	// A pin with many open issues of its title: their list overflows the pipe.
	var issues []string
	for i := 0; i < 10000; i++ {
		issues = append(issues, driftTitle(e.pin), "https://x/issues/"+strconv.Itoa(7+i))
	}
	e.setIssues(issues...)
	base := filepath.Join(e.dir, "base.bazel")
	changed := filepath.Join(e.dir, "changed")
	writeDriftFile(t, base, buildWithPin(e.pin))
	writeDriftFile(t, changed, "x\n")
	e.extra = []string{"WORKER_ENV_REMOTE=true"}
	out, err := e.run("preflight", base, changed)
	if code := exitCode(err); code != 1 || !strings.Contains(out, "::error title=rbe worker-env drift::https://x/issues/7:") {
		t.Errorf("preflight on a pin with many drift issues: exit %d, want 1 naming the first issue\n%s", code, out)
	}
	e.setIssues()

	// A run attempt whose jobs overflow the pipe, the worker's listed first.
	jobs := filepath.Join(e.dir, "jobs.json")
	entry := `{"name":"` + job + `","status":"in_progress","steps":[{"name":"` + step + `","status":"completed","conclusion":"success"}]}`
	writeDriftFile(t, jobs, `{"jobs":[`+strings.TrimSuffix(strings.Repeat(entry+",", pipeOverflow/len(entry)+1), ",")+`]}`)
	e.extra = []string{"GH_JOBS=" + jobs, "WORKER_ENV_AWAIT_SECONDS=0"}
	_ = os.Remove(e.output)
	if out, err := e.run("await", job, step); err != nil || strings.TrimSpace(e.read(e.output)) != "drift=true" {
		t.Errorf("await with the upload done: %v, output %q, want drift=true\n%s", err, e.read(e.output), out)
	}

	// report: the issue already holds this measurement, at the top of a
	// thread that overflows the pipe.
	commits := filepath.Join(e.dir, "commits")
	files := filepath.Join(e.dir, "files")
	view := filepath.Join(e.dir, "view")
	writeDriftFile(t, commits, "c0\n")
	writeDriftFile(t, filepath.Join(files, "c0"), buildWithPin(e.pin))
	e.extra = []string{"GH_COMMITS=" + commits, "GH_FILES=" + files, "GH_VIEW=" + view, "DEFAULT_BRANCH=main"}
	dir, got := e.driftDir("new\n")
	writeDriftFile(t, view, "worker-env="+got+"\n"+strings.Repeat("later comment\n", pipeOverflow/len("later comment\n")+1))
	e.setIssues(driftTitle(e.pin), "https://x/issues/9")
	_ = os.Remove(e.ghLog)
	if out, err := e.run("report", dir); err != nil || strings.Contains(e.read(e.ghLog), "issue comment") {
		t.Errorf("report of a known measurement: %v, gh calls:\n%s\n%s", err, e.read(e.ghLog), out)
	}
}

func readDriftOut(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func exitCode(err error) int {
	var ee interface{ ExitCode() int }
	if errors.As(err, &ee) {
		return ee.ExitCode()
	}
	if err != nil {
		return -1
	}
	return 0
}
