package scripts_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

// worker-env tiers (rbe-reexecution-design.md (c), slice S4b). A pool worker
// advertises a second key, worker-env-base: the sha256 of its manifest
// without the go and dolt lines. An action that sends only worker-env-base
// (beads) gets no host go or dolt: rbe-action-launch binds
// undeclared-host-tool (exit 127) over every one it could reach, so a Go or
// dolt change on the workers can move none of its keys and none of its
// results can have used them. Only a worker whose actions go through that
// launcher (isolation on) advertises the key. The launcher's mounts run in a
// mount namespace (root); the selftest proves them on every worker before
// NativeLink starts, and a privileged-container run of isolate() did the same
// for this change (commit message).

const (
	workerEnvBaseLine = `WORKER_ENV_BASE=sha256:$(grep -v -E '^(go|dolt) ' "$RUNNER_TEMP/worker-env.txt" | sha256sum | cut -d' ' -f1)` + "\n"
	sha64a            = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	sha64b            = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

// TestRBEWorkerEnvBaseIsTheManifestWithoutGoAndDolt: the base is measured
// right after worker-env, from the same file, dropping exactly the lines
// worker/worker-env writes for go and dolt; beads computes the same
// projection of gascity's committed manifest for its pin.
func TestRBEWorkerEnvBaseIsTheManifestWithoutGoAndDolt(t *testing.T) {
	root := repoRoot(t)
	script := readFile(t, root, rbeWorkerScript)
	full := strings.Index(script, "\nWORKER_ENV=sha256:")
	base := strings.Index(script, "\n"+workerEnvBaseLine)
	if full < 0 || base < full {
		t.Fatalf("%s: want %q right after the WORKER_ENV measurement", rbeWorkerScript, workerEnvBaseLine)
	}
	if n := strings.Count(script, "WORKER_ENV_BASE="); n != 1 {
		t.Errorf("%s assigns WORKER_ENV_BASE %d times", rbeWorkerScript, n)
	}
	// worker/worker-env names those two lines "go <go version>" and
	// "dolt <dolt version>": the projection must match them and nothing else.
	measure := readFile(t, root, rbeWorkerEnvScript)
	for _, want := range []string{`echo "go $go_version"`, `echo "dolt $dolt_version"`} {
		if !strings.Contains(measure, want) {
			t.Errorf("%s: no %s line; worker-env-base's grep -v -E '^(go|dolt) ' assumes it", rbeWorkerEnvScript, want)
		}
	}
	manifest := "arch x86_64\ndolt dolt version 2.1.8\ngo go version go1.26.9 linux/amd64\nos ubuntu 24.04\npkg golang-x 1\ntool goyq 4\n"
	cmd := exec.Command("bash", "-c", `grep -v -E '^(go|dolt) '`)
	cmd.Stdin = strings.NewReader(manifest)
	out, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(out), "arch x86_64\nos ubuntu 24.04\npkg golang-x 1\ntool goyq 4\n"; got != want {
		t.Errorf("projection %q, want %q (only the go and dolt lines dropped)", got, want)
	}
}

// TestRBEWorkerAdvertisesWorkerEnvBaseOnlyWithIsolation runs render()'s jq
// program: with the isolation keys the worker advertises worker-env-base and
// passes both properties to the launcher; without them (RBE_ACTION_ISOLATION=0,
// or a failed canary's fallback) it advertises neither, so a base-only action
// never lands on a worker that would not hide the host's go and dolt.
func TestRBEWorkerAdvertisesWorkerEnvBaseOnlyWithIsolation(t *testing.T) {
	root := repoRoot(t)
	prog, iso := workerJSONProgram(t, root)
	if !strings.Contains(readFile(t, root, rbeWorkerScript), `--arg worker_env_base "${WORKER_ENV_BASE:-}"`) {
		t.Errorf("%s: render() does not pass WORKER_ENV_BASE", rbeWorkerScript)
	}
	render := func(isolation string, base bool) map[string]any {
		args := []string{"-n", "--arg", "host", "grpcs://h:443", "--arg", "root", "/r", "--arg", "store", "/r",
			"--arg", "name", "w", "--argjson", "slots", "2", "--arg", "tier", "oss", "--arg", "worker_env", sha64a}
		if base {
			args = append(args, "--arg", "worker_env_base", sha64b)
		}
		args = append(args, "--argjson", "isolation", isolation, prog)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		var stderr bytes.Buffer
		cmd := exec.CommandContext(ctx, "jq", args...)
		cmd.Stderr = &stderr
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("jq: %v\n%s", err, stderr.String())
		}
		var cfg struct {
			Workers []struct {
				Local map[string]any `json:"local"`
			} `json:"workers"`
		}
		if err := json.Unmarshal(out, &cfg); err != nil {
			t.Fatal(err)
		}
		return cfg.Workers[0].Local
	}
	props := func(local map[string]any) map[string]any { return local["platform_properties"].(map[string]any) }

	on := render(iso, true)
	if got := fmt.Sprint(props(on)["worker-env-base"]); got != "map[values:["+sha64b+"]]" {
		t.Errorf("isolation on: worker-env-base %s, want %s", got, sha64b)
	}
	if got := fmt.Sprint(props(on)["worker-env"]); got != "map[values:["+sha64a+"]]" {
		t.Errorf("isolation on: worker-env %s, want it unchanged", got)
	}
	ae, _ := on["additional_environment"].(map[string]any)
	if fmt.Sprint(ae["RBE_X_WORKER_ENV"]) != "map[property:worker-env]" || fmt.Sprint(ae["RBE_X_WORKER_ENV_BASE"]) != "map[property:worker-env-base]" {
		t.Errorf("isolation on: additional_environment %v lacks the two properties", ae)
	}
	for name, local := range map[string]map[string]any{
		"isolation off":          render("{}", true),
		"isolation on, no base":  render(iso, false),
		"isolation off, no base": render("{}", false),
	} {
		if _, ok := props(local)["worker-env-base"]; ok {
			t.Errorf("%s: advertises worker-env-base", name)
		}
	}
}

// TestRBEActionHostTools: the launcher's decision, run here in bash. Only
// worker-env-base without worker-env masks; worker-env (with or without the
// base) or neither keeps today's behaviour; a malformed value is refused, never
// guessed. The action can override both (its environment wins), but naming
// worker-env only gives back what its own key names.
func TestRBEActionHostTools(t *testing.T) {
	root := repoRoot(t)
	launch := readFile(t, root, "worker/rbe-action-launch")
	fn := regexp.MustCompile(`(?s)\naction_host_tools\(\) \{\n.*?\n\}\n`).FindString(launch)
	if fn == "" {
		t.Fatal("rbe-action-launch: no action_host_tools() function")
	}
	cases := []struct{ wenv, wbase, want string }{
		{"", "", "host"},
		{sha64a, "", "host"},
		{sha64a, sha64b, "host"},
		{"", sha64b, "masked"},
		{"", "sha256:" + strings.Repeat("B", 64), "refused"},
		{"", "sha256:abc", "refused"},
		{"", sha64b + "\n", "refused"},
		{"", " " + sha64b, "refused"},
		{"bogus", sha64b, "refused"},
		{"", "md5:" + strings.Repeat("a", 64), "refused"},
	}
	script := fn
	env := map[string]string{}
	for i, c := range cases {
		env["E"+strconv.Itoa(i)], env["B"+strconv.Itoa(i)] = c.wenv, c.wbase
		script += fmt.Sprintf("if r=$(action_host_tools \"$E%[1]d\" \"$B%[1]d\"); then echo \"%[1]d $r\"; else echo \"%[1]d refused\"; fi\n", i)
	}
	out, err := runWorkflowStepScript(t, t.TempDir(), script, env)
	if err != nil {
		t.Fatalf("action_host_tools: %v\n%s", err, out)
	}
	got := strings.Split(strings.TrimSpace(out), "\n")
	if len(got) != len(cases) {
		t.Fatalf("%d results for %d cases:\n%s", len(got), len(cases), out)
	}
	for i, c := range cases {
		if want := strconv.Itoa(i) + " " + c.want; got[i] != want {
			t.Errorf("RBE_X_WORKER_ENV=%q RBE_X_WORKER_ENV_BASE=%q: got %q, want %q", c.wenv, c.wbase, got[i], want)
		}
	}

	for _, want := range []string{
		"HOST_TOOL_STUB=/usr/local/libexec/rbe-action/undeclared-host-tool\n",
		// Control values: validated, stripped, never given twice.
		"\t\tRBE_X_WORKER_ENV=*)\n\t\t\t((!wenv_set)) || die \"RBE_X_WORKER_ENV given twice\"\n",
		"\t\tRBE_X_WORKER_ENV_BASE=*)\n\t\t\t((!wbase_set)) || die \"RBE_X_WORKER_ENV_BASE given twice\"\n",
		"\ttools=$(action_host_tools \"$wenv\" \"$wbase\") ||\n",
		`if [[ $tools == masked ]]; then envs+=("RBE_HOST_TOOLS=masked"); fi`,
		// pid 1 masks before ROOT_RO's remount walk, and refuses anything else.
		"\tmasked) mask_host_tools ;;\n\thost) ;;\n",
		// Every reachable go, gofmt and dolt (PATH directories and Go roots,
		// symlinks resolved: the image's /usr/bin/go is the toolcache's), and
		// each Go root's pkg.
		`for root in /usr/local/go /opt/hostedtoolcache/go/*/*; do`,
		`for d in /usr/local/bin /usr/bin /bin /usr/local/sbin /usr/sbin /sbin "${roots[@]/%//bin}"; do`,
		"\t\tfor t in go gofmt dolt; do\n\t\t\tf=$(realpath -e -- \"$d/$t\" 2>/dev/null) || continue\n",
		`mount --bind "$HOST_TOOL_STUB" "$f"`,
		`mount -t tmpfs -o ro,size=4k,mode=0555,nosuid,nodev rbe-nohost "$root/pkg"`,
	} {
		if !strings.Contains(launch, want) {
			t.Errorf("rbe-action-launch missing %q", want)
		}
	}
	if mask, walk := strings.Index(launch, "\tmasked) mask_host_tools ;;"), strings.Index(launch, "if [[ $ROOT_RO == 1 ]]; then mount --bind \"$RUN\" \"$RUN\"; fi"); mask < 0 || walk < mask {
		t.Errorf("rbe-action-launch: mask_host_tools must run before the ROOT_RO walk (mask %d, walk %d)", mask, walk)
	}

	stub := readFile(t, root, "worker/undeclared-host-tool")
	if !strings.HasPrefix(stub, "#!/bin/sh\n") || !strings.HasSuffix(stub, "\nexit 127\n") || !strings.Contains(stub, "worker-env-base") {
		t.Errorf("worker/undeclared-host-tool must be a sh script that says why and exits 127:\n%s", stub)
	}
	script = readFile(t, root, rbeWorkerScript)
	if !strings.Contains(script, `sudo install -m 0755 "$HERE/undeclared-host-tool" "$LIB/undeclared-host-tool"`) {
		t.Errorf("%s does not install worker/undeclared-host-tool", rbeWorkerScript)
	}
	selftest := readFile(t, root, "worker/rbe-action-selftest")
	for _, want := range []string{
		"for f in launch sweep selftest undeclared-host-tool; do",
		`check "base only: every host go, gofmt and dolt exits 127`,
		`check "base only: RBE_HOST_TOOLS=masked, the Go root's pkg empty, exit 0"`,
		`check "$what: the host's go, gofmt and dolt run as on the host"`,
		`check "a malformed RBE_X_WORKER_ENV_BASE: refused, never run (exit 125)"`,
		`check "platform_properties: worker-env-base (sha256) beside worker-env"`,
	} {
		if !strings.Contains(selftest, want) {
			t.Errorf("rbe-action-selftest missing %q", want)
		}
	}
}
