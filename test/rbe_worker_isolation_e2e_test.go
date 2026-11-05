//go:build integration

package scripts_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"
)

// The isolation phases end to end, as blacksmith-worker.sh runs them on a
// pool VM: the real isolate() (packages, slot users, the launcher built and
// installed, sudoers, the slot nft chain, the reserved port, render, userns
// for fork, the full selftest, sudo, sockets, the probes), then the hot
// sidecar's start (its serve probe and reach probe, with a stub sidecar on
// :50090), then NativeLink's start (a stub). Then, in the same boot, every
// probe again. Each tier in turn, the OSS tier first, cleaned up between.
//
// A probe that reuses an action directory name fails it two ways: the
// launcher refuses a second launch of the same name and inode (rbe-action:
// "action directory ... was already launched", the 2026-11-04 OSS outage),
// and, since the inode may differ, the test also requires every claim the
// launcher recorded to name a distinct directory.
//
// It changes the host (users, /usr/local/libexec/rbe-action, /etc/sudoers.d,
// nft, sysctls; fork: user.max_user_namespaces=0), so it runs only with
// RBE_E2E_ISOLATION=1, as a non-root user with passwordless sudo: a
// throwaway VM or container (test/e2e/isolation-in-docker.sh). Not under -short.
func TestRBEWorkerIsolationE2E(t *testing.T) {
	if os.Getenv("RBE_E2E_ISOLATION") != "1" {
		t.Skip("set RBE_E2E_ISOLATION=1 on a throwaway host (it changes users, nft and sysctls)")
	}
	if os.Geteuid() == 0 {
		t.Fatal("run as the worker user (non-root, passwordless sudo), as on a runner")
	}
	if err := exec.Command("sudo", "-n", "true").Run(); err != nil {
		t.Fatalf("passwordless sudo: %v", err)
	}
	root := repoRoot(t)
	script := readFile(t, root, rbeWorkerScript)
	from := strings.Index(script, "\nLIB=/usr/local/libexec/rbe-action\n")
	to := strings.Index(script, "\nnl=$!\n")
	if from < 0 || to < from {
		t.Fatalf("%s: no section from LIB= to nl=$!", rbeWorkerScript)
	}
	section := script[from : to+len("\nnl=$!\n")]
	tiers := []string{"oss", "fork"}
	if v := os.Getenv("RBE_E2E_TIERS"); v != "" {
		tiers = strings.Fields(v)
	}
	for _, tier := range tiers {
		t.Run(tier, func(t *testing.T) {
			runIsolationE2E(t, root, section, tier)
			cleanupIsolation(t)
		})
	}
}

func runIsolationE2E(t *testing.T, root, section, tier string) {
	home, _ := os.UserHomeDir()
	base := filepath.Join(home, "rbe-e2e-"+tier)
	os.RemoveAll(base)
	t.Cleanup(func() { exec.Command("sudo", "-n", "rm", "-rf", base).Run() })
	temp, nlRoot, bin := filepath.Join(base, "temp"), filepath.Join(base, "nl-worker"), filepath.Join(base, "nl-bin")
	// As blacksmith-worker.sh lays them out (mkdir "$STORE"/{content,tmp}).
	for _, d := range []string{temp, bin, filepath.Join(nlRoot, "work"), filepath.Join(nlRoot, "pki"),
		filepath.Join(nlRoot, "content"), filepath.Join(nlRoot, "tmp")} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	// The worker key the probe must not read.
	if err := os.WriteFile(filepath.Join(nlRoot, "pki", "worker.key"), []byte("SECRET\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(bin, "nativelink"), []byte("#!/bin/sh\ncp \"$1\" \"$1.started\"\n"), 0o755)
	// A stub sidecar: listens on 127.0.0.1:50090 until killed.
	os.WriteFile(filepath.Join(temp, "rbe-hotread"), []byte("#!/usr/bin/env python3\nimport socket\n"+
		"s = socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1); s.bind(('127.0.0.1', 50090)); s.listen(8)\n"+
		"while True:\n    c, _ = s.accept(); c.close()\n"), 0o755)
	netns := map[string]string{"oss": "0", "fork": "1"}[tier]
	// Round 2, the same boot: every probe again, each must take a fresh
	// directory. The serve probe needs :50090 free (the stub sidecar
	// stopped), the reach probe a listener there (the stub again).
	round2 := `
echo "== round 2"
# As at boot: the isolation probes before anything holds :50090.
if [ -n "${hot_pid:-}" ]; then kill "$hot_pid"; wait "$hot_pid" 2>/dev/null || true; fi
isolation_probes
if [ -n "${hot_pid:-}" ]; then
	if [ "$NETNS" = 0 ]; then
		r=$(hot_serve_probe); [ "$r" = 0 ] || { echo "round 2 serve probe: $r"; exit 1; }
	fi
	"$RUNNER_TEMP/rbe-hotread" & stub=$!
	for _ in $(seq 1 50); do (exec 3<>/dev/tcp/127.0.0.1/50090) 2>/dev/null && break; sleep 0.1; done
	r=$(hot_reach_probe); kill "$stub"; [ "$r" = 1 ] || { echo "round 2 reach probe: $r"; exit 1; }
fi
echo "ROUND2-OK"
`
	prog := "set -euo pipefail\n" +
		fmt.Sprintf("ACTION_ISOLATION=1 slots=4 STORE=$ROOT WORK=$ROOT/work wire_zstd=true WORKER_TIER=%s NETNS=%s\n", tier, netns) +
		"(exit 0) & hot_build_pid=$!\n" + section + "wait \"$nl\"\n" + round2
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "bash", "-c", prog)
	cmd.Dir = filepath.Join(root, "worker")
	cmd.Env = []string{
		"PATH=" + bin + ":/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin", "HOME=" + home,
		"RUNNER_TEMP=" + temp, "ROOT=" + nlRoot, "NL_BIN_DIR=" + bin, "HERE=" + filepath.Join(root, "worker"),
		"RBE_WEST_HOST=rbe-west.example.invalid", "RBE_WEST_PORT=443", "WORKER_NAME=e2e-" + tier,
		"WORKER_ENV=" + rbeWorkerEnvSample, "WORKER_MODE=pool", "GITHUB_RUN_ID=7",
		"ZSTD_READ_URL=grpcs://rbe-zread.example.invalid:443", "RBE_HOT_URL=https://rbe-warm.example.invalid/h/" + tier,
		"RBE_HOT_GRACE=20", "RBE_WORKER_TLS_KEY=STEP-ENV-SECRET",
	}
	out, runErr := cmd.CombinedOutput()
	log := string(out)
	t.Logf("tier %s output:\n%s", tier, tailString(log, 6000))
	// The claims first: a reused name shows even when the run stopped.
	defer func() {
		if runErr != nil {
			t.Fatalf("tier %s: the isolation sequence failed: %v", tier, runErr)
		}
	}()
	for _, want := range []string{
		"isolation: probe\n",
		"rbe-hot: status=on ",
		"ok    action: hot sidecar refused (127.0.0.1:50090)",
		"ROUND2-OK\n",
	} {
		if !strings.Contains(log, want) {
			t.Errorf("tier %s: output lacks %q", tier, want)
		}
	}
	if tier == "oss" {
		for _, want := range []string{"isolation probe: loopback connect from :50090 passes", "ok    action: cannot serve on 127.0.0.1:50090"} {
			if !strings.Contains(log, want) {
				t.Errorf("tier oss: output lacks %q", want)
			}
		}
	}
	if strings.Contains(log, "was already launched") {
		t.Errorf("tier %s: the launcher refused a reused action directory", tier)
	}
	started, err := os.ReadFile(filepath.Join(nlRoot, "worker.json.started"))
	if runErr == nil && (err != nil || !bytes.Contains(started, []byte(`"grpc://127.0.0.1:50090"`))) {
		t.Errorf("tier %s: NativeLink did not start with the sidecar as REMOTE_READ (%v)", tier, err)
	}
	// Every launch's claim (name.inode): no action directory name twice.
	claims, err := exec.Command("sudo", "-n", "ls", "/run/rbe-action/claims").Output()
	if err != nil {
		t.Fatalf("claims: %v", err)
	}
	seen := map[string]int{}
	var names []string
	for _, c := range strings.Fields(string(claims)) {
		n := c[:strings.LastIndex(c, ".")]
		seen[n]++
		names = append(names, n)
	}
	sort.Strings(names)
	probes := 0
	for n, k := range seen {
		if k > 1 {
			t.Errorf("tier %s: action directory %s launched %d times", tier, n, k)
		}
		if regexp.MustCompile(`^(isolation-probe|hot-serve-probe|hot-reach-probe)`).MatchString(n) {
			probes++
		}
	}
	// Round 1 and round 2: isolation (2 on OSS, 1 on fork), reach (1), serve (OSS 1), twice.
	want := map[string]int{"oss": 8, "fork": 4}[tier]
	if runErr == nil && probes != want {
		t.Errorf("tier %s: %d probe launches, want %d: %v", tier, probes, want, names)
	}
}

// cleanupIsolation undoes what isolate() set up, so the next tier boots
// as on a fresh VM (the reserved port and the userns limit stay: fork last).
func cleanupIsolation(t *testing.T) {
	script := `
set -u
pkill -f "[r]be-hotread" 2>/dev/null
for i in $(seq 0 63); do
	u=$(printf 'rbe-a%02d' "$i")
	id "$u" >/dev/null 2>&1 || continue
	pkill -KILL -u "$u" 2>/dev/null; sleep 0.2
	userdel -f "$u" || echo "userdel $u failed"
	getent group "$u" >/dev/null && groupdel "$u"
done
nft delete table inet rbe_action 2>/dev/null
rm -rf /usr/local/libexec/rbe-action /var/lib/rbe-action /etc/rbe-west/rbe-action.env /etc/sudoers.d/rbe-action /run/rbe-action
true`
	out, err := exec.Command("sudo", "-n", "bash", "-c", script).CombinedOutput()
	if err != nil || len(out) > 0 {
		t.Logf("cleanup: %v\n%s", err, out)
	}
}

func tailString(s string, n int) string {
	if len(s) > n {
		return s[len(s)-n:]
	}
	return s
}
