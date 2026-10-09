package scripts_test

import (
	"regexp"
	"strings"
	"testing"
)

// rbe-west's fork pool (infra nativelink-cas/west README "rbe-fork"): Blacksmith
// workers for the fork scheduler, instance oss-fork, which executes untrusted
// fork and Dependabot PR actions. rbe-fork-pool.yml runs blacksmith-worker.sh
// with WORKER_TIER=fork: isolation always on, every action in its own network
// namespace with loopback only (NETNS=1), no action cache, and the fork CA's
// worker certificate (CN=rbe-fork-worker), which reaches only the fork
// scheduler and the CAS.

// The tier gate sits right after RBE_ACTION_ISOLATION is validated and before
// the canary picks a mode, so a fork worker refuses 0 and canary alike, and
// before anything is installed. NETNS is set here and nowhere else.
func TestRBEWorkerScriptForkTier(t *testing.T) {
	script := readFile(t, repoRoot(t), rbeWorkerScript)
	const gate = "WORKER_TIER=${WORKER_TIER:-oss}\n" +
		"case \"$WORKER_TIER\" in\n" +
		"oss) NETNS=0 ;;\n" +
		"fork)\n" +
		"\t# Untrusted actions never run unisolated, and never with a network.\n" +
		"\t[ \"$ACTION_ISOLATION\" = 1 ] || { echo \"WORKER_TIER=fork needs RBE_ACTION_ISOLATION=1\" >&2; exit 2; }\n" +
		"\tNETNS=1\n" +
		"\t;;\n" +
		"*) echo \"WORKER_TIER must be oss or fork\" >&2; exit 2 ;;\n" +
		"esac\n"
	at := 0
	for _, want := range []string{
		"ACTION_ISOLATION=${RBE_ACTION_ISOLATION:-1}\n",
		`*) echo "RBE_ACTION_ISOLATION must be 0, 1 or canary" >&2; exit 2 ;;`,
		gate,
		"RBE_WEST_PORT=${RBE_WEST_PORT:-443}\n",
		// zstd needs the dedicated zread host: the worker's own endpoint
		// answers compressed reads with InvalidArgument (no fallback), so the
		// worker exits before installing anything rather than register.
		// Indented: measure mode (no farm host) skips the farm checks.
		`if [ "$wire_zstd" = true ] && [ "$ZSTD_READ_URL" = "grpcs://${RBE_WEST_HOST}:${RBE_WEST_PORT}" ]; then`,
		"\t\texit 2\n\tfi\nfi\n",
		"sudo DEBIAN_FRONTEND=noninteractive",
		`if [ "$ACTION_ISOLATION" = canary ]; then`,
		"\t\tNETNS=${NETNS:-0}\n",
	} {
		i := strings.Index(script[at:], want)
		if i < 0 {
			t.Fatalf("%s: %q missing or out of order", rbeWorkerScript, want)
		}
		at += i + len(want)
	}
	if n := len(regexp.MustCompile(`(?m)^\s*(?:oss\) )?NETNS=`).FindAllString(script, -1)); n != 3 {
		t.Errorf("%s assigns NETNS %d times, want 3 (oss, fork, rbe-action.env)", rbeWorkerScript, n)
	}
	// render runs without the preamble too (the canary harness): it defaults
	// the tier and port itself, to the OSS ones.
	for _, want := range []string{
		`--arg host "grpcs://${RBE_WEST_HOST}:${RBE_WEST_PORT:-443}"`,
		`--arg tier "${WORKER_TIER:-oss}"`,
	} {
		if !strings.Contains(script, want) {
			t.Errorf("%s render missing %q", rbeWorkerScript, want)
		}
	}
}

// A fork VM runs untrusted actions: user namespaces (the usual first step of a
// kernel escape from an unprivileged process) are off on it before any action
// runs, set inside isolate() after the render and before the selftest, so the
// selftest and the probe run with the limit in place, and the probe action
// checks it cannot make one. Nothing on the worker needs them: the launcher's
// unshare runs as root with pid/mount/ipc/net namespaces only.
func TestRBEWorkerScriptForkNoUserNamespaces(t *testing.T) {
	root := repoRoot(t)
	script := readFile(t, root, rbeWorkerScript)
	at := 0
	for _, want := range []string{
		"isolate() {\n",
		"\tphase render\n\trender\n",
		"\tif [ \"$WORKER_TIER\" = fork ]; then\n\t\tphase userns\n" +
			"\t\tsudo sysctl -q -w user.max_user_namespaces=0\n" +
			"\t\t[ \"$(cat /proc/sys/user/max_user_namespaces)\" = 0 ] || fail \"user.max_user_namespaces is not 0\"\n\tfi\n",
		"\tphase selftest\n",
		"\t\t[ \"$2\" = fork ] && unshare --user true >/dev/null 2>&1 && echo \"LEAK userns\"\n",
		"' probe \"$ROOT/pki/worker.key\" \"$WORKER_TIER\" 2>&1) || true\n",
	} {
		i := strings.Index(script[at:], want)
		if i < 0 {
			t.Fatalf("%s: %q missing or out of order", rbeWorkerScript, want)
		}
		at += i + len(want)
	}
	for _, f := range []string{"worker/rbe-action-launch", "worker/rbe-action-entry.c", "worker/rbe-action-selftest", "worker/rbe-action-sweep"} {
		if regexp.MustCompile(`unshare[^\n]*(--user|\s-U\b)|CLONE_NEWUSER`).MatchString(readFile(t, root, f)) {
			t.Errorf("%s creates a user namespace; fork VMs run with user.max_user_namespaces=0", f)
		}
	}
}
