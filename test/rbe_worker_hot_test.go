package scripts_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Hot blobs on R2 (infra README "Hot blobs on R2"): rbe-hotread fronts
// REMOTE_READ only when everything says so, and every failure to build,
// start or confine it leaves today's worker.json.

func scriptSection(t *testing.T, script, from, to string) string {
	t.Helper()
	i := strings.Index(script, from)
	j := strings.Index(script[max(i, 0):], to)
	if i < 0 || j < 0 {
		t.Fatalf("%s: no section %q .. %q", rbeWorkerScript, from, to)
	}
	return script[i : i+j]
}

func TestRBEWorkerHotGate(t *testing.T) {
	section := scriptSection(t, readFile(t, repoRoot(t), rbeWorkerScript), "# Hot tier (RBE_HOT_URL): decided here", "\nRBE_WEST_PORT=")
	for _, c := range []struct {
		name, url, tier, mode, zstd, every, run, min string
		on                                           bool
	}{
		{"oss on", "https://rbe-warm.ops.gascity.com/h/oss", "oss", "pool", "true", "", "7", "", true},
		{"fork on", "https://rbe-warm.ops.gascity.com/h/fork", "fork", "pool", "true", "", "7", "", true},
		{"unset", "", "oss", "pool", "true", "", "7", "", false},
		{"run mode", "https://rbe-warm.ops.gascity.com/h/oss", "oss", "run", "true", "", "7", "", false},
		{"no wire zstd", "https://rbe-warm.ops.gascity.com/h/oss", "oss", "pool", "false", "", "7", "", false},
		// An OSS VM never reads what fork VMs had published, nor the reverse.
		{"oss reads fork prefix", "https://rbe-warm.ops.gascity.com/h/fork", "oss", "pool", "true", "", "7", "", false},
		{"fork reads oss prefix", "https://rbe-warm.ops.gascity.com/h/oss", "fork", "pool", "true", "", "7", "", false},
		{"http", "http://rbe-warm.ops.gascity.com/h/oss", "oss", "pool", "true", "", "7", "", false},
		{"other prefix", "https://rbe-warm.ops.gascity.com/chunks", "oss", "pool", "true", "", "7", "", false},
		{"suffix", "https://rbe-warm.ops.gascity.com/h/oss/x", "oss", "pool", "true", "", "7", "", false},
		{"canary hit", "https://rbe-warm.ops.gascity.com/h/oss", "oss", "pool", "true", "2", "36861390718", "", true},
		{"canary miss", "https://rbe-warm.ops.gascity.com/h/oss", "oss", "pool", "true", "2", "36861390719", "", false},
		{"bad every", "https://rbe-warm.ops.gascity.com/h/oss", "oss", "pool", "true", "0", "7", "", false},
		{"min 4 MiB", "https://rbe-warm.ops.gascity.com/h/oss", "oss", "pool", "true", "", "7", "4194304", true},
		{"min too small", "https://rbe-warm.ops.gascity.com/h/oss", "oss", "pool", "true", "", "7", "65536", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			prog := "set -euo pipefail\n" + section + "\necho \"hot=$hot\"\n"
			cmd := exec.Command("bash", "-c", prog)
			cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "RBE_HOT_URL=" + c.url, "WORKER_TIER=" + c.tier,
				"WORKER_MODE=" + c.mode, "wire_zstd=" + c.zstd, "GITHUB_RUN_ID=" + c.run}
			if c.every != "" {
				cmd.Env = append(cmd.Env, "RBE_HOT_EVERY="+c.every)
			}
			if c.min != "" {
				cmd.Env = append(cmd.Env, "RBE_HOT_MIN_BYTES="+c.min)
			}
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("bash: %v\n%s", err, out)
			}
			if got := strings.Contains(string(out), "hot=1\n"); got != c.on {
				t.Errorf("hot = %v, want %v\n%s", got, c.on, out)
			}
		})
	}
}

// With the sidecar, REMOTE_READ is rbe-hotread on loopback (h2c, no TLS:
// the sidecar holds the certificate) and nothing else in worker.json moves.
func TestRBEWorkerJSONHotRead(t *testing.T) {
	root := repoRoot(t)
	prog, iso := workerJSONProgram(t, root)
	render := func(tier, host, isolation string, extra ...string) map[string]any {
		args := []string{"-n", "--arg", "host", host, "--arg", "root", "/r", "--arg", "store", "/r", "--arg", "name", "w",
			"--argjson", "slots", "8", "--arg", "tier", tier, "--arg", "worker_env", rbeWorkerEnvSample,
			"--argjson", "zstd", "true", "--arg", "read", "grpcs://rbe-zread.example.invalid:443"}
		args = append(append(args, extra...), "--argjson", "isolation", isolation, prog)
		out, err := exec.Command("jq", args...).Output()
		if err != nil {
			t.Fatalf("jq: %v", err)
		}
		var v map[string]any
		if err := json.Unmarshal(out, &v); err != nil {
			t.Fatal(err)
		}
		return v
	}
	for _, c := range []struct{ tier, host, isolation string }{
		{"oss", "grpcs://rbe-west.example.invalid:443", "{}"},
		{"fork", "grpcs://rbe-fork.example.invalid:8444", iso},
	} {
		plain := render(c.tier, c.host, c.isolation)
		empty := render(c.tier, c.host, c.isolation, "--arg", "hotread", "")
		if !reflect.DeepEqual(plain, empty) {
			t.Errorf("tier %s: an empty hotread changes worker.json", c.tier)
		}
		hot := render(c.tier, c.host, c.isolation, "--arg", "hotread", "grpc://127.0.0.1:50090")
		stores := func(v map[string]any) []any { return v["stores"].([]any) }
		var sawRead bool
		for i, s := range stores(hot) {
			st, want := s.(map[string]any), stores(plain)[i].(map[string]any)
			if st["name"] != "REMOTE_READ" {
				if !reflect.DeepEqual(st, want) {
					t.Errorf("tier %s: store %v changed with hotread", c.tier, st["name"])
				}
				continue
			}
			sawRead = true
			g := st["grpc"].(map[string]any)
			eps := g["endpoints"].([]any)
			if len(eps) != 1 || !reflect.DeepEqual(eps[0], map[string]any{"address": "grpc://127.0.0.1:50090"}) {
				t.Errorf("tier %s: REMOTE_READ endpoints %v, want the sidecar alone, no tls_config", c.tier, eps)
			}
			if g["experimental_remote_cache_compression"] != true {
				t.Errorf("tier %s: REMOTE_READ must still ask for zstd", c.tier)
			}
			wg := want["grpc"].(map[string]any)
			delete(g, "endpoints")
			delete(wg, "endpoints")
			if !reflect.DeepEqual(g, wg) {
				t.Errorf("tier %s: REMOTE_READ differs beyond its endpoint: %v vs %v", c.tier, g, wg)
			}
		}
		if !sawRead {
			t.Errorf("tier %s: no REMOTE_READ", c.tier)
		}
		hot["stores"], plain["stores"] = nil, nil
		if !reflect.DeepEqual(hot, plain) {
			t.Errorf("tier %s: workers or servers changed with hotread", c.tier)
		}
	}
}

// The start: from the isolation section to NativeLink's start, with a stub
// sidecar. Up: REMOTE_READ is the sidecar. A build or start failure: the
// worker.json without it, and the worker starts anyway.
func TestRBEWorkerHotStart(t *testing.T) {
	if l, err := net.Listen("tcp", "127.0.0.1:50090"); err != nil {
		t.Skipf("127.0.0.1:50090 is taken here: %v", err)
	} else {
		l.Close()
	}
	root := repoRoot(t)
	section := scriptSection(t, readFile(t, root, rbeWorkerScript), "\nLIB=/usr/local/libexec/rbe-action\n", "\nnl=$!\n") + "\nnl=$!\n"
	listener := "#!/usr/bin/env python3\nimport socket, sys\nopen(sys.argv[0] + '.args', 'w').write('\\n'.join(sys.argv[1:]))\n" +
		"s = socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1); s.bind(('127.0.0.1', 50090)); s.listen(8)\n" +
		"while True:\n    c, _ = s.accept(); c.close()\n"
	for _, c := range []struct {
		name, stub string
		buildRC    int
		on         bool
		reason     string
	}{
		{"up", listener, 0, true, ""},
		{"start fails", "#!/bin/sh\nexit 1\n", 0, false, "reason=start"},
		{"build fails", listener, 1, false, "reason=build"},
	} {
		t.Run(c.name, func(t *testing.T) {
			home := t.TempDir()
			temp, nlRoot, bin := filepath.Join(home, "temp"), filepath.Join(home, "nl-worker"), filepath.Join(home, "nl-bin")
			for _, d := range []string{temp, bin, filepath.Join(nlRoot, "work"), filepath.Join(nlRoot, "pki")} {
				if err := os.MkdirAll(d, 0o755); err != nil {
					t.Fatal(err)
				}
			}
			os.WriteFile(filepath.Join(bin, "nativelink"), []byte("#!/bin/sh\ncp \"$1\" \"$1.started\"\n"), 0o755)
			os.WriteFile(filepath.Join(temp, "rbe-hotread"), []byte(c.stub), 0o755)
			prog := "set -euo pipefail\nACTION_ISOLATION=0 slots=8 STORE=$ROOT wire_zstd=true\n" +
				"(exit $BUILD_RC) & hot_build_pid=$!\n" + section +
				"wait \"$nl\"\n[ -z \"${hot_pid:-}\" ] || kill \"$hot_pid\"\n"
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, "bash", "-c", prog)
			cmd.Env = []string{
				"PATH=" + bin + string(os.PathListSeparator) + os.Getenv("PATH"), "HOME=" + home, "RUNNER_TEMP=" + temp,
				"ROOT=" + nlRoot, "NL_BIN_DIR=" + bin, "RBE_WEST_HOST=rbe-west.example.invalid", "WORKER_NAME=w",
				"WORKER_ENV=" + rbeWorkerEnvSample, "ZSTD_READ_URL=grpcs://rbe-zread.example.invalid:443",
				"RBE_HOT_URL=https://rbe-warm.example.invalid/h/oss", "BUILD_RC=" + string(rune('0'+c.buildRC)),
			}
			out, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if err != nil && !errors.As(err, &exit) {
				t.Fatalf("bash: %v\n%s", err, out)
			}
			if err != nil {
				t.Fatalf("the worker must start whatever the sidecar does: %v\n%s", err, out)
			}
			started, err := os.ReadFile(filepath.Join(nlRoot, "worker.json.started"))
			if err != nil {
				t.Fatalf("NativeLink not started:\n%s", out)
			}
			var cfg struct {
				Stores []struct {
					Name string `json:"name"`
					GRPC *struct {
						Endpoints []map[string]any `json:"endpoints"`
					} `json:"grpc"`
				} `json:"stores"`
			}
			if err := json.Unmarshal(started, &cfg); err != nil {
				t.Fatal(err)
			}
			read := ""
			for _, s := range cfg.Stores {
				if s.Name == "REMOTE_READ" && s.GRPC != nil && len(s.GRPC.Endpoints) == 1 {
					read, _ = s.GRPC.Endpoints[0]["address"].(string)
				}
			}
			want := "grpcs://rbe-zread.example.invalid:443"
			if c.on {
				want = "grpc://127.0.0.1:50090"
				if !strings.Contains(string(out), "rbe-hot: status=on ") {
					t.Errorf("no status=on line:\n%s", out)
				}
				args, _ := os.ReadFile(filepath.Join(temp, "rbe-hotread.args"))
				for _, a := range []string{"-listen\n127.0.0.1:50090\n", "-upstream\nrbe-zread.example.invalid:443\n",
					"-hot\nhttps://rbe-warm.example.invalid/h/oss\n",
					"-promote\nhttps://rbe-zread.example.invalid:443/rbe.hot.v1.Hot/Promote\n", "-min\n67108864\n"} {
					if !bytes.Contains(args, []byte(a)) {
						t.Errorf("sidecar args missing %q:\n%s", a, args)
					}
				}
			} else if !strings.Contains(string(out), "rbe-hot: status=off "+c.reason) {
				t.Errorf("want rbe-hot: status=off %s:\n%s", c.reason, out)
			}
			if read != want {
				t.Errorf("REMOTE_READ %q, want %q\n%s", read, want, out)
			}
		})
	}
}

// The order the script runs it in: built after Go is installed, started
// after the warm fetch and before NativeLink, stopped after NativeLink (its
// report line last).
func TestRBEWorkerHotOrder(t *testing.T) {
	script := readFile(t, repoRoot(t), rbeWorkerScript)
	order := []string{
		"# Hot tier (RBE_HOT_URL): decided here",
		`if ! /usr/local/go/bin/go version`,
		"timeout 240 go build -trimpath -o \"$RUNNER_TEMP/rbe-hotread\" ./cmd/rbe-hotread",
		"meta skuid 59000-59063 oif lo tcp dport 50090 ct direction original reject with tcp reset",
		`grep -m1 '^rbe-warm: ' "$RUNNER_TEMP/rbe-warm.out"`,
		// A slot listener on :50090 is probed before the sidecar takes it.
		`s.bind(("127.0.0.1", 50090))`,
		`"$RUNNER_TEMP/rbe-hotread" -listen 127.0.0.1:50090`,
		`HOT_READ_URL=grpc://127.0.0.1:50090`,
		`"$NL_BIN_DIR/nativelink" "$ROOT/worker.json"`,
		// The pool loop retires when the sidecar is gone (never restarts it).
		`echo "rbe-hot: sidecar exited, retiring"; break`,
		`kill -TERM "$nl"`,
		`grep -m1 '^rbe-hot: ' "$RUNNER_TEMP/rbe-hot.out"`,
	}
	at := 0
	for _, want := range order {
		i := strings.Index(script[at:], want)
		if i < 0 {
			t.Fatalf("%s: %q missing or out of order", rbeWorkerScript, want)
		}
		at += i + len(want)
	}
}

// Port 50090 is inside ip_local_port_range: isolate() reserves it (no
// ephemeral connect is ever given it) before the slot rules load, and the
// rules match the conntrack direction, so a slot's own connection on it
// passes. reserves_50090 reads the kernel's list of ports and ranges.
func TestRBEWorkerReserves50090(t *testing.T) {
	script := readFile(t, repoRoot(t), rbeWorkerScript)
	fn := scriptSection(t, script, "reserves_50090() {", "\n}\n") + "\n}\n"
	for list, want := range map[string]bool{"": false, "50090": true, "22,50090": true, "50000-50100": true,
		"50091": false, "1-5,50080-50089": false, "50090-50090": true} {
		dir := t.TempDir()
		f := filepath.Join(dir, "reserved")
		os.WriteFile(f, []byte(list+"\n"), 0o644)
		prog := strings.ReplaceAll(fn, "/proc/sys/net/ipv4/ip_local_reserved_ports", f) + "reserves_50090\n"
		err := exec.Command("bash", "-c", prog).Run()
		if got := err == nil; got != want {
			t.Errorf("reserves_50090 with %q = %v, want %v", list, got, want)
		}
	}
	nft := scriptSection(t, script, "\tphase nft\n", "\tphase render\n")
	for _, want := range []string{
		`sudo sysctl -q -w net.ipv4.ip_local_reserved_ports="${reserved:+$reserved,}50090"`,
		`reserves_50090 || fail "net.ipv4.ip_local_reserved_ports does not hold 50090"`,
	} {
		if !strings.Contains(nft, want) {
			t.Errorf("phase nft missing %q", want)
		}
	}
	if strings.Index(nft, "reserves_50090 || fail") > strings.Index(nft, "sudo nft -f -") {
		t.Error("the port must be reserved before the slot rules load")
	}
	// The isolation probe also checks a slot's loopback connect from
	// :50090 to another server gets through (OSS tier).
	probe := scriptSection(t, script, "\tphase probe\n", "\n}\n")
	// Its server takes a free port (bind 0): never a fixed or random pick.
	if !strings.Contains(probe, `s.bind(("127.0.0.1", 0))`) || strings.Contains(probe, "RANDOM") {
		t.Error("the loopback probe's server must bind port 0")
	}
	for _, want := range []string{`c.bind(("127.0.0.1", 50090)); c.settimeout(3); c.connect(`,
		`[ "$lo" = OK ] || fail "a slot's loopback connect from :50090 did not get through ($lo)"`} {
		if !strings.Contains(probe, want) {
			t.Errorf("phase probe missing %q", want)
		}
	}
}
