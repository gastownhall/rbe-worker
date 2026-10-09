// Command product-check verifies the contract between rbe-worker and a
// product checkout (gascity or beads): §5.6 of the design. It is run two
// ways:
//
//   - gated: gascity's bazel.yml worker-host job runs the worker items
//     against the PR's own checkout (product-root ".") whenever a worker
//     bump or re-pin changed anything (§5.5); both products run the client
//     items on a client bump.
//   - informational: rbe-worker's own ci.yml product-contract job fetches
//     gascity and beads main anonymously and runs every item against each,
//     nightly and on every PR, so a drift between this repo and either
//     product's contract shows up here first.
//
// worker-root is this repo's own checkout (the worker/ side of the
// contract); product-root is the product's (the manifest, go.mod, BUILD
// pin and .bazelrc test env it is pinning). They default to "." and
// differ only when a caller passes a product checkout fetched elsewhere.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

func main() {
	var workerRoot, productRoot string
	var skipClient, skipWorker bool
	flag.StringVar(&workerRoot, "worker-root", ".", "rbe-worker checkout to read worker/ from")
	flag.StringVar(&productRoot, "product-root", ".", "product checkout to check (gascity or beads)")
	flag.BoolVar(&skipClient, "skip-client", false, "skip the client items (§5.6): a product-root with no client/ caller")
	flag.BoolVar(&skipWorker, "skip-worker", false, "skip the worker items (§5.6): a product-root that does not run the rbe-west farm (e.g. beads)")
	flag.Parse()

	var errs []error
	if !skipWorker {
		errs = append(errs, workerItems(workerRoot, productRoot)...)
	}
	if !skipClient {
		errs = append(errs, clientItems(workerRoot, productRoot)...)
	}
	if len(errs) == 0 {
		fmt.Println("product-check: ok")
		return
	}
	for _, err := range errs {
		fmt.Fprintln(os.Stderr, "product-check:", err)
	}
	os.Exit(1)
}

// --- worker items (§5.6 "Worker items") ---

func workerItems(workerRoot, productRoot string) []error {
	var errs []error

	manifestPath := filepath.Join(productRoot, "tools", "rbe", "worker-env.txt")
	manifest, err := os.ReadFile(manifestPath)
	if err != nil {
		return []error{fmt.Errorf("read %s: %w", manifestPath, err)}
	}

	// the manifest's package set equals `measured` (worker/worker-env's own
	// list of NAME:PARTS entries).
	measured, err := measuredPackageNames(workerRoot)
	if err != nil {
		errs = append(errs, err)
	} else if pkgs, err := manifestPackageNames(manifest); err != nil {
		errs = append(errs, err)
	} else if !sameSet(pkgs, measured) {
		errs = append(errs, fmt.Errorf("%s pkg set %v, want worker-env's measured set %v", manifestPath, pkgs, measured))
	}

	// dolt equals DOLT_VERSION (worker/blacksmith-worker.sh's own pin).
	doltVersion, err := doltVersionPin(workerRoot)
	if err != nil {
		errs = append(errs, err)
	} else if got := manifestField(manifest, "dolt"); !strings.Contains(got, doltVersion) {
		errs = append(errs, fmt.Errorf("%s dolt line %q does not contain blacksmith-worker.sh's DOLT_VERSION=%s", manifestPath, got, doltVersion))
	}

	// go equals the product's go.mod.
	goModPath := filepath.Join(productRoot, "go.mod")
	goVersion, err := goModGoVersion(goModPath)
	if err != nil {
		errs = append(errs, err)
	} else if got := manifestField(manifest, "go"); !strings.Contains(got, goVersion) {
		errs = append(errs, fmt.Errorf("%s go line %q does not contain %s's go %s", manifestPath, got, goModPath, goVersion))
	}

	// .bazelrc test PATH equals worker-env's.
	bazelrcPath := filepath.Join(productRoot, ".bazelrc")
	if testPath, err := bazelrcTestPath(bazelrcPath); err != nil {
		errs = append(errs, err)
	} else if envPath, err := workerEnvDefaultPath(workerRoot); err != nil {
		errs = append(errs, err)
	} else if testPath != envPath {
		errs = append(errs, fmt.Errorf("%s test --test_env=PATH=%s, want worker-env's PATH %s", bazelrcPath, testPath, envPath))
	}

	// the BUILD pin equals sha256 of the manifest.
	buildPath := filepath.Join(productRoot, "platforms", "BUILD.bazel")
	if err := checkBuildPin(buildPath, manifest); err != nil {
		errs = append(errs, err)
	}

	return errs
}

var measuredLineRE = regexp.MustCompile(`([A-Za-z0-9.+-]+):[0-9]+`)

// measuredPackageNames parses worker/worker-env's `measured=( ... )` array
// for the package names it lists (ignoring the :PARTS suffix), without
// executing the script.
func measuredPackageNames(workerRoot string) ([]string, error) {
	path := filepath.Join(workerRoot, "worker", "worker-env")
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	m := regexp.MustCompile(`(?s)measured=\((.*?)\n\)`).FindStringSubmatch(string(content))
	if m == nil {
		return nil, fmt.Errorf("%s: no measured=( ... ) array", path)
	}
	var names []string
	for _, line := range measuredLineRE.FindAllStringSubmatch(m[1], -1) {
		names = append(names, line[1])
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("%s: measured=( ... ) array named no packages", path)
	}
	sort.Strings(names)
	return names, nil
}

func manifestPackageNames(manifest []byte) ([]string, error) {
	var names []string
	for _, line := range strings.Split(string(manifest), "\n") {
		if name, ok := strings.CutPrefix(line, "pkg "); ok {
			if n, _, ok := strings.Cut(name, " "); ok {
				names = append(names, n)
			}
		}
	}
	if len(names) == 0 {
		return nil, errors.New("manifest named no pkg lines")
	}
	sort.Strings(names)
	return names, nil
}

func sameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func manifestField(manifest []byte, name string) string {
	for _, line := range strings.Split(string(manifest), "\n") {
		if v, ok := strings.CutPrefix(line, name+" "); ok {
			return v
		}
	}
	return ""
}

func doltVersionPin(workerRoot string) (string, error) {
	path := filepath.Join(workerRoot, "worker", "blacksmith-worker.sh")
	content, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", path, err)
	}
	m := regexp.MustCompile(`(?m)^DOLT_VERSION=(\S+)`).FindStringSubmatch(string(content))
	if m == nil {
		return "", fmt.Errorf("%s: no DOLT_VERSION= assignment", path)
	}
	return m[1], nil
}

func goModGoVersion(path string) (string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", path, err)
	}
	m := regexp.MustCompile(`(?m)^go (\S+)`).FindStringSubmatch(string(content))
	if m == nil {
		return "", fmt.Errorf("%s: no go directive", path)
	}
	return m[1], nil
}

func bazelrcTestPath(path string) (string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", path, err)
	}
	m := regexp.MustCompile(`(?m)^test --test_env=PATH=(\S+)`).FindStringSubmatch(string(content))
	if m == nil {
		return "", fmt.Errorf("%s: no test --test_env=PATH= line", path)
	}
	return m[1], nil
}

func workerEnvDefaultPath(workerRoot string) (string, error) {
	path := filepath.Join(workerRoot, "worker", "worker-env")
	content, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", path, err)
	}
	m := regexp.MustCompile(`PATH=\$\{WORKER_ENV_PATH:-([^}]+)\}`).FindStringSubmatch(string(content))
	if m == nil {
		return "", fmt.Errorf("%s: no PATH=${WORKER_ENV_PATH:-...} default", path)
	}
	return m[1], nil
}

func checkBuildPin(buildPath string, manifest []byte) error {
	content, err := os.ReadFile(buildPath)
	if err != nil {
		return fmt.Errorf("read %s: %w", buildPath, err)
	}
	m := regexp.MustCompile(`"worker-env":\s*"sha256:([0-9a-f]{64})"`).FindStringSubmatch(string(content))
	if m == nil {
		return fmt.Errorf(`%s: no "worker-env": "sha256:<64 hex>" exec_properties entry`, buildPath)
	}
	sum := sha256.Sum256(manifest)
	want := hex.EncodeToString(sum[:])
	if m[1] != want {
		return fmt.Errorf("%s worker-env pin %s, want sha256 of the manifest %s", buildPath, m[1], want)
	}
	return nil
}

// --- client items (§5.6 "Client items") ---
//
// Only the composite-only, no-pre/post check runs today: the transport-only
// rc-line parity, Bazelisk sha256 pin and extractor redaction checks need
// client/setup-bazel's S10 reconcile (still beads' unreconciled copy as of
// S2) and are left for that slice.

func clientItems(_, productRoot string) []error {
	var errs []error
	matches, err := filepath.Glob(filepath.Join(productRoot, "client", "*", "action.yml"))
	if err != nil {
		return []error{err}
	}
	for _, path := range matches {
		content, err := os.ReadFile(path)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if !regexp.MustCompile(`(?m)^\s*using:\s*composite\s*$`).Match(content) {
			errs = append(errs, fmt.Errorf("%s: not `using: composite`", path))
		}
		if regexp.MustCompile(`(?m)^\s*(pre|post):`).Match(content) {
			errs = append(errs, fmt.Errorf("%s: has a pre or post hook; client actions run composite steps only", path))
		}
	}
	return errs
}
