package scripts_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// repoRoot returns this repo's root: the test's own package lives in
// test/, one level below it, and go test's cwd is always the package
// directory, so the root is simply the parent of the current working
// directory. Unlike gascity's copy of this helper, there is no Bazel
// runfiles tree to prefer here: rbe-worker's own tests always run with
// `go test ./...`, never under Bazel.
func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	return filepath.Dir(wd)
}

func readFile(t *testing.T, root, rel string) string {
	t.Helper()
	content, err := os.ReadFile(filepath.Join(root, rel))
	if err != nil {
		t.Fatalf("read %s: %v", rel, err)
	}
	return string(content)
}

// runWorkflowStepScript runs a workflow step's script as Actions does (bash
// --noprofile --norc -eo pipefail) in dir, with this process's PATH and env
// (env's PATH, if set, replaces it), and returns its combined output.
func runWorkflowStepScript(t *testing.T, dir, script string, env map[string]string) (string, error) {
	t.Helper()
	path := filepath.Join(dir, "step.sh")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("bash", "--noprofile", "--norc", "-eo", "pipefail", path)
	cmd.Dir = dir
	cmd.Env = []string{"PATH=" + os.Getenv("PATH")}
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	out, err := cmd.CombinedOutput()
	return string(out), err
}
