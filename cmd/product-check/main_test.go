package main

import "testing"

// This repo's own root doubles as a self-test fixture (tools/rbe/worker-env.txt,
// platforms/BUILD.bazel, .bazelrc, go.mod paired with worker/worker-env and
// worker/blacksmith-worker.sh): product-check must pass against it with no
// product checkout of gascity or beads around.
func TestWorkerItemsPassAgainstTheOwnFixture(t *testing.T) {
	if errs := workerItems("../..", "../.."); len(errs) != 0 {
		t.Errorf("workerItems: %v", errs)
	}
}

func TestClientItemsRequireCompositeNoHooks(t *testing.T) {
	if errs := clientItems("../..", "../.."); len(errs) != 0 {
		t.Errorf("clientItems: %v", errs)
	}
}
