# Provenance

`gascity-commit-map.txt` and `beads-commit-map.txt` are the two
`git filter-repo` commit maps produced by the S2 history import (§9 "S2.
Create rbe-worker"): one line per original commit in each source repo,
`<old sha> <new sha>`, where `new` is `0000000000000000000000000000000000000000`
for a commit that touched none of the paths this repo kept (pruned), and the
rewritten commit's SHA otherwise.

- `gascity-commit-map.txt`: from `gastownhall/gascity`, filtered to the 7
  `tools/rbe` worker files, the 4 `scripts/rbe_*_test.go` files and their
  goldens, and the CI-analytics extractor and its testdata, with the renames
  in §9 S2 step 1 and `--replace-message` rewriting `(#N)` to
  `(gastownhall/gascity#N)`.
- `beads-commit-map.txt`: from `gastownhall/beads`, filtered to
  `.github/actions/setup-bazel/{action.yml,fork-credential.sh,write-bazelrc.sh}`,
  renamed to `client/setup-bazel/`, with `--replace-message` rewriting `(#N)`
  to `(gastownhall/beads#N)`.

Use these to find where a line in `worker/` or `client/setup-bazel/` came
from: look up its blame SHA in the relevant map's `new` column to get the
original commit in gascity or beads.
