# rbe-worker

The rbe-west remote-execution farm's worker code and shared client tooling,
split out of [gastownhall/gascity](https://github.com/gastownhall/gascity)
and [gastownhall/beads](https://github.com/gastownhall/beads). Products
consume this repo by commit SHA, never by `main` at run time: see
[`.github/actions/rbe-worker/`](.github/actions/rbe-worker) (worker code) and
`client/*/action.yml` (shared client actions).

This README covers only the contract a caller needs. Provenance of the
imported history (gascity and beads commit maps) is in `docs/provenance/`.

## Layout

```
worker/                   # the rbe-west farm half: run by gascity's pool and canary workflows
client/
  setup-bazel/            # shared Bazel CI setup, consumed by both products' setup-bazel wrappers
  ci-analytics/           # the CI-analytics extractor
cmd/product-check/        # cross-repo contract checks (below)
bin/bump-summary          # a worker-pin bump PR's step-summary body
test/                     # go test ./... : worker, client and product-fixture tests
docs/provenance/          # the gascity and beads commit maps this history was filtered from
```

## Worker contract

- **Run** `worker/blacksmith-worker.sh` with the working directory, or
  `RBE_PRODUCT_ROOT`, set to the product checkout.
- **Product files read, and nothing else:** `go.mod`, `platforms/BUILD.bazel`
  (an exec property `"worker-env": "sha256:<64 hex>",`) and
  `tools/rbe/worker-env.txt`.
- **`WORKER_MODE`:** `pool` or `measure` today (`selftest` is a later
  hardening slice; `run` was removed).
- **Log lines** other systems read: `isolation: <phase>`,
  `worker <name> started (pid …`, `RBE_ISOLATION_CANARY=`, `worker-env: `,
  `rbe-pull: `, `rbe-first-exec: `, and with a warm set `rbe-warm: ` and
  `rbe-warm-audit: `.
- **Warm set (optional, pool mode):** `RBE_WARM_URL` (an `https://` base
  serving `current.json` and `chunks/`), `RBE_WARM_EVERY`,
  `RBE_WARM_SECONDS`, `RBE_WARM_GRACE`. `worker/warm-cas` fetches it before
  NativeLink starts and re-hashes every blob. Unset means a cold CAS; an
  unusable value warns and starts cold, never exit 2.
- **Exit codes:** 2 for configuration, 3 for drift in measure mode.

## Client contract

- Every `client/*/action.yml` is `using: composite` with **no `pre`/`post`
  hooks**, so nothing runs before the product's verify step
  (`cmd/product-check` and an rbe-worker test both enforce this).
- Inputs and outputs are versioned here, in each action's own `action.yml`.

## Drift-issue protocol v1

Label `rbe-worker-env-drift`, title `rbe worker-env drift: <pin>`. Three
consumers each pin these two literals: rbe-worker's own `worker-env-drift
report`/`resolve`, gascity's client `preflight`, and infra's
`oss-pool-scaler.py`. Do not change either literal without updating all
three.

## Bumping a pin

**Worker pin (gascity `.github/actions/rbe-worker/pin`):** a one-line commit
SHA edit, `chore(rbe): bump rbe-worker to <sha8> (<subject>)`. The PR body is
`bin/bump-summary OLD NEW`'s output: the compare URL, the commit list, and
whether the bump touches the measurement, isolation or microVM files, which
call for a closer look before merging. If the host measurement moved, the
same PR re-pins `tools/rbe/worker-env.txt` and `platforms/BUILD.bazel`, and a
beads re-pin PR follows right after (beads carries a byte-identical copy of
the manifest and the same pin). `bazel.yml`'s `worker-host` job then runs
`cmd/product-check`'s worker items, the signature range check, and measures
the PR's manifest on a live host.

**Client pin (`pin-client`, both products):** edit `pin-client` and the
matching `uses: gastownhall/rbe-worker/client/*@<sha>` lines; a test in each
product keeps them equal. A client bump runs in mode `cache` (no
credentials) until it merges.

Both pins are bumped manually (Dependabot and Renovate are configured to
ignore this repo in both products): see the design's D3.

## Review

Changes here merge with **0 required approvals** (`rbe-worker-maintainers` =
`julianknutsen`, signed squash commits, required CI); see the design's §6 for
the compensating controls (visibility, signed and verified commits, the
`worker-host`/`worker-isolation` gates, the independent isolation probe) and
the accepted residual (§6.5, D4, R26).

## Merging

Changes land by squash-merged pull request only; `main` requires signed commits and green CI (see the repository ruleset).
