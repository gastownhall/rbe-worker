# Security

## What runs here, and with what

Nothing in this repository's own CI (`.github/workflows/ci.yml`) holds a
secret: it runs on GitHub-hosted `ubuntu-24.04` runners with
`permissions: { contents: read }` and no repository secrets, variables or
environments are configured (see the ruleset and Actions settings this repo
is governed by).

Code merged here reaches secrets only through a **second, separate** step in
a consuming product:

- **Worker code** (`worker/`) runs with the rbe-west farm's worker
  certificate and key only after a gascity pin-bump PR moves
  `.github/actions/rbe-worker/pin` to a commit here, and only inside
  gascity's `rbe-pool` deployment Environment, main-branch only.
- **Client code** (`client/setup-bazel`, `client/ci-analytics`) runs with a
  product's CI remote-execution certificate, or a fork's short-lived minted
  one, only after a client-bump PR moves `pin-client` and the matching
  `uses: gastownhall/rbe-worker/client/*@<sha>` lines in gascity or beads.
  That bump PR itself runs in cache mode, with no credentials present.

Merging a change here is therefore necessary but never sufficient to run it
with a secret: a second, visible, separately reviewable merge in the
consuming product is required first. See the design document's §6 ("Trust
boundary") for the full reasoning and the accepted single-maintainer review
residual (D4).

## Maintainers

`@gastownhall/rbe-worker-maintainers` (currently `julianknutsen` alone, D5).
Every commit to `main` is a signed, squash-merged pull request; branch
protection requires the commit signature and blocks force-push and deletion.

## Reporting a vulnerability

Please do not open a public issue for a security concern. Instead, report it
privately to the maintainers listed in `.github/CODEOWNERS`, through
GitHub's private vulnerability reporting for this repository
(Security tab → "Report a vulnerability"), or to the gascity or beads
maintainers if the issue is in how a product consumes this repo (a pin, a
wrapper, or the trust boundary between them).

Please include:
- the affected path(s) (`worker/...`, `client/.../...`, `cmd/product-check`);
- whether the issue requires a change here, in a consuming product's pin or
  wrapper, or both;
- for a worker or client code issue: whether it is reachable only after a
  pin bump, or sooner (e.g. through `cmd/product-check`'s `product-contract`
  job, which runs informationally against gascity and beads `main`).

We will acknowledge within a reasonable time and coordinate a fix and
disclosure timeline with the affected products' maintainers.
