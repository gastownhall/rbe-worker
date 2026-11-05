#!/usr/bin/env bash
# test/e2e/isolation-in-docker.sh [GOROOT]: TestRBEWorkerIsolationE2E (the
# isolation phases end to end, both tiers, twice in one boot) in a throwaway
# privileged ubuntu:24.04 container, as a non-root runner user with
# passwordless sudo, as on a pool VM. GOROOT: a Go >= go.mod's (default:
# `go env GOROOT`); the module cache is mounted read-only, nothing is fetched.
#
# What the container is not: a Blacksmith VM (no Blacksmith image, tool
# caches or VM shutdown socket; another kernel config). The fork tier runs
# in a child user namespace (below), so its root is not the host's: the
# launcher's namespaces and remounts nest one level deeper than on a VM.
# /home is a loop-mounted ext4, the closest to a VM disk here; it did not
# hand a freed inode back in the runs so far, so the launcher's refusal of a
# reused directory does not fire: the test's distinct-claim check is what
# catches a reused name (TestRBEWorkerIsolationE2E). The OSS tier runs
# first in the container itself, then fork: one boot, as the test
# documents. On a real runner (CI's privileged job) the test runs without
# this wrapper.
set -euo pipefail
here=$(cd "$(dirname "$0")/../.." && pwd -P)
goroot=${1:-$(go env GOROOT)}
modcache=$(go env GOMODCACHE)
docker run --rm --privileged -v "$here:/src:ro" -v "$goroot:/opt/go:ro" -v "$modcache:/opt/gomod:ro" \
	ubuntu:24.04 bash -euo pipefail -c '
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq >/dev/null
apt-get install -y -qq gcc libc6-dev nftables file util-linux procps python3 jq iproute2 sudo uidmap ca-certificates >/dev/null
# /home on ext4, as a runner VM'"'"'s disk is (tmpfs never hands an inode
# number back; ext4 can, though it did not in these runs: the test'"'"'s
# distinct-claim check is the assertion that catches a reused name).
truncate -s 4G /run/home.ext4 && mkfs.ext4 -q /run/home.ext4 && mount -o loop /run/home.ext4 /home
useradd -m -s /bin/bash runner
echo "runner ALL=(ALL) NOPASSWD:ALL" >/etc/sudoers.d/runner && chmod 0440 /etc/sudoers.d/runner
echo root:0:65536 >/etc/subuid && echo root:0:65536 >/etc/subgid
run() { # tier: the test as the runner user
	su runner -c "cd /src && env PATH=/opt/go/bin:/usr/bin:/bin GOCACHE=/tmp/gocache-runner GOMODCACHE=/opt/gomod \
		GOFLAGS=-mod=readonly GOPROXY=off GOTOOLCHAIN=local RBE_E2E_ISOLATION=1 RBE_E2E_TIERS=$1 \
		go test -tags integration -count=1 -v -run TestRBEWorkerIsolationE2E ./test/"
}
# OSS: the container as it is (the init user namespace, its own netns).
run oss
# fork: user.max_user_namespaces=0 must not reach the host (a privileged
# container shares the init user namespace), so the fork tier runs in a
# child user namespace mapping uids and gids 0-65535 to themselves, with its
# own network and mount namespaces: the limit isolate() sets is real there,
# and binds that namespace alone.
export -f run
unshare --user --map-users=0:0:65536 --map-groups=0:0:65536 --net --mount --pid --fork --mount-proc \
	bash -euo pipefail -c "ip link set lo up; run fork"
'
