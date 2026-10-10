#!/usr/bin/env bash
# Run a NativeLink remote-execution worker on a Blacksmith runner for the life of
# this workflow run's Bazel job, then drain and exit.
#
# Blacksmith donates this compute for OSS-project work only. The worker
# registers with rbe-west's OSS scheduler (clients reach it with
# --remote_instance_name=oss): OSS CI, plus allowlisted maintainers building
# OSS code with an operator-issued client certificate. Other developer and
# agent builds use the default instance and never run here. The OSS and
# default instances share one CAS,
# but rbe-west splits the action cache: results written here go to the "oss"
# action cache, which fork PRs read anonymously and the default instance
# reads through (never writes). So REMOTE_AC must say "oss", and so must the
# CAS stores: rbe-west's :443 confines this certificate to listeners that
# know instance "oss" alone (infra README "OSS worker certificate confined",
# FU2), where "" is 'instance_name' not configured. The fork tier says
# oss-fork, never "".
#
# Env (from the workflow):
#   RBE_WORKER_TLS_CERT / RBE_WORKER_TLS_KEY  base64 PEM, CN=rbe-oss-worker
#   RBE_WEST_HOST       e.g. rbe-west.ops.gascity.com (from a secret; not in git)
#   RBE_WEST_PORT       443 (default); 8444 for WORKER_TIER=fork
#   WORKER_TIER         oss (default): rbe-west's OSS scheduler, results cached in
#                       AC_OSS (REMOTE_AC). fork: the fork scheduler (rbe-fork,
#                       instance oss-fork; infra README "rbe-fork"): untrusted fork
#                       PR actions, nothing cached (no REMOTE_AC, upload strategy
#                       never), isolation mandatory (RBE_ACTION_ISOLATION must be
#                       1), every action in its own network namespace with
#                       loopback only (NETNS=1), and no user namespaces on the
#                       VM (user.max_user_namespaces=0). Its certificate
#                       (CN=rbe-fork-worker,OU=rbe-fork,O=gascity, fork CA) reaches
#                       nothing but the fork scheduler and the CAS.
#   WORKER_NAME         unique per worker
#   WORKER_MODE         run  (default): serve until BAZEL_JOB_NAME in this run completes
#                                       (needs GH_TOKEN with actions:read)
#                       pool: serve the shared OSS queue; retire after POOL_IDLE_MINUTES
#                             with nothing in flight, or at POOL_MAX_MINUTES
#                       measure: provision and measure this host, check it against
#                             the pin (tools/rbe/worker-env-drift check) and exit;
#                             no certificate, no worker (the canary, and the
#                             bazel job for changes to the worker host)
#   CAS_MAX_BYTES       local CAS cap (default 150 GB)
#   RBE_WIRE_ZSTD       1: fetches of blobs of 64 KiB or more travel as REAPI
#                       compressed-blobs/zstd (NativeLink 1.7.1 GrpcStore
#                       experimental_remote_cache_compression), for both tiers;
#                       uploads stay identity (no writable rbe-west listener
#                       accepts zstd). The remote CAS is split: REMOTE_READ
#                       (zstd, RBE_WIRE_ZSTD_READ_URL) and REMOTE_WRITE
#                       (identity, the worker's own endpoint). Needs
#                       RBE_WIRE_ZSTD_READ_URL set to the dedicated zread
#                       host below, else every such fetch fails with
#                       InvalidArgument (there is no identity fallback). 0
#                       (default): identity through one REMOTE_CAS, as before.
#   RBE_WIRE_ZSTD_READ_URL  where REMOTE_READ fetches when RBE_WIRE_ZSTD=1.
#                       The farm serves zstd reads only on dedicated zread
#                       hostnames, never the worker's own endpoint: OSS
#                       grpcs://rbe-zread.ops.gascity.com:443, fork
#                       grpcs://rbe-fork-zread.ops.gascity.com:8444 (the
#                       normal hosts return InvalidArgument for compressed
#                       reads). Required when RBE_WIRE_ZSTD=1; the worker
#                       exits 2 instead of registering if this is empty or
#                       equal to its own grpcs://RBE_WEST_HOST:RBE_WEST_PORT.
#                       Rollback: set RBE_WIRE_ZSTD (RBE_FORK_WIRE_ZSTD for
#                       the fork pool) to 0; for an emergency rollback cancel
#                       the pool runs, since an in-flight worker keeps its
#                       store (and this switch) until it retires.
#   RBE_WARM_URL        optional https://HOST[/PREFIX] of rbe-west's warm set
#                       (infra README "Warm worker caches"): a read-only bundle
#                       of hot CAS blobs (toolchains, the Go SDK, dependency
#                       archives) that the core selects from trusted results
#                       and publishes on R2; no pool VM can write it. Fetched in
#                       the background while isolation sets up, every blob
#                       re-hashed against its name (warm-cas) before NativeLink
#                       starts and loads it: a failure costs the warm start,
#                       never the worker. Empty (default): a cold CAS, as before.
#   RBE_WARM_EVERY      1 (default): every worker warms. N: only runs whose
#                       GITHUB_RUN_ID is a multiple of N (the A/B canary).
#   RBE_WARM_SECONDS    how long the warm fetch may run, from its start (default
#                       75; isolation and its selftest take about 65 s)
#   RBE_WARM_GRACE      seconds it may run on once isolation is ready (default 5):
#                       registration waits for the warm fetch at most this long
#   RBE_ACTION_ISOLATION  1 (default): every action runs as a per-action slot
#                       user in private namespaces (below). 0: the rollback
#                       switch, actions run as this runner user as before.
#                       canary: workers of one run in RBE_ACTION_CANARY_EVERY
#                       (default 4; GITHUB_RUN_ID % N == 0) try isolation and
#                       fall back to 0 instead of exiting when it fails; the
#                       others run as 0. Logs RBE_ISOLATION_CANARY=ok,
#                       failed phase=<phase> or skipped.
set -euo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
RBE_PRODUCT_ROOT=$(cd "${RBE_PRODUCT_ROOT:-.}" && pwd -P)
echo "rbe-worker: ${RBE_WORKER_REVISION:-in-tree} ($HERE), product $RBE_PRODUCT_ROOT"

WORKER_MODE=${WORKER_MODE:-run}
[ "$WORKER_MODE" = measure ] || : "${RBE_WORKER_TLS_CERT:?}" "${RBE_WORKER_TLS_KEY:?}" "${RBE_WEST_HOST:?}" "${WORKER_NAME:?}"
case "$WORKER_MODE" in
run) : "${BAZEL_JOB_NAME:?}" ;;
pool) POOL_IDLE_MINUTES=${POOL_IDLE_MINUTES:-15} POOL_MAX_MINUTES=${POOL_MAX_MINUTES:-300} ;;
measure) ;;
*) echo "WORKER_MODE must be run, pool or measure" >&2; exit 2 ;;
esac
ACTION_ISOLATION=${RBE_ACTION_ISOLATION:-1}
case "$ACTION_ISOLATION" in
0 | 1 | canary) ;;
*) echo "RBE_ACTION_ISOLATION must be 0, 1 or canary" >&2; exit 2 ;;
esac
case "${RBE_WIRE_ZSTD:-0}" in
1) wire_zstd=true ;;
0) wire_zstd=false ;;
*) echo "RBE_WIRE_ZSTD must be 0 or 1" >&2; exit 2 ;;
esac
warm=0
if [ -n "${RBE_WARM_URL:-}" ] && [ "$WORKER_MODE" = pool ]; then
	if [[ $RBE_WARM_URL =~ ^https://[A-Za-z0-9.-]+(/[A-Za-z0-9._/-]*)?$ && ${RBE_WARM_EVERY:-1} =~ ^[1-9][0-9]{0,5}$ &&
		${RBE_WARM_SECONDS:-75} =~ ^[1-9][0-9]{0,3}$ && ${RBE_WARM_GRACE:-5} =~ ^[0-9]{1,3}$ ]]; then
		warm=1
	else
		echo "::warning title=rbe warm::RBE_WARM_URL, RBE_WARM_EVERY, RBE_WARM_SECONDS or RBE_WARM_GRACE unusable; this worker starts cold"
	fi
fi
WORKER_TIER=${WORKER_TIER:-oss}
case "$WORKER_TIER" in
oss) NETNS=0 ;;
fork)
	# Untrusted actions never run unisolated, and never with a network.
	[ "$ACTION_ISOLATION" = 1 ] || { echo "WORKER_TIER=fork needs RBE_ACTION_ISOLATION=1" >&2; exit 2; }
	NETNS=1
	;;
*) echo "WORKER_TIER must be oss or fork" >&2; exit 2 ;;
esac
RBE_WEST_PORT=${RBE_WEST_PORT:-443}
# measure has no farm host (RBE_WEST_HOST) and never reaches NativeLink.
if [ "$WORKER_MODE" != measure ]; then
	ZSTD_READ_URL=${RBE_WIRE_ZSTD_READ_URL:-grpcs://${RBE_WEST_HOST}:${RBE_WEST_PORT}}
	[[ $ZSTD_READ_URL =~ ^grpcs://[A-Za-z0-9.-]+:[0-9]{1,5}$ ]] || { echo "RBE_WIRE_ZSTD_READ_URL must be grpcs://host:port" >&2; exit 2; }
	# The worker's own endpoint answers compressed reads with InvalidArgument (no
	# identity fallback): zstd needs the dedicated zread host.
	if [ "$wire_zstd" = true ] && [ "$ZSTD_READ_URL" = "grpcs://${RBE_WEST_HOST}:${RBE_WEST_PORT}" ]; then
		echo "RBE_WIRE_ZSTD=1 needs RBE_WIRE_ZSTD_READ_URL (fork pool: RBE_FORK_WIRE_ZSTD_READ_URL) set to the zread host, not grpcs://${RBE_WEST_HOST}:${RBE_WEST_PORT}" >&2
		exit 2
	fi
fi
NL_VERSION=1.7.1
NL_SHA256=a3d7abc2598e976d022fcdabe88a2f8fae46a3ae64f1868698002ca968dd88e9
GO_VERSION=$(awk '/^go /{print $2; exit}' "$RBE_PRODUCT_ROOT/go.mod")
DOLT_VERSION=2.1.8
DOLT_SHA256=f66318f08ed66e409fc39363ae0fff8ce6fbf6dba9f5bac632b91527b9632a74
ROOT="$RUNNER_TEMP/nl-worker"
NL_BIN_DIR="$RUNNER_TEMP/nl-bin"

# Host toolset: test actions exec tools via the client PATH
# (/usr/local/go/bin:/usr/local/bin:/usr/bin:/bin). C/C++ and cgo actions
# compile and link with the hermetic LLVM toolchain and Ubuntu 24.04 sysroot
# registered in MODULE.bazel, never the host's gcc, lld or headers (the
# isolation phase's gcc only builds the action launcher), but the host still
# runs that toolchain and the binaries it links:
# - libstdc++6, libgcc-s1, zlib1g: loaded by clang, lld and the llvm-* tools;
# - libxml2 (and its liblzma5): loaded by lld;
# - libicu74, libstdc++6, libgcc-s1: loaded by every Bazel-built Go binary
#   that links Dolt's go-icu-regex (most tests), as are glibc's;
# - xz-utils: unpacks the toolchain's .tar.xz archive.
# tools/rbe/worker-env measures each of these that an action can reach (its
# measured list) or names it unmeasured, with the reason.
WORKER_TOOLSET=(make jq sqlite3 tmux lsof cmake git libstdc++6 libgcc-s1 zlib1g
	libxml2 liblzma5 xz-utils libicu74 zlib1g-dev libsqlite3-dev libbz2-dev
	liblzma-dev libffi-dev libexpat1-dev libxml2-dev libreadline-dev
	libncurses-dev python3-dev)
sudo DEBIAN_FRONTEND=noninteractive NEEDRESTART_SUSPEND=1 apt-get install -y -qq \
	"${WORKER_TOOLSET[@]}" >/dev/null
if ! /usr/local/go/bin/go version 2>/dev/null | grep -q "go${GO_VERSION} "; then
	sum=$(curl -fsSL "https://go.dev/dl/?mode=json&include=all" |
		jq -r --arg f "go${GO_VERSION}.linux-amd64.tar.gz" '.[].files[] | select(.filename==$f) | .sha256')
	curl -fsSL -o "$RUNNER_TEMP/go.tgz" "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz"
	echo "${sum}  $RUNNER_TEMP/go.tgz" | sha256sum -c -
	sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf "$RUNNER_TEMP/go.tgz"
fi
if ! dolt version 2>/dev/null | grep -q "$DOLT_VERSION"; then
	curl -fsSL -o "$RUNNER_TEMP/dolt.tgz" "https://github.com/dolthub/dolt/releases/download/v${DOLT_VERSION}/dolt-linux-amd64.tar.gz"
	echo "${DOLT_SHA256}  $RUNNER_TEMP/dolt.tgz" | sha256sum -c -
	tar -C "$RUNNER_TEMP" -xzf "$RUNNER_TEMP/dolt.tgz"
	sudo cp -f "$RUNNER_TEMP/dolt-linux-amd64/bin/dolt" /usr/local/bin/dolt
fi

# The worker-env platform property (tools/rbe/worker-env): the sha256 of this
# host's toolchain manifest. rbe-west's schedulers match it exactly against
# the worker-env CI's actions request (//platforms:rbe_worker: the sha256 of
# the committed tools/rbe/worker-env.txt), so an action runs only on a host
# with the toolchain its key names and its cached result is never one another
# toolchain produced. The raw listing (dpkg's versions as installed) is only
# for the drift report and the log; it is never hashed.
"$HERE/worker-env" >"$RUNNER_TEMP/worker-env.txt"
WORKER_ENV=sha256:$(sha256sum <"$RUNNER_TEMP/worker-env.txt" | cut -d' ' -f1)
echo "worker-env: $WORKER_ENV"
# worker-env-base (rbe-reexecution-design.md (c)): the same manifest without its
# go and dolt lines, a second key for actions that use neither (beads builds
# with its go_sdk and tests with a hermetic dolt), so a Go or dolt change on
# this host moves none of their keys. rbe-action-launch hides the host's go and
# dolt from an action that sends only worker-env-base, so its result can never
# have used them. Advertised only with action isolation on (render): without
# the launcher nothing would hide them.
WORKER_ENV_BASE=sha256:$(grep -v -E '^(go|dolt) ' "$RUNNER_TEMP/worker-env.txt" | sha256sum | cut -d' ' -f1)
echo "worker-env-base: $WORKER_ENV_BASE (advertised with action isolation only)"
"$HERE/worker-env" --raw >"$RUNNER_TEMP/worker-env.raw.txt" || :
# A worker with any other toolchain (a new distribution release, a glibc,
# library or tool release, Go or dolt) can serve no gascity action; security
# patches of the same releases measure the same. It registers anyway,
# advertising what it measured: the pools are shared, and actions that send
# no worker-env still run on it. The check prints the diff, the raw listing,
# and the manifest and pin to commit (log and step summary) and leaves them
# in $RUNNER_TEMP/worker-env-drift. The pool workflows measure in a step of
# their own first (WORKER_MODE=measure) and turn that into the pin's drift
# issue, which also caps the farm's pools while it is open. measure: drift is
# the result, so it fails.
if ! (cd "$RBE_PRODUCT_ROOT" && "$HERE/worker-env-drift" check "$RUNNER_TEMP/worker-env.txt" "$RUNNER_TEMP/worker-env.raw.txt"); then
	[ "$WORKER_MODE" != measure ] || exit 3
	echo "worker-env: registering anyway with worker-env=$WORKER_ENV (actions without worker-env only)"
fi
[ "$WORKER_MODE" != measure ] || exit 0
curl -fsSL -o "$RUNNER_TEMP/nl.tgz" "https://github.com/TraceMachina/nativelink/releases/download/v${NL_VERSION}/nativelink-${NL_VERSION}-x86_64-unknown-linux-musl.tar.gz"
echo "${NL_SHA256}  $RUNNER_TEMP/nl.tgz" | sha256sum -c -
mkdir -p "$NL_BIN_DIR" && tar -C "$NL_BIN_DIR" -xzf "$RUNNER_TEMP/nl.tgz" nativelink

# Local CAS (and its temp dir, which must share the filesystem for renames).
# content.exec sits next to content and is wiped by NativeLink at startup. A
# warm start comes only from the read-only warm set (RBE_WARM_URL, warm-cas),
# never from a Blacksmith sticky disk: any job in the product repository could
# commit one, and NativeLink never re-hashes what it loads.
STORE=$ROOT
# work/ must share the CAS's filesystem: NativeLink 1.7.1 hardlinks every input
# from the CAS into the action directory (fs::hard_link_many, no copy fallback).
WORK=$STORE/work
CAS_MAX_BYTES=${CAS_MAX_BYTES:-150000000000}
[[ $CAS_MAX_BYTES =~ ^[1-9][0-9]{9,12}$ ]] || { echo "CAS_MAX_BYTES must be bytes (1e9..1e13)" >&2; exit 2; }
mkdir -p "$ROOT/pki" "$WORK" "$STORE"/{content,tmp}
umask 077
printf '%s' "$RBE_WORKER_TLS_CERT" | base64 -d >"$ROOT/pki/worker.pem"
printf '%s' "$RBE_WORKER_TLS_KEY" | base64 -d >"$ROOT/pki/worker.key"
umask 022

# Warm set (RBE_WARM_URL): fetched in the background (warm-cas makes itself a
# session leader) while isolation sets up and the selftest runs; NativeLink
# starts once it ends (below). It runs as this user, before NativeLink and
# before any action.
warm_pid=
if [ "$warm" = 1 ]; then
	if ((10#${GITHUB_RUN_ID:-0} % ${RBE_WARM_EVERY:-1} == 0)); then
		warm_end=$(($(date +%s) + ${RBE_WARM_SECONDS:-75}))
		"$HERE/warm-cas" "$RBE_WARM_URL" "$STORE" "$warm_end" >"$RUNNER_TEMP/rbe-warm.out" 2>&1 &
		warm_pid=$!
	else
		echo "rbe-warm: status=off reason=canary every=${RBE_WARM_EVERY:-1}"
	fi
fi

# One action per two vCPUs. NativeLink ignores the client cert when
# use_native_roots is set, so trust the system bundle via ca_file instead.
slots=$(($(nproc) / 2)); [ "$slots" -ge 1 ] || slots=1

# Action isolation (S11.3; infra nativelink-cas/west README "Action isolation";
# rbe-action-* here are copies of infra's, keep them in sync). Actions run as
# per-action slot users rbe-aNN (no sudo, no docker, no other group) in private
# pid/mount/ipc namespaces with this runner's home hidden: the checkout,
# $RUNNER_TEMP and pki/worker.key are invisible and unreadable, and so is this
# step's environment (another uid, another pid namespace). An action can no
# longer take the worker cert, so it cannot write the action cache or register
# workers. The runner's own sudo (NOPASSWD) runs the root launcher; slot egress
# is filtered by nftables (NETNS=0), or there is none (WORKER_TIER=fork,
# NETNS=1: loopback only). Nothing of the image changes for the
# runner: its world-writable directories stay so; inside an action / and
# every other mount but the action's own are read-only (ROOT_RO=1), and its
# world-writable sockets on /run are masked (MASK_SOCKETS=1). Each
# phase is logged ("isolation: <phase>") so a step that dies without an error
# still shows where.
LIB=/usr/local/libexec/rbe-action
iso_phase=$RUNNER_TEMP/rbe-isolation-phase iso_reason=$RUNNER_TEMP/rbe-isolation-reason
phase() { echo "isolation: $1"; echo "$1" >"$iso_phase"; }
fail() { echo "isolation: $*" >&2; echo "$*" >"$iso_reason"; exit 1; }
# The canary's ERR trap: the command set -e stops on.
isolation_err() { local rc=$?; echo "${BASH_COMMAND%%$'\n'*} (exit $rc)" >"$iso_reason"; }

# Canary (RBE_ACTION_ISOLATION=canary): five pool-wide rollouts of isolation
# each failed on real Blacksmith runners, every worker exited and the OSS pool
# had none for minutes. A canary worker of a selected run tries isolation; if
# any phase fails it warns, undoes what could touch plain actions and runs as
# RBE_ACTION_ISOLATION=0 (same worker.json, same start, same slots). Mode 1
# stays fail-closed.
canary=
if [ "$ACTION_ISOLATION" = canary ]; then
	every=${RBE_ACTION_CANARY_EVERY:-4}
	ACTION_ISOLATION=0 canary=skipped
	if ! [[ $every =~ ^[1-9][0-9]{0,5}$ && ${GITHUB_RUN_ID:-} =~ ^[0-9]{1,18}$ ]]; then
		echo "::warning title=rbe isolation canary::RBE_ACTION_CANARY_EVERY ($every) or GITHUB_RUN_ID (${GITHUB_RUN_ID:-}) unusable: not selected, actions run without isolation"
	elif ((10#$GITHUB_RUN_ID % every == 0)); then
		ACTION_ISOLATION=1 canary=selected
	fi
	echo "isolation canary: run ${GITHUB_RUN_ID:-} every $every: $canary"
fi
canary_report() { # result [detail]
	canary=${1%% *}
	echo "RBE_ISOLATION_CANARY=$1"
	[ -z "${GITHUB_STEP_SUMMARY:-}" ] ||
		echo "rbe isolation canary: \`RBE_ISOLATION_CANARY=$1\`${2:+ ($2)}, worker $WORKER_NAME, run ${GITHUB_RUN_ID:-}, every $every" >>"$GITHUB_STEP_SUMMARY" || true
}
[ "$canary" != skipped ] || canary_report skipped
# A failed canary: nothing it set up may touch plain actions. NativeLink has
# not started, so work/ holds only what the selftest or probe left (pool mode
# would count it in flight forever). The nft rules match slot uids alone, the
# slot users, sudoers Defaults and $LIB serve nothing else: the table goes
# anyway, the rest stays.
isolation_undo() {
	sudo nft delete table inet rbe_action 2>/dev/null || true
	sudo find "${WORK:-$ROOT/work}" -mindepth 1 -maxdepth 1 -exec rm -rf --one-file-system {} + 2>/dev/null ||
		find "${WORK:-$ROOT/work}" -mindepth 1 -maxdepth 1 -exec rm -rf {} + 2>/dev/null || true
}

render() {
	# zstd, read, casmax, work and worker_env_base are read as $ARGS.named with
	# today's values as defaults, so worker.json is byte-identical to before
	# unless they change.
	jq -n --arg host "grpcs://${RBE_WEST_HOST}:${RBE_WEST_PORT:-443}" --arg root "$ROOT" --arg store "$STORE" --arg name "$WORKER_NAME" --argjson slots "$slots" --arg tier "${WORKER_TIER:-oss}" --arg worker_env "$WORKER_ENV" --argjson zstd "${wire_zstd:-false}" --arg read "${ZSTD_READ_URL:-grpcs://${RBE_WEST_HOST}:${RBE_WEST_PORT:-443}}" --argjson casmax "${CAS_MAX_BYTES:-150000000000}" --arg work "${WORK:-$ROOT/work}" --arg worker_env_base "${WORKER_ENV_BASE:-}" --argjson isolation "$isolation" '
  { cert_file: ($root + "/pki/worker.pem"), key_file: ($root + "/pki/worker.key"),
    ca_file: "/etc/ssl/certs/ca-certificates.crt" } as $tls |
  (if $tier == "fork" then "oss-fork" else "oss" end) as $cas_instance |
  {
    stores: [
      (if ($ARGS.named.zstd // false) then
        # Both tiers: inputs come compressed, uploads stay identity. rbe-west
        # sends ByteStream.Read from this worker certificate to its read-only
        # zstd process (fork: Caddyfile.fork; oss: the :443 route), and no
        # writable listener accepts zstd. fast_direction read_only: reads try
        # REMOTE_READ, writes go to REMOTE_WRITE alone, which says false
        # explicitly so a NativeLink default can never compress an upload.
        { name: "REMOTE_READ", grpc: { instance_name: $cas_instance, endpoints: [{ address: ($ARGS.named.read // $host), tls_config: $tls }], store_type: "cas",
            experimental_remote_cache_compression: true } },
        { name: "REMOTE_WRITE", grpc: { instance_name: $cas_instance, endpoints: [{ address: $host, tls_config: $tls }], store_type: "cas",
            experimental_remote_cache_compression: false } },
        { name: "REMOTE_CAS", fast_slow: { fast: { ref_store: { name: "REMOTE_READ" } }, fast_direction: "read_only",
            slow: { ref_store: { name: "REMOTE_WRITE" } } } }
      else
        { name: "REMOTE_CAS", grpc: { instance_name: $cas_instance, endpoints: [{ address: $host, tls_config: $tls }], store_type: "cas" } }
      end),
      (if $tier == "fork" then empty else
        { name: "REMOTE_AC", grpc: { instance_name: "oss", endpoints: [{ address: $host, tls_config: $tls }], store_type: "ac" } } end),
      { name: "WFS", fast_slow: {
          fast: { filesystem: { content_path: ($store + "/content"), temp_path: ($store + "/tmp"),
                                eviction_policy: { max_bytes: ($ARGS.named.casmax // 150000000000) } } },
          slow: { ref_store: { name: "REMOTE_CAS" } } } }
    ],
    workers: [ { local: ({
      name: $name,
      worker_api_endpoint: { uri: $host, tls_config: $tls },
      cas_fast_slow_store: "WFS",
      # fork: nothing is cached for anyone (README "rbe-fork"); NativeLink
      # refuses a strategy other than never without an ac_store.
      upload_action_result: (if $tier == "fork" then { upload_ac_results_strategy: "never" } else { ac_store: "REMOTE_AC" } end),
      work_directory: ($ARGS.named.work // ($root + "/work")),
      max_inflight_tasks: $slots,
      # worker-env-base only through the launcher (isolation), which hides the
      # host go and dolt it leaves out; a fallback to no isolation drops it.
      platform_properties: ({
        OSFamily: { values: ["linux"] },
        "container-image": { values: [""] },
        ISA: { values: ["x86_64"] },
        "worker-env": { values: [$worker_env] }
      } + (if ($isolation | length) > 0 and ($ARGS.named.worker_env_base // "") != "" then
        { "worker-env-base": { values: [$ARGS.named.worker_env_base] } } else {} end))
    } + $isolation) } ],
    servers: []
  }' >"$ROOT/worker.json"
}

isolate() {
	phase paths
	# Canonical paths: the launcher compares them with the action's pwd -P.
	MASK_ROOT=$(cd "$HOME" && pwd -P)
	WORK_ROOT=$(cd "${WORK:-$ROOT/work}" && pwd -P)
	# The CAS must stay under the mask: outside $HOME it would be visible to
	# actions, so isolation refuses it.
	for d in "$WORK_ROOT" "$(cd "$ROOT" && pwd -P)" "$(cd "$STORE" && pwd -P)"; do
		case "$d" in "$MASK_ROOT"/*) ;; *) fail "$d must be under $MASK_ROOT (MASK_ROOT, hidden from actions)" ;; esac
	done
	phase packages
	# --no-upgrade: this host already advertised worker-env, so add what is
	# missing but move no installed package (libc6-dev would pull libc6, and
	# util-linux and procps are measured).
	sudo DEBIAN_FRONTEND=noninteractive NEEDRESTART_SUSPEND=1 apt-get install -y -qq --no-upgrade \
		gcc libc6-dev nftables file util-linux procps >/dev/null
	phase users
	# The nft rules cover uids 59000-59063 whatever owns them: anything already
	# there (an image user or group) would be filtered like a slot, and a slot
	# sharing its uid or gid would share its files.
	taken=$(awk -F: '$3 >= 59000 && $3 <= 59063 { print FILENAME ": " $1 " (" $3 ")" }' /etc/passwd /etc/group)
	[ -z "$taken" ] || fail "uids/gids 59000-59063 must be free for the slot users, taken: $taken"
	for i in $(seq 0 $((slots - 1))); do
		u=$(printf 'rbe-a%02d' "$i") id=$((SLOT_UID0 + i))
		sudo groupadd --system --gid "$id" "$u"
		sudo useradd --system --uid "$id" --gid "$id" --no-create-home --home-dir /var/lib/rbe-action/home --shell /bin/bash "$u"
	done
	phase compile
	sudo install -d -m 0755 /var/lib/rbe-action /var/lib/rbe-action/home "$LIB" /etc/rbe-west
	gcc -static -O2 -Wall -Wextra -o "$RUNNER_TEMP/rbe-entry" "$HERE/rbe-action-entry.c"
	gcc -static -O2 -Wall -Wextra -DRBE_ACTION_EXEC -o "$RUNNER_TEMP/rbe-exec" "$HERE/rbe-action-entry.c"
	phase install
	sudo install -m 0755 "$RUNNER_TEMP/rbe-entry" "$LIB/entry"
	sudo install -m 0755 "$RUNNER_TEMP/rbe-exec" "$LIB/exec"
	sudo install -m 0755 "$HERE/rbe-action-launch" "$LIB/launch"
	sudo install -m 0755 "$HERE/rbe-action-sweep" "$LIB/sweep"
	sudo install -m 0755 "$HERE/rbe-action-selftest" "$LIB/selftest"
	sudo install -m 0755 "$HERE/undeclared-host-tool" "$LIB/undeclared-host-tool"
	# No directory but the action's own (its outputs, /tmp, /var/tmp, HOME,
	# /dev/shm, TMPFS_DIRS: private per action) may be writable by every
	# action, or one could leave files for a later one. The image's
	# world-writable directories (tool caches the runner or Blacksmith's agent
	# may write, any names) stay as they are on the host; ROOT_RO=1 makes /
	# and every other mount read-only inside each action, so nothing has to be
	# listed. The full selftest checks no action can write one.
	# NETNS_ROOT=0: no action may ask for infra's privileged network namespace
	# class (RBE_X_NETNS_ROOT=1; README "Privileged network namespace": MAIN
	# only, never a worker that runs untrusted code). This launcher copy does
	# not offer it yet; the pin holds once it is synced.
	phase env
	sudo tee /etc/rbe-west/rbe-action.env >/dev/null <<-EOF
		WORK_ROOT=$WORK_ROOT
		MASK_ROOT=$MASK_ROOT
		WORKER_UID=$(id -u)
		WORKER_GID=$(id -g)
		SLOT_UID0=$SLOT_UID0
		SLOT_COUNT=$slots
		BACKSTOP_S=1260
		MAX_TIMEOUT_S=1200
		HOME_DIR=/var/lib/rbe-action/home
		NETNS=${NETNS:-0}
		NETNS_ROOT=0
		SHM_SIZE=8g
		TMPFS_DIRS="/run/lock /var/crash"
		ROOT_RO=1
		MASK_SOCKETS=1
		EGRESS_CHAIN="inet rbe_action output"
		PROBE_DENY="169.254.169.254:80"
		WORKER_JSON=$ROOT/worker.json
	EOF
	phase sudoers
	printf 'Defaults!%s/launch !pam_session, !log_allowed, !use_pty, !lecture\n' "$LIB" |
		sudo tee /etc/sudoers.d/rbe-action >/dev/null
	sudo chmod 0440 /etc/sudoers.d/rbe-action && sudo visudo -cq
	# Slot DNS: a loopback nameserver (systemd-resolved's 127.0.0.53, which
	# queries upstream as itself) needs nothing. A non-loopback IPv4 one, e.g. a
	# private VPC resolver, is allowed to slots on port 53 alone; with neither
	# (IPv6 only), slots could not resolve at all: refuse. No nameserver line:
	# glibc uses 127.0.0.1.
	phase nft
	nameservers=$(awk '$1 == "nameserver" { print $2 }' /etc/resolv.conf)
	dns_v4=$(grep -E '^[0-9]+(\.[0-9]+){3}$' <<<"$nameservers" | grep -v '^127\.' | paste -sd, - | sed 's/,/, /g' || true)
	dns_allow=
	if [ -n "$dns_v4" ]; then
		dns_allow="meta skuid 59000-59063 ip daddr { $dns_v4 } meta l4proto { tcp, udp } th dport 53 accept"
	elif [ -n "$nameservers" ] && ! grep -qE '^(127\.|::1$)' <<<"$nameservers"; then
		fail "no nameserver in /etc/resolv.conf that slot users can reach (loopback or IPv4): $nameservers"
	fi
	sudo nft -f - <<-EOF
		table inet rbe_action
		delete table inet rbe_action
		table inet rbe_action {
			set slot_dst { type ipv4_addr . inet_proto . inet_service; size 65536; flags dynamic,timeout; timeout 1d; counter; }
			chain output {
				type filter hook output priority 0; policy accept;
				oif lo accept
				$dns_allow
				meta skuid 59000-59063 meta nfproto ipv6 meta l4proto tcp reject with tcp reset
				meta skuid 59000-59063 meta nfproto ipv6 reject with icmpx admin-prohibited
				meta skuid 59000-59063 ip daddr { 0.0.0.0/8, 10.0.0.0/8, 100.64.0.0/10, 169.254.0.0/16, 172.16.0.0/12, 192.168.0.0/16, 224.0.0.0/3 } meta l4proto tcp reject with tcp reset
				meta skuid 59000-59063 ip daddr { 0.0.0.0/8, 10.0.0.0/8, 100.64.0.0/10, 169.254.0.0/16, 172.16.0.0/12, 192.168.0.0/16, 224.0.0.0/3 } reject with icmpx admin-prohibited
				meta skuid 59000-59063 meta l4proto { tcp, udp } ct state new update @slot_dst { ip daddr . meta l4proto . th dport }
			}
		}
	EOF
	phase render
	render
	# WORKER_TIER=fork: no user namespaces on this VM before any action runs.
	# They are the usual first step of a kernel escape from an unprivileged
	# process, and nothing here needs one: the launcher's unshare runs as root
	# and creates pid/mount/ipc/net namespaces only (NETNS_ROOT=0: infra's
	# privileged netns class, the only user-namespace path, is never offered).
	# The selftest below and the probe then run with the limit in place.
	if [ "$WORKER_TIER" = fork ]; then
		phase userns
		sudo sysctl -q -w user.max_user_namespaces=0
		[ "$(cat /proc/sys/user/max_user_namespaces)" = 0 ] || fail "user.max_user_namespaces is not 0"
	fi
	# Full selftest: it also checks worker.json routes actions through the
	# entrypoint, the timeout path, and that an action can write no shared
	# directory on any mount (ROOT_RO=1, TMPFS_DIRS private), and no
	# world-writable socket on /run it can connect to (MASK_SOCKETS=1). On
	# every VM, not just a first boot or a new image+script: the mount walk
	# that dominates its cost on Blacksmith's large tool caches (~49 s,
	# 2026-10-06) skips a mount that is already read-only, so it stays in
	# the few-seconds range the rest of the selftest runs in without
	# trusting an unverified VM on a cached pass (max review, 2026-10-07).
	phase selftest
	selftest_out=$RUNNER_TEMP/rbe-selftest.out
	# shellcheck disable=SC2024 # the runner's file, not root's
	if ! sudo "$LIB/selftest" >"$selftest_out"; then
		cat "$selftest_out"
		fail "selftest: $(grep -E '^(FAIL|      )' "$selftest_out" | sed -E 's/^ +[^:]+: / /' | tr -s '\n ' ' ')"
	fi
	cat "$selftest_out"
	phase sudo
	LC_ALL=C sudo -l -U rbe-a00 2>&1 | grep -q 'not allowed to run sudo' || fail "slot users must have no sudo"
	# A world-writable socket an action can open (Blacksmith's 0666 VM
	# shutdown socket, snapd, a docker.sock) is a way out of the sandbox,
	# read-only mount or not. The host keeps them; what counts is what an
	# action reaches: the selftest above connected to each from inside one
	# (MASK_SOCKETS=1 masks them there, journald's aside), and asked the
	# host's resolver over the system bus and varlink (no-resolver).
	phase sockets
	grep -q '^ok    action: no-open-socket' "$selftest_out" ||
		fail "world-writable sockets reachable by actions: $(sed -n 's/^ *world-writable socket the action can connect to: //p' "$selftest_out" | tr '\n' ' ')"
	grep -q '^ok    action: no-resolver' "$selftest_out" ||
		fail "the host's resolver is reachable by actions (a DNS tunnel): $(grep -m1 'action: no-resolver' "$selftest_out")"
	phase probe
	# What S11.3 is about, on this VM's layout: a probe action through the real
	# entrypoint must run as a slot user and fail to read the worker key, find
	# this step's environment in any process, or sudo.
	probe=$WORK_ROOT/isolation-probe-$$
	mkdir -p "$probe/work"
	# shellcheck disable=SC2016 # the action's own script
	out=$(cd "$probe/work" && env -i PATH=/usr/bin:/bin "$LIB/entry" /bin/bash -c '
		echo "uid $(id -u)"
		cat "$1" >/dev/null 2>&1 && echo "LEAK worker.key"
		grep -qs RBE_WORKER_TLS_KEY /proc/[0-9]*/environ && echo "LEAK step environment"
		sudo -n true >/dev/null 2>&1 && echo "LEAK sudo"
		[ "$2" = fork ] && unshare --user true >/dev/null 2>&1 && echo "LEAK userns"
		true' probe "$ROOT/pki/worker.key" "$WORKER_TIER" 2>&1) || true
	rm -rf "$probe"
	echo "isolation probe: $(tr '\n' ' ' <<<"$out")"
	if ! grep -qE "^uid 590[0-9]{2}$" <<<"$out" || grep -q LEAK <<<"$out"; then
		fail "probe failed ($(tr '\n' ' ' <<<"$out")); RBE_ACTION_ISOLATION=0 (repository variable) is the rollback"
	fi
}

plain_slots=$slots
isolation='{}'
if [ "$ACTION_ISOLATION" = 1 ]; then
	# The nft rules cover uids 59000-59063.
	[ "$slots" -le 64 ] || slots=64
	SLOT_UID0=59000
	# RBE_X_NETWORK: the action's `network` platform property ("" without
	# one; infra README "Per-action network"). off: the launcher gives it
	# loopback only, as NETNS=1 does for every fork action; on or none: this
	# tier's default. The fork tier ignores it. RBE_X_WORKER_ENV and
	# RBE_X_WORKER_ENV_BASE: the action's worker-env and worker-env-base;
	# with the base alone the launcher hides the host's go and dolt
	# (rbe-action-launch action_host_tools).
	isolation='{ "entrypoint": "/usr/local/libexec/rbe-action/entry", "timeout_handled_externally": true, "max_action_timeout": 1260,
		"additional_environment": { "RBE_X_TIMEOUT_MS": "timeout_millis", "RBE_X_SIDE_CHANNEL": "side_channel_file",
			"RBE_X_NETWORK": { "property": "network" },
			"RBE_X_WORKER_ENV": { "property": "worker-env" }, "RBE_X_WORKER_ENV_BASE": { "property": "worker-env-base" } } }'
	if [ "$canary" = selected ]; then
		# A subshell: set -e works there (it would not in a condition), and a
		# failure ends it, not the worker. The phase file says where.
		rm -f "$iso_phase" "$iso_reason"
		set +e
		(
			set -eE
			trap isolation_err ERR
			isolate
		)
		rc=$?
		set -e
		if [ "$rc" = 0 ]; then
			canary_report ok
		else
			failed=$(cat "$iso_phase" 2>/dev/null || echo start)
			reason=$(head -c 1000 "$iso_reason" 2>/dev/null | tr -s '\r\n ' ' ' | sed 's/ $//' || true)
			reason=${reason:-exit $rc}
			echo "::warning title=rbe isolation canary::isolation failed in phase $failed: ${reason//%/%25}; this worker runs actions without isolation"
			canary_report "failed phase=$failed" "$reason"
			isolation_undo
			ACTION_ISOLATION=0 slots=$plain_slots isolation='{}'
			render
		fi
	else
		isolate
	fi
else
	render
fi

if [ -n "${warm_pid:-}" ]; then
	# Isolation is ready: the warm fetch gets RBE_WARM_GRACE more seconds (never
	# past its own deadline), then SIGTERM (it reports what it placed and
	# exits), then 10 s, then its whole session goes: registration waits at
	# most the grace and the report. Its session id is read now, while it runs
	# (it made itself a session leader at start).
	warm_sid=$(ps -o sid= -p "$warm_pid" | tr -d ' ' || true)
	stop=$(($(date +%s) + ${RBE_WARM_GRACE:-5}))
	[ "$stop" -lt "$warm_end" ] || stop=$warm_end
	while kill -0 "$warm_pid" 2>/dev/null && [ "$(date +%s)" -lt "$stop" ]; do sleep 0.5; done
	kill -TERM "$warm_pid" 2>/dev/null || true
	stop=$(($(date +%s) + 10))
	while kill -0 "$warm_pid" 2>/dev/null && [ "$(date +%s)" -lt "$stop" ]; do sleep 0.5; done
	# Its whole session (warm-cas, its chunk readers, their curl and zstd),
	# also when warm-cas itself has exited and left a member behind. Only if it
	# was a session of its own: the kernel keeps a session id from reuse while
	# any member lives, so the id names nothing else.
	if [ "$warm_sid" = "$warm_pid" ]; then
		pkill -KILL -s "$warm_sid" || true
	fi
	wait "$warm_pid" 2>/dev/null || true
	grep -m1 '^rbe-warm: ' "$RUNNER_TEMP/rbe-warm.out" ||
		{ echo "rbe-warm: status=error reason=no-report"; tail -n 3 "$RUNNER_TEMP/rbe-warm.out" || true; }
fi
# Each action's receipt is a debug line of local_worker: the boot-to-first-
# action measurement below reads it. NativeLink 1.7.1 clears the environment
# of every action it spawns (running_actions_manager: env_clear), so actions
# never see this filter. Its persistent-worker spawn (supports-workers=1) does
# pass NativeLink's own environment on: with isolation that spawn is the
# entrypoint in WORK_ROOT, which the launcher refuses (exit 125) before any
# action code runs; without isolation (the rollback) such an action already
# gets this step's whole environment. Neither product sends persistent-worker
# actions remotely (infra README "Action isolation").
NL_RUST_LOG="info,nativelink_worker::local_worker=debug"
if [ "$ACTION_ISOLATION" = 1 ]; then
	# The cert is in files from here on. NativeLink's persistent-worker spawn
	# hands its own environment to the action: give it none of this step's.
	env -i PATH="$PATH" HOME="$HOME" RUST_LOG="$NL_RUST_LOG" "$NL_BIN_DIR/nativelink" "$ROOT/worker.json" >"$ROOT/worker.log" 2>&1 &
else
	RUST_LOG="$NL_RUST_LOG" "$NL_BIN_DIR/nativelink" "$ROOT/worker.json" >"$ROOT/worker.log" 2>&1 &
fi
nl=$!
echo "worker $WORKER_NAME started (pid $nl, $slots slots, action isolation $ACTION_ISOLATION${canary:+, canary $canary})"

# A launcher NativeLink killed (cancel, backstop timeout) leaves a slot-owned
# action directory NativeLink cannot remove, which pool mode would count as in
# flight forever: the sweep removes it (the MAIN worker runs it on a timer).
sweep() { [ "$ACTION_ISOLATION" = 0 ] || sudo "$LIB/sweep" || true; }

# SIGTERM makes nativelink finish in-flight actions and send GoingAway.
if [ "$WORKER_MODE" = run ]; then
	# Until this run's Bazel job completes. 30s polls keep a run's two
	# workers well under GITHUB_TOKEN's 1,000 requests/hour/repository.
	jobs_url="repos/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}/attempts/${GITHUB_RUN_ATTEMPT}/jobs?per_page=100"
	while kill -0 "$nl" 2>/dev/null; do
		sweep
		status=$(gh api "$jobs_url" --jq ".jobs[] | select(.name == \"$BAZEL_JOB_NAME\") | .status" 2>/dev/null || true)
		[ "$status" = "completed" ] && break
		sleep 30
	done
else
	# Pool: one running action = one directory under work/. No GitHub API use.
	started=$SECONDS idle=0
	while kill -0 "$nl" 2>/dev/null; do
		sweep
		inflight=$(find "${WORK:-$ROOT/work}" -mindepth 1 -maxdepth 1 -type d | wc -l)
		if [ "$inflight" -eq 0 ]; then idle=$((idle + 30)); else idle=0; fi
		[ "$idle" -ge $((POOL_IDLE_MINUTES * 60)) ] && { echo "idle ${POOL_IDLE_MINUTES}m, retiring"; break; }
		[ $((SECONDS - started)) -ge $((POOL_MAX_MINUTES * 60)) ] && { echo "max age ${POOL_MAX_MINUTES}m, retiring"; break; }
		sleep 30
	done
fi
if kill -0 "$nl" 2>/dev/null; then
	kill -TERM "$nl"
	timeout 300 tail --pid="$nl" -f /dev/null || kill -KILL "$nl"
fi
grep -E 'registered|GoingAway|ERROR' "$ROOT/worker.log" | tail -20 || true
# P5: what this VM cost rbe-west (bytes received on the default route since boot,
# the local CAS size) and how many compressed reads fell back or failed.
dev=$(ip route show default | awk '{ for (i = 1; i < NF; i++) if ($i == "dev") print $(i + 1); exit }')
echo "rbe-pull: tier=$WORKER_TIER zstd=$wire_zstd rx_bytes=$(cat "/sys/class/net/$dev/statistics/rx_bytes" 2>/dev/null || echo ?)" \
	"cas_bytes=$(du -sb "$STORE/content" | cut -f1) zstd_fallbacks=$(grep -c 'falling back to identity' "$ROOT/worker.log" || true)" \
	"zstd_refused=$(grep -c 'refusing identity fallback' "$ROOT/worker.log" || true)"
# Boot to first action: seconds from registration to the first action received
# and to the first one executing (its inputs fetched), and the actions run.
python3 - "$ROOT/worker.log" <<'PY' || true
import re, sys
from datetime import datetime
ansi, ts = re.compile(r"\x1b\[[0-9;]*m"), re.compile(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{1,6})\d*Z")
reg = recv = exe = None
n = 0
for ln in open(sys.argv[1], errors="replace"):
    ln = ansi.sub("", ln)
    m = ts.search(ln)
    if not m:
        continue
    t = datetime.fromisoformat(m.group(1)).timestamp()
    if reg is None and "Worker registered with scheduler" in ln:
        reg = t
    elif recv is None and "Received request to run action" in ln:
        recv = t
    elif "Executing command" in ln:
        exe = t if exe is None else exe
        n += 1
d = lambda a, b: "-" if a is None or b is None else f"{a - b:.1f}"
print(f"rbe-first-exec: registered_to_first_recv_s={d(recv, reg)} first_recv_to_first_exec_s={d(exe, recv)}"
      f" registered_to_first_exec_s={d(exe, reg)} actions={n}")
PY
# The warm blobs still in the CAS (generation 0: NativeLink writes 1 and up),
# re-hashed after the run. A mismatch means a CAS file was rewritten in place,
# which nothing may do: the pin bump's go/no-go requires bad=0.
if [ -n "${warm_pid:-}" ]; then
	(cd "$STORE/content/d2" && find . -maxdepth 1 -type f -name '*-0' -print0 |
		xargs -0 -r -n 64 -P "$(nproc)" sha256sum |
		awk '{ n = $2; sub(".*/", "", n); c++; if (substr(n, 1, 64) != $1) bad++ } END { printf "rbe-warm-audit: checked=%d bad=%d\n", c, bad }') || true
fi
if [ "$ACTION_ISOLATION" = 1 ]; then
	echo "slot egress (destination . protocol . port, packets): for the NETNS=1 decision"
	sudo nft list set inet rbe_action slot_dst | sed -n '/elements/,/}/p' || true
fi
