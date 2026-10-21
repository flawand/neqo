#!/usr/bin/env bash
# Self-contained reproducer for the SO_SNDBUF experiment (mozilla/neqo#1962, PR #3898).
#
#   ./reproduce.sh setup            create 3 read-only-ish worktrees under $WORK/src and apply the temporary patch
#   ./reproduce.sh mac   [rounds]   macOS A/B run (needs a Mac; Rust toolchain + NSS as for normal neqo builds)
#   ./reproduce.sh linux [rounds]   Linux A/B run in Docker (builds an image with NSS from source, ~10+ min first time)
#   ./reproduce.sh summary <dir>    print mean timings (and WouldBlock counts) from a results dir
#   ./reproduce.sh probe            quick facts: SO_SNDBUF defaults, clamp/doubling rule (python3; Docker optional)
#
# Env: REPO (your neqo checkout, default ~/firefox_projects/neqo), WORK (default $REPO/../sndbuf-repro),
#      FILTERS (default: all four variants), ROTATE=1 rotates configuration order per round (default 0 = fixed
#      order, which is what produced the published numbers; fixed order puts `main` first in every block).
#      LTO / JOBS for the Docker build (default thin / 2; fat LTO is OOM-killed in a 2 GB Docker Desktop VM).
#
# Configurations:  main (pristine) | pr (PR #3898) | exp-* (= main + temporary instrumentation patch,
#   NEQO_SNDBUF env forces SO_SNDBUF and `WouldBlock`/send counts are printed at exit).
#   Linux: exp-default, exp-64k (65536), exp-1m (1048576).   macOS: exp-default, exp-256k, exp-1m
#   (sizes must exceed 65631: quinn-udp already raises Apple sockets to 65631 and silently rounds smaller requests up).
# Nothing here is committed, pushed or posted. The patch is for experiments only: never merge it.
set -euo pipefail

REPO=${REPO:-$HOME/firefox_projects/neqo}
WORK=${WORK:-$(cd "$REPO/.." && pwd)/sndbuf-repro}
MAIN_SHA=ed8d6b28ef3d10d61d0cf05cf931931b60e0e9e8   # main when the experiment was run
PR_SHA=07b39e9c                                     # head of PR #3898 when the experiment was run
FILTERS=${FILTERS:-"1-100mb-resp 1-100mb-req 10_000-parallel 1-1b-resp"}
cmd=${1:-help}; shift || true

write_assets() {
  mkdir -p "$WORK/docker"
  cat >"$WORK/exp-sndbuf.patch" <<'__PATCH__'
diff --git a/neqo-bin/src/udp.rs b/neqo-bin/src/udp.rs
index d7287b6c..528564c0 100644
--- a/neqo-bin/src/udp.rs
+++ b/neqo-bin/src/udp.rs
@@ -11,6 +11,11 @@ use std::{io, iter, net::SocketAddr};
 use neqo_common::{datagram, qdebug, qwarn};
 use neqo_udp::{DatagramIter, RecvBuf};
 
+// EXPERIMENT (never merge).
+static EXP_ONCE: std::sync::Once = std::sync::Once::new();
+static EXP_SENDS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
+static EXP_WOULDBLOCK: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
+
 /// Ideally this would live in [`neqo_udp`]. [`neqo_udp`] is used in Firefox.
 ///
 /// Firefox uses `cargo vet`. [`tokio`] the dependency of [`neqo_udp`] is not
@@ -56,6 +61,35 @@ impl Socket {
             state.send_buffer_size((&socket).into())
         );
 
+        // EXPERIMENT (never merge): NEQO_SNDBUF=<bytes> forces SO_SNDBUF; print sizes and
+        // WouldBlock counts once, at process exit.
+        let sndbuf_initial = state.send_buffer_size((&socket).into());
+        if let Some(n) = std::env::var("NEQO_SNDBUF")
+            .ok()
+            .and_then(|v| v.parse::<usize>().ok())
+        {
+            let r = state.set_send_buffer_size((&socket).into(), n);
+            if r.is_err() {
+                eprintln!("EXP set SO_SNDBUF {n} FAILED: {r:?}");
+            }
+        }
+        EXP_ONCE.call_once(|| {
+            eprintln!(
+                "EXP SO_SNDBUF initial={sndbuf_initial:?} final={:?} requested={:?}",
+                state.send_buffer_size((&socket).into()),
+                std::env::var("NEQO_SNDBUF").ok()
+            );
+            extern "C" fn report() {
+                eprintln!(
+                    "EXP SENDS={} WOULDBLOCK={}",
+                    EXP_SENDS.load(std::sync::atomic::Ordering::Relaxed),
+                    EXP_WOULDBLOCK.load(std::sync::atomic::Ordering::Relaxed)
+                );
+            }
+            // SAFETY: `report` is a plain `extern "C" fn()` with no unwinding.
+            unsafe { libc::atexit(report) };
+        });
+
         let recv_buf_before = state.recv_buffer_size((&socket).into())?;
         if recv_buf_before < ONE_MB {
             // Same as Firefox.
@@ -104,9 +138,14 @@ impl Socket {
 
     /// Send a [`datagram::Batch`] on the given [`Socket`].
     pub fn send(&self, d: &datagram::Batch) -> io::Result<()> {
-        self.inner.try_io(tokio::io::Interest::WRITABLE, || {
+        EXP_SENDS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
+        let r = self.inner.try_io(tokio::io::Interest::WRITABLE, || {
             neqo_udp::send_inner(&self.state, (&self.inner).into(), d)
-        })
+        });
+        if matches!(&r, Err(e) if e.kind() == io::ErrorKind::WouldBlock) {
+            EXP_WOULDBLOCK.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
+        }
+        r
     }
 
     /// Receive a batch of [`neqo_common::Datagram`]s on the given [`Socket`], each set with
__PATCH__
  cat >"$WORK/docker/Dockerfile" <<'__DOCKERFILE__'
# Linux benchmark environment for the SO_SNDBUF experiment (mozilla/neqo#1962).
# Toolchain + NSS only. Neqo sources are bind-mounted at run time (see run-linux.sh),
# so nothing from the host checkouts is copied into the image.
#
# Mirrors neqo's qns/Dockerfile: same pinned base image, NSS built from source.
#
# Build (NSS version/checksum come from `cargo metadata`, see ../README or results file):
#   docker build -t neqo-sndbuf \
#     --build-arg NSS_VERSION=3.126 \
#     --build-arg NSS_SHA256=c95a4cf93d6939000642254422d71dffccb5c017d0f40733d3e2be18aa93f1be \
#     sndbuf-experiment/docker
FROM lukemathwalker/cargo-chef@sha256:b8cfeff0a392bc4bf5066210875728562b5b6ba66fe3b67aae0c6b7d56df1043

ARG NSS_VERSION
ARG NSS_SHA256

RUN apt-get update && apt-get install -y --no-install-recommends \
    libclang-dev gyp ninja-build python-is-python3 iproute2 procps curl ca-certificates \
    && apt-get clean -y && rm -rf /var/lib/apt/lists/*

ENV NSS_DIR=/nss \
    NSS_TARGET=Release \
    NSS_PREBUILT=1 \
    NSPR_DIR=/nspr \
    LD_LIBRARY_PATH=/dist/Release/lib

RUN set -eux; \
    NSS_TAG=$(echo "$NSS_VERSION" | tr '.' '_'); \
    RELEASE_DIR="https://ftp.mozilla.org/pub/security/nss/releases/NSS_${NSS_TAG}_RTM/src"; \
    FILENAME=$(curl -fsSL "$RELEASE_DIR/SHA256SUMS" | grep -F "$NSS_SHA256 " | awk '{print $2}' | sed 's/^\*//'); \
    case "$FILENAME" in \
      nss-*-with-nspr-*.tar.gz) ;; \
      *) echo "No with-nspr artifact in NSS_${NSS_TAG}_RTM matches sha256 $NSS_SHA256" >&2; exit 1 ;; \
    esac; \
    curl -fL -o /tmp/nss.tar.gz "$RELEASE_DIR/$FILENAME"; \
    echo "$NSS_SHA256  /tmp/nss.tar.gz" | sha256sum -c; \
    tar xzf /tmp/nss.tar.gz --strip-components=1 -C /; \
    rm /tmp/nss.tar.gz
RUN "$NSS_DIR"/build.sh --static -Ddisable_tests=1 -Ddisable_dbm=1 -Ddisable_libpkix=1 -Ddisable_ckbi=1 -Ddisable_fips=1 -o

WORKDIR /work
__DOCKERFILE__
  cat >"$WORK/docker/inner.sh" <<'__INNER__'
#!/usr/bin/env bash
# Runs INSIDE the container (see ../run-linux.sh). Sources are mounted read-only at
# /src/{main,pr,exp}; per-side build dirs live in the /target volume; raw output goes to /out.
set -uo pipefail

ROUNDS=${ROUNDS:-3}
FILTERS=(${FILTERS:-1-100mb-resp 1-100mb-req 10_000-parallel 1-1b-resp})
export LC_ALL=C MTU=1500 CARGO_HOME=/cargo-home
# Docker Desktop's VM has ~2 GB RAM; the default fat-LTO bench link gets OOM-killed (SIGKILL).
# Applied identically to every side, but differs from the CI profile. Override with LTO=fat if the VM has more RAM.
export CARGO_PROFILE_RELEASE_LTO=${LTO:-thin} CARGO_PROFILE_BENCH_LTO=${LTO:-thin} CARGO_BUILD_JOBS=${JOBS:-2}

# Docker disables IPv6 by default; the bench binds [::]:0 (run-linux.sh passes --sysctl ...disable_ipv6=0).
# CI sets loopback MTU to 1500 (.github/actions/testdata-setup); needs NET_ADMIN, affects only this container.
ip link set dev lo mtu 1500 || echo "WARNING: could not set lo mtu (need --cap-add NET_ADMIN)"

{
  echo "== environment"
  uname -a; nproc; free -m | head -2
  echo "lo: $(ip -o link show lo)"
  python3 -c "import socket;s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM);print('plain UDP socket default SO_SNDBUF',s.getsockopt(1,socket.SO_SNDBUF))" 2>/dev/null || true
  echo "wmem sysctls: $(cat /proc/sys/net/core/wmem_default /proc/sys/net/core/wmem_max 2>&1 | tr '\n' ' ')"
  rustc --version
  echo "commits (from host): main=$SHA_MAIN pr=$SHA_PR exp=$SHA_EXP(+exp-sndbuf.patch)"; echo "cargo LTO=$CARGO_PROFILE_BENCH_LTO jobs=$CARGO_BUILD_JOBS (CI uses fat)"
} | tee /out/environment.txt

# label  source-dir  extra-env (NEQO_SNDBUF only applies to the patched `exp` build)
SIDES=(
  "main|main|"
  "pr|pr|"
  "exp-default|exp|"
  "exp-64k|exp|NEQO_SNDBUF=65536"
  "exp-1m|exp|NEQO_SNDBUF=1048576"
)

build() { # dir
  echo ">>> building $1"
  (cd /src/$1 && CARGO_TARGET_DIR=/target/$1 cargo bench --locked -p neqo-bin --bench main --features bench --no-run) 2>&1 | tail -3
}
for d in main pr exp; do build $d || exit 1; done

for n in $(seq 1 "$ROUNDS"); do
  for f in "${FILTERS[@]}"; do
    off=$(( ${ROTATE:-0} ? (n-1) % ${#SIDES[@]} : 0 ))
    for k in $(seq 0 $(( ${#SIDES[@]} - 1 ))); do
      side=${SIDES[$(( (k+off) % ${#SIDES[@]} ))]}
      IFS='|' read -r label dir extra <<<"$side"
      file="/out/${label}-run${n}-${f}.txt"
      echo ">>> $label run $n: $f"
      (cd /src/$dir && env ${extra:+$extra} CARGO_TARGET_DIR=/target/$dir \
        cargo bench --locked -p neqo-bin --bench main --features bench -- "$f") >"$file" 2>&1
      if ! grep -q "time:" "$file"; then
        echo "RUN FAILED (no timing line): $file"; grep -vE "warning|nightly|Zcargo-lints|^$" "$file" | tail -8; exit 1
      fi
    done
  done
done
echo "done"
__INNER__
  cat >"$WORK/summary.py" <<'__SUMMARY__'
import re,glob,collections,sys,os
d=collections.defaultdict(list); ex=collections.defaultdict(list); sides=[]
for f in sorted(glob.glob(os.path.join(sys.argv[1],'*-run*.txt'))):
    m=re.match(r'(.*/)?([\w-]+)-run(\d+)-(.*)\.txt',f); _,side,run,var=m.groups()
    t=open(f).read()
    mt=re.search(r'time:\s+\[([\d.]+) (\S+) ([\d.]+) (\S+) ([\d.]+) (\S+)\]',t)
    if not mt: print("NO TIMING:",f); continue
    d[(var,side)].append(float(mt.group(3))); 
    if side not in sides: sides.append(side)
    e=re.search(r'EXP SENDS=(\d+) WOULDBLOCK=(\d+)',t); b=re.search(r'EXP SO_SNDBUF initial=\S+ final=Ok\((\d+)\)',t)
    if e: ex[(var,side)].append((int(e.group(2)),b.group(1) if b else '?'))
order=[s for s in ('main','pr','exp-default','exp-64k','exp-256k','exp-1m') if s in sides]+[s for s in sides if s not in ('main','pr','exp-default','exp-64k','exp-256k','exp-1m')]
print("Mean of Criterion middle estimates, ms per iteration (runs in parentheses)")
for var in sorted({v for v,_ in d}):
    print("\n##",var)
    for s in order:
        v=d[(var,s)]
        if v: print(f"  {s:12} mean {sum(v)/len(v):9.3f}   runs {v}")
        if ex[(var,s)]: print(f"  {'':12} WouldBlock per run / final SO_SNDBUF: {ex[(var,s)]}")
__SUMMARY__
}

setup() {
  write_assets
  mkdir -p "$WORK/src"
  git -C "$REPO" fetch https://github.com/mozilla/neqo.git pull/3898/head >/dev/null 2>&1 || true
  git -C "$REPO" cat-file -e "$MAIN_SHA^{commit}" 2>/dev/null || git -C "$REPO" fetch https://github.com/mozilla/neqo.git main >/dev/null 2>&1
  local pr=$PR_SHA
  git -C "$REPO" cat-file -e "$pr^{commit}" 2>/dev/null || { echo "WARN: $PR_SHA not found (PR force-pushed?); using current PR head"; pr=FETCH_HEAD; }
  [ -e "$WORK/src/main" ] || git -C "$REPO" worktree add --detach "$WORK/src/main" "$MAIN_SHA"
  [ -e "$WORK/src/pr" ]   || git -C "$REPO" worktree add --detach "$WORK/src/pr" "$pr"
  if [ ! -e "$WORK/src/exp" ]; then
    git -C "$REPO" worktree add --detach "$WORK/src/exp" "$MAIN_SHA"
    git -C "$WORK/src/exp" apply "$WORK/exp-sndbuf.patch"
  fi
  for d in main pr exp; do echo "$d: $(git -C "$WORK/src/$d" rev-parse --short HEAD)"; done
  echo "setup done: $WORK"
}

mac() {
  [ "$(uname -s)" = Darwin ] || { echo "mac mode needs macOS"; exit 1; }
  setup; local rounds=${1:-3} out="$WORK/results/macos"; mkdir -p "$out"
  export MTU=1500 LC_ALL=C
  local sides=("main|main|" "pr|pr|" "exp-default|exp|" "exp-256k|exp|NEQO_SNDBUF=262144" "exp-1m|exp|NEQO_SNDBUF=1048576")
  { echo "== environment"; uname -a; sysctl -n machdep.cpu.brand_string; rustc --version
    sysctl net.inet.udp.maxdgram; for d in main pr exp; do echo "$d: $(git -C "$WORK/src/$d" rev-parse --short HEAD)"; done; } | tee "$out/environment.txt"
  for d in main pr exp; do echo ">>> building $d"; (cd "$WORK/src/$d" && cargo bench --locked -p neqo-bin --bench main --features bench --no-run 2>&1 | tail -1); done
  local n f k off side label dir extra file
  for n in $(seq 1 "$rounds"); do for f in $FILTERS; do
    off=$(( ${ROTATE:-0} ? (n-1) % ${#sides[@]} : 0 ))
    for k in $(seq 0 $(( ${#sides[@]} - 1 ))); do
      side=${sides[$(( (k+off) % ${#sides[@]} ))]}; IFS='|' read -r label dir extra <<<"$side"
      file="$out/${label}-run${n}-${f}.txt"; echo ">>> $label run $n: $f"
      (cd "$WORK/src/$dir" && env ${extra:+$extra} cargo bench --locked -p neqo-bin --bench main --features bench -- "$f") >"$file" 2>&1
      grep -q "time:" "$file" || { echo "RUN FAILED (no timing line): $file"; tail -8 "$file"; exit 1; }
    done
  done; done
  echo "done: $out"; python3 "$WORK/summary.py" "$out"
}

linux() {
  command -v docker >/dev/null && docker info >/dev/null 2>&1 || { echo "Docker daemon not running"; exit 1; }
  setup; local rounds=${1:-3} out="$WORK/results/linux"; mkdir -p "$out"
  local meta nssv nsss
  meta=$(cd "$WORK/src/main" && cargo metadata --locked --offline --format-version=1 -q 2>/dev/null || cargo metadata --locked --format-version=1 -q)
  nssv=$(python3 -c "import json,sys;m=json.load(sys.stdin);print([p for p in m['packages'] if p['name']=='nss-rs'][0]['metadata']['nss']['min-version'])" <<<"$meta")
  nsss=$(python3 -c "import json,sys;m=json.load(sys.stdin);print([p for p in m['packages'] if p['name']=='nss-rs'][0]['metadata']['nss']['sha256'])" <<<"$meta")
  echo ">>> building image neqo-sndbuf (NSS $nssv from source; slow the first time)"
  docker build -t neqo-sndbuf --build-arg NSS_VERSION="$nssv" --build-arg NSS_SHA256="$nsss" "$WORK/docker"
  # IPv6 must be enabled (the bench binds [::]:0); NET_ADMIN lets the container set its OWN lo MTU to 1500 like CI.
  docker run --rm --cap-add NET_ADMIN --sysctl net.ipv6.conf.all.disable_ipv6=0 \
    -e SHA_MAIN="$(git -C "$WORK/src/main" rev-parse --short HEAD)" -e SHA_PR="$(git -C "$WORK/src/pr" rev-parse --short HEAD)" -e SHA_EXP="$(git -C "$WORK/src/exp" rev-parse --short HEAD)" \
    -e LTO="${LTO:-}" -e JOBS="${JOBS:-}" -e ROUNDS="$rounds" -e FILTERS="$FILTERS" -e ROTATE="${ROTATE:-0}" \
    -v "$WORK/src/main:/src/main:ro" -v "$WORK/src/pr:/src/pr:ro" -v "$WORK/src/exp:/src/exp:ro" \
    -v sndbuf-cargo-home:/cargo-home -v sndbuf-target:/target \
    -v "$WORK/docker/inner.sh:/inner.sh:ro" -v "$out:/out" \
    neqo-sndbuf bash /inner.sh
  python3 "$WORK/summary.py" "$out"
}

probe() {
  write_assets
  echo "--- host (this machine)"
  python3 -c "import socket;s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM);print('default SO_SNDBUF',s.getsockopt(socket.SOL_SOCKET,socket.SO_SNDBUF))"
  [ "$(uname -s)" = Darwin ] && sysctl net.inet.udp.maxdgram
  if command -v docker >/dev/null && docker info >/dev/null 2>&1; then
    echo "--- Docker VM kernel (host netns: real sysctls)"
    docker run --rm --net=host alpine sh -c 'echo wmem_default=$(cat /proc/sys/net/core/wmem_default) wmem_max=$(cat /proc/sys/net/core/wmem_max)'
    echo "--- request -> getsockopt inside a container (Linux: clamp to wmem_max, then double)"
    docker run --rm python:3-slim python3 -c "
import socket
s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM); print('no setsockopt ->',s.getsockopt(socket.SOL_SOCKET,socket.SO_SNDBUF))
for r in (65536,106496,212992,300000,1048576):
    s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM); s.setsockopt(socket.SOL_SOCKET,socket.SO_SNDBUF,r); print('request',r,'->',s.getsockopt(socket.SOL_SOCKET,socket.SO_SNDBUF))"
  fi
}

case "$cmd" in
  setup) setup ;; mac) mac "$@" ;; linux) linux "$@" ;; probe) probe ;;
  summary) python3 - "$@" <<'__SUM2__'
import re,glob,collections,sys,os
d=collections.defaultdict(list); ex=collections.defaultdict(list); sides=[]
for f in sorted(glob.glob(os.path.join(sys.argv[1],'*-run*.txt'))):
    m=re.match(r'(.*/)?([\w-]+)-run(\d+)-(.*)\.txt',f); _,side,run,var=m.groups()
    t=open(f).read()
    mt=re.search(r'time:\s+\[([\d.]+) (\S+) ([\d.]+) (\S+) ([\d.]+) (\S+)\]',t)
    if not mt: print("NO TIMING:",f); continue
    d[(var,side)].append(float(mt.group(3))); 
    if side not in sides: sides.append(side)
    e=re.search(r'EXP SENDS=(\d+) WOULDBLOCK=(\d+)',t); b=re.search(r'EXP SO_SNDBUF initial=\S+ final=Ok\((\d+)\)',t)
    if e: ex[(var,side)].append((int(e.group(2)),b.group(1) if b else '?'))
order=[s for s in ('main','pr','exp-default','exp-64k','exp-256k','exp-1m') if s in sides]+[s for s in sides if s not in ('main','pr','exp-default','exp-64k','exp-256k','exp-1m')]
print("Mean of Criterion middle estimates, ms per iteration (runs in parentheses)")
for var in sorted({v for v,_ in d}):
    print("\n##",var)
    for s in order:
        v=d[(var,s)]
        if v: print(f"  {s:12} mean {sum(v)/len(v):9.3f}   runs {v}")
        if ex[(var,s)]: print(f"  {'':12} WouldBlock per run / final SO_SNDBUF: {ex[(var,s)]}")
__SUM2__
  ;;
  *) sed -n '2,22p' "$0" ;;
esac
