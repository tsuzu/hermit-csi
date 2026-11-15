#!/usr/bin/env bash
# End-to-end test on kind: the released manifests (deploy/, deploy/scheduler/)
# with real kubelets, CSI sidecars, snapshot-controller, hermit-scheduler and
# MinIO, on LVM thin pools backed by loop devices.
#
# Usage: test/e2e/run.sh up | test | dump | down
#
# Prerequisites: root via sudo, docker, kind, kubectl, lvm2 and
# thin-provisioning-tools on the host, the images ghcr.io/tsuzu/hermit-csi:e2e
# and ghcr.io/tsuzu/hermit-scheduler:e2e built locally, and an S3 endpoint
# (MinIO) on the host at port $S3_PORT with bucket $S3_BUCKET.
set -euo pipefail
cd "$(dirname "$0")"

CLUSTER=${CLUSTER:-hermit-e2e}
NODE_IMAGE=${NODE_IMAGE:-kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5}
SNAPSHOTTER_VERSION=${SNAPSHOTTER_VERSION:-v8.2.0} # matches the csi-snapshotter sidecar
BUSYBOX=${BUSYBOX:-busybox:1.37}
S3_PORT=${S3_PORT:-9000}
S3_BUCKET=${S3_BUCKET:-hermit-e2e}
IMG_DIR=${IMG_DIR:-/var/tmp/hermit-e2e}
WORKERS=("$CLUSTER-worker" "$CLUSTER-worker2")
NS=hermit-e2e
K=(kubectl --context "kind-$CLUSTER")

log() { printf '\n=== %s\n' "$*"; }
fail() { printf '\nFAIL: %s\n' "$*" >&2; exit 1; }

# ------------------------------------------------------------------ up / down

up() {
  log "host: thin pools on loop devices (one VG per kind worker, named after it)"
  sudo modprobe dm_thin_pool
  sudo modprobe loop
  # kind nodes only see device nodes that exist when they start, and kubelet
  # needs free loop devices for Block volumes: create spare nodes up front.
  for i in $(seq 0 63); do
    [ -e "/dev/loop$i" ] || sudo mknod -m 0660 "/dev/loop$i" b 7 "$i"
  done
  sudo mkdir -p "$IMG_DIR"
  for vg in "${WORKERS[@]}"; do
    img="$IMG_DIR/$vg.img"
    sudo truncate -s 2G "$img"
    loop=$(sudo losetup -f --show "$img")
    sudo pvcreate -y "$loop"
    # The host must never activate these: the driver inside the node does.
    sudo vgcreate --setautoactivation n "$vg" "$loop"
    sudo lvcreate -y -L 1500M --chunksize 64k -T "$vg/pool"
    sudo vgchange -an "$vg"
  done
  sudo vgs

  log "kind cluster $CLUSTER"
  kind create cluster --name "$CLUSTER" --image "$NODE_IMAGE" --config kind-config.yaml --wait 3m
  docker pull -q "$BUSYBOX"
  kind load docker-image --name "$CLUSTER" ghcr.io/tsuzu/hermit-csi:e2e ghcr.io/tsuzu/hermit-scheduler:e2e "$BUSYBOX"

  log "snapshot CRDs and snapshot-controller $SNAPSHOTTER_VERSION"
  "${K[@]}" apply -k "https://github.com/kubernetes-csi/external-snapshotter/client/config/crd?ref=$SNAPSHOTTER_VERSION"
  "${K[@]}" apply -k "https://github.com/kubernetes-csi/external-snapshotter/deploy/kubernetes/snapshot-controller?ref=$SNAPSHOTTER_VERSION"

  log "hermit-csi + hermit-scheduler"
  curl -sf "http://127.0.0.1:$S3_PORT/minio/health/ready" || fail "no S3 endpoint on the host at port $S3_PORT"
  gw=$(docker network inspect kind -f '{{range .IPAM.Config}}{{.Gateway}} {{end}}' | tr ' ' '\n' | grep -E '^[0-9]+\.' | head -1)
  cat > overlay/s3.yaml <<EOF
apiVersion: v1
kind: ConfigMap
metadata: {name: hermit-csi, namespace: kube-system}
data:
  S3_BUCKET: $S3_BUCKET
  S3_PREFIX: e2e
  S3_ENDPOINT: http://$gw:$S3_PORT
  S3_REGION: us-east-1
  S3_PATH_STYLE: "true"
---
apiVersion: v1
kind: Secret
metadata: {name: hermit-csi, namespace: kube-system}
stringData:
  AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID:-minioadmin}
  AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY:-minioadmin}
  HERMIT_AGENT_TOKEN: e2e-token
EOF
  "${K[@]}" apply -k overlay
  "${K[@]}" -n kube-system rollout status deploy/snapshot-controller --timeout=3m
  "${K[@]}" -n kube-system rollout status deploy/hermit-csi-controller --timeout=3m
  "${K[@]}" -n kube-system rollout status ds/hermit-csi-node --timeout=3m
  "${K[@]}" -n kube-system rollout status deploy/hermit-scheduler --timeout=3m
}

down() {
  kind delete cluster --name "$CLUSTER" || true
  for vg in "${WORKERS[@]}"; do
    sudo vgremove -ff -y "$vg" 2>/dev/null || true
    img="$IMG_DIR/$vg.img"
    for loop in $(sudo losetup -j "$img" -O NAME --noheadings 2>/dev/null); do
      sudo losetup -d "$loop"
    done
    sudo rm -f "$img"
  done
}

dump() {
  set +e
  "${K[@]}" get nodes -o wide
  "${K[@]}" get pods -A -o wide
  "${K[@]}" get pv,pvc,volumeattachment,volumesnapshot,volumesnapshotcontent -A
  "${K[@]}" -n "$NS" describe pods
  "${K[@]}" get events -A --sort-by=.lastTimestamp | tail -80
  for sel in app=hermit-csi-controller app=hermit-csi-node app=hermit-scheduler; do
    for p in $("${K[@]}" -n kube-system get pod -l "$sel" -o name); do
      echo "----- logs $p"
      "${K[@]}" -n kube-system logs "$p" --all-containers --prefix --tail=300
    done
  done
  sudo lvs -a -o+thin_id,lv_tags
  sudo dmsetup ls
}

# ------------------------------------------------------------------ helpers

# run_pod NAME CLAIM fs|block SCHEDULER AVOID_NODE SCRIPT
# The pod runs SCRIPT (set -e) and then reports Ready; a failing SCRIPT makes
# it exit, so waiting for Ready doubles as the assertion.
run_pod() {
  local name=$1 claim=$2 mode=$3 sched=$4 avoid=$5 script=$6 mount affinity=""
  if [ "$mode" = fs ]; then
    mount="volumeMounts: [{name: data, mountPath: /data}]"
  else
    mount="volumeDevices: [{name: data, devicePath: /dev/xvda}]"
  fi
  if [ -n "$avoid" ]; then
    affinity="affinity: {nodeAffinity: {requiredDuringSchedulingIgnoredDuringExecution: {nodeSelectorTerms: [{matchExpressions: [{key: kubernetes.io/hostname, operator: NotIn, values: [$avoid]}]}]}}}"
  fi
  "${K[@]}" -n "$NS" apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata: {name: $name}
spec:
  schedulerName: $sched
  $affinity
  restartPolicy: Never
  terminationGracePeriodSeconds: 5
  containers:
    - name: c
      image: $BUSYBOX
      command: [sh, -c]
      args:
        - |
          set -e
$(printf '%s\n' "$script" | sed 's/^/          /')
          touch /tmp/ready
          exec sleep 3600
      readinessProbe: {exec: {command: [cat, /tmp/ready]}, periodSeconds: 2}
      $mount
  volumes:
    - {name: data, persistentVolumeClaim: {claimName: $claim}}
EOF
  if ! "${K[@]}" -n "$NS" wait --for=condition=Ready "pod/$name" --timeout=5m; then
    "${K[@]}" -n "$NS" describe pod "$name"
    "${K[@]}" -n "$NS" logs "$name" || true
    fail "pod $name did not become ready"
  fi
}

node_of() { "${K[@]}" -n "$NS" get pod "$1" -o jsonpath='{.spec.nodeName}'; }
pv_of() { "${K[@]}" -n "$NS" get pvc "$1" -o jsonpath='{.spec.volumeName}'; }
other_worker() { if [ "$1" = "${WORKERS[0]}" ]; then echo "${WORKERS[1]}"; else echo "${WORKERS[0]}"; fi; }

# delete_pod NAME [CLAIM]: graceful stop; with CLAIM, also wait until the
# volume is detached (NodeUnstage's final backup and ControllerUnpublish done).
delete_pod() {
  "${K[@]}" -n "$NS" delete pod "$1" --wait --timeout=5m
  [ -n "${2:-}" ] || return 0
  local pv; pv=$(pv_of "$2")
  for _ in $(seq 1 150); do
    if ! "${K[@]}" get volumeattachment -o jsonpath='{range .items[*]}{.spec.source.persistentVolumeName}{"\n"}{end}' | grep -qx "$pv"; then
      return 0
    fi
    sleep 2
  done
  fail "volume $pv of $2 was not detached"
}

# driver_log_count NODE MESSAGE PV: occurrences of a driver log line for PV.
driver_log_count() {
  local pod
  pod=$("${K[@]}" -n kube-system get pod -l app=hermit-csi-node --field-selector "spec.nodeName=$1" -o jsonpath='{.items[0].metadata.name}')
  "${K[@]}" -n kube-system logs "$pod" -c driver | grep -F "\"$2\"" | grep -cF "volume=\"$3\"" || true
}

expect_eq() { [ "$2" = "$3" ] || fail "$1: got '$2', want '$3'"; echo "ok: $1 = $3"; }

# ballast NODE: a pod requesting most of NODE's CPU, so the default scoring
# (least allocated) prefers the other node. hermit-scheduler must still pick
# the node with the local copy.
ballast() {
  local cpu milli
  cpu=$("${K[@]}" get node "$1" -o jsonpath='{.status.allocatable.cpu}')
  case $cpu in *m) milli=${cpu%m} ;; *) milli=$((cpu * 1000)) ;; esac
  "${K[@]}" -n "$NS" apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata: {name: ballast}
spec:
  nodeName: $1
  terminationGracePeriodSeconds: 0
  containers:
    - {name: c, image: $BUSYBOX, command: [sleep, "3600"], resources: {requests: {cpu: $((milli * 6 / 10))m}}}
EOF
  "${K[@]}" -n "$NS" wait --for=condition=Ready pod/ballast --timeout=2m
  # Sanity check: without locality the default scheduler now avoids NODE.
  "${K[@]}" -n "$NS" apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata: {name: probe}
spec:
  terminationGracePeriodSeconds: 0
  containers: [{name: c, image: $BUSYBOX, command: [sleep, "3600"]}]
EOF
  "${K[@]}" -n "$NS" wait --for=condition=PodScheduled pod/probe --timeout=2m
  expect_eq "default-scheduler placement with ballast on $1" "$(node_of probe)" "$(other_worker "$1")"
  "${K[@]}" -n "$NS" delete pod probe --wait
}

# ------------------------------------------------------------------ test

test_filesystem() {
  log "Filesystem volume: write, locality, move, in-use snapshot"
  "${K[@]}" -n "$NS" apply -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: fs-data}
spec:
  storageClassName: hermit
  accessModes: [ReadWriteOnce]
  resources: {requests: {storage: 1Gi}}
EOF
  run_pod fs-w1 fs-data fs hermit-scheduler "" '
dd if=/dev/urandom of=/data/a.bin bs=1M count=8
md5sum /data/a.bin > /data/a.md5
sync'
  local n1 n2 pv
  n1=$(node_of fs-w1); n2=$(other_worker "$n1"); pv=$(pv_of fs-data)
  echo "fs-data ($pv) first staged on $n1"
  delete_pod fs-w1 fs-data

  log "locality: back to $n1 despite the ballast, without a restore"
  ballast "$n1"
  local reused; reused=$(driver_log_count "$n1" "local copy is current, skipping restore" "$pv")
  run_pod fs-r1 fs-data fs hermit-scheduler "" 'md5sum -c /data/a.md5'
  expect_eq "fs-r1 node" "$(node_of fs-r1)" "$n1"
  expect_eq "reuse count on $n1" "$(driver_log_count "$n1" "local copy is current, skipping restore" "$pv")" "$((reused + 1))"
  delete_pod fs-r1 fs-data
  "${K[@]}" -n "$NS" delete pod ballast --wait

  log "move: forced off $n1, restored from S3 on $n2"
  local restores; restores=$(driver_log_count "$n2" "restoring volume from S3" "$pv")
  run_pod fs-r2 fs-data fs hermit-scheduler "$n1" '
md5sum -c /data/a.md5
dd if=/dev/urandom of=/data/b.bin bs=1M count=4
md5sum /data/b.bin > /data/b.md5
sync'
  expect_eq "fs-r2 node" "$(node_of fs-r2)" "$n2"
  expect_eq "restores on $n2" "$(driver_log_count "$n2" "restoring volume from S3" "$pv")" "$((restores + 1))"
  delete_pod fs-r2 fs-data

  log "locality follows the volume: $n2 now, despite the ballast there"
  ballast "$n2"
  run_pod fs-r3 fs-data fs hermit-scheduler "" 'md5sum -c /data/a.md5 /data/b.md5'
  expect_eq "fs-r3 node" "$(node_of fs-r3)" "$n2"
  "${K[@]}" -n "$NS" delete pod ballast --wait

  log "in-use snapshot and a volume restored from it"
  "${K[@]}" -n "$NS" apply -f - <<EOF
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshot
metadata: {name: fs-snap}
spec:
  volumeSnapshotClassName: hermit
  source: {persistentVolumeClaimName: fs-data}
EOF
  "${K[@]}" -n "$NS" wait --for=jsonpath='{.status.readyToUse}'=true volumesnapshot/fs-snap --timeout=5m
  "${K[@]}" -n "$NS" exec fs-r3 -- sh -c 'echo later > /data/c.txt; sync'
  "${K[@]}" -n "$NS" apply -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: fs-clone}
spec:
  storageClassName: hermit
  accessModes: [ReadWriteOnce]
  resources: {requests: {storage: 1Gi}}
  dataSource: {apiGroup: snapshot.storage.k8s.io, kind: VolumeSnapshot, name: fs-snap}
EOF
  run_pod fs-c1 fs-clone fs hermit-scheduler "" '
md5sum -c /data/a.md5 /data/b.md5
test ! -e /data/c.txt'
  delete_pod fs-c1 fs-clone
  delete_pod fs-r3 fs-data
}

test_block() {
  log "Block volume: write, move, verify"
  "${K[@]}" -n "$NS" apply -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: blk-data}
spec:
  storageClassName: hermit
  volumeMode: Block
  accessModes: [ReadWriteOnce]
  resources: {requests: {storage: 256Mi}}
EOF
  run_pod blk-w1 blk-data block hermit-scheduler "" '
dd if=/dev/urandom of=/tmp/x bs=1M count=16
dd if=/tmp/x of=/dev/xvda bs=1M seek=3 conv=fsync
md5sum < /tmp/x | cut -d" " -f1 > /tmp/sum
echo "SUM $(cat /tmp/sum)"'
  local n1 sum
  n1=$(node_of blk-w1)
  sum=$("${K[@]}" -n "$NS" logs blk-w1 | sed -n 's/^SUM //p')
  [ -n "$sum" ] || fail "no checksum from blk-w1"
  delete_pod blk-w1 blk-data
  run_pod blk-r1 blk-data block hermit-scheduler "$n1" "
got=\$(dd if=/dev/xvda bs=1M skip=3 count=16 | md5sum | cut -d' ' -f1)
[ \"\$got\" = $sum ]"
  expect_eq "blk-r1 node" "$(node_of blk-r1)" "$(other_worker "$n1")"
  delete_pod blk-r1 blk-data
}

test_cleanup() {
  log "cleanup: DeleteSnapshot / DeleteVolume"
  "${K[@]}" -n "$NS" delete volumesnapshot fs-snap --wait --timeout=2m
  "${K[@]}" -n "$NS" delete pvc --all --wait --timeout=2m
  for _ in $(seq 1 60); do
    [ -z "$("${K[@]}" get pv -o name)" ] && { echo "ok: all PVs deleted"; return 0; }
    sleep 2
  done
  "${K[@]}" get pv
  fail "PVs were not deleted"
}

run_tests() {
  "${K[@]}" create namespace "$NS" --dry-run=client -o yaml | "${K[@]}" apply -f -
  test_filesystem
  test_block
  test_cleanup
  log "PASS"
}

case "${1:-}" in
  up) up ;;
  test) run_tests ;;
  dump) dump ;;
  down) down ;;
  *) echo "usage: $0 up|test|dump|down" >&2; exit 2 ;;
esac
