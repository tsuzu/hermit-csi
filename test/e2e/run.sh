#!/usr/bin/env bash
# End-to-end test on kind: the released manifests (deploy/, deploy/scheduler/)
# with real kubelets, CSI sidecars, snapshot-controller, hermit-scheduler and
# MinIO, on LVM thin pools backed by loop devices.
#
# Usage: test/e2e/run.sh up | test | dump | down
#
# Prerequisites: root via sudo, docker, kind, kubectl, aws CLI, python3, lvm2
# and thin-provisioning-tools on the host, the images
# ghcr.io/tsuzu/hermit-csi:e2e and ghcr.io/tsuzu/hermit-scheduler:e2e built
# locally, and either a MinIO server binary at $MINIO_BIN (E2E_S3=minio, the
# default) or docker access to the SeaweedFS image (E2E_S3=seaweedfs).
#
# S3 access uses no static keys: the pods' projected service account tokens
# are exchanged for temporary credentials with AssumeRoleWithWebIdentity
# (deploy/components/web-identity). The S3 store's own STS trusts the kind API
# server as its OpenID provider, so this exercises the IRSA code path.
set -euo pipefail
cd "$(dirname "$0")"

CLUSTER=${CLUSTER:-hermit-e2e}
NODE_IMAGE=${NODE_IMAGE:-kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5}
SNAPSHOTTER_VERSION=${SNAPSHOTTER_VERSION:-v8.2.0} # matches the csi-snapshotter sidecar
BUSYBOX=${BUSYBOX:-busybox:1.37}
S3_PORT=${S3_PORT:-9000}
E2E_S3=${E2E_S3:-minio} # minio | seaweedfs
SEAWEEDFS_IMAGE=${SEAWEEDFS_IMAGE:-chrislusf/seaweedfs:4.53}
MINIO_BIN=${MINIO_BIN:-$HOME/minio-bin/minio}
MINIO_DIR=${MINIO_DIR:-/tmp/hermit-e2e-minio}
MINIO_ROOT=minioadmin # only used here, to create the bucket
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
  # Containers cannot modprobe: load every device-mapper target the driver uses.
  sudo modprobe dm_thin_pool
  sudo modprobe dm_snapshot
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

  case $E2E_S3 in
    minio) start_minio ;;
    seaweedfs) start_seaweedfs ;;
    *) fail "unknown E2E_S3=$E2E_S3" ;;
  esac

  log "hermit-csi + hermit-scheduler"
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
  S3_ROLE_ARN: $ROLE_ARN
  S3_STS_ENDPOINT: http://$gw:$S3_PORT
---
# No S3 keys: credentials come only from AssumeRoleWithWebIdentity.
apiVersion: v1
kind: Secret
metadata: {name: hermit-csi, namespace: kube-system}
stringData:
  HERMIT_AGENT_TOKEN: e2e-token
EOF
  "${K[@]}" apply -k overlay
  "${K[@]}" -n kube-system rollout status deploy/snapshot-controller --timeout=3m
  "${K[@]}" -n kube-system rollout status deploy/hermit-csi-controller --timeout=3m
  "${K[@]}" -n kube-system rollout status ds/hermit-csi-node --timeout=3m
  "${K[@]}" -n kube-system rollout status deploy/hermit-scheduler --timeout=3m
  preflight
}

driver_pod() {
  "${K[@]}" -n kube-system get pod -l app=hermit-csi-node --field-selector "spec.nodeName=$1" -o jsonpath='{.items[0].metadata.name}'
}

# preflight: create a thin LV and a thin snapshot of it from inside each node
# plugin, the way the driver does, then remove them. If it hangs, show where (wchan, kernel stack, LVM's
# own debug log) instead of timing out a pod much later.
preflight() {
  log "preflight: thin LV inside each node plugin"
  local n
  for n in "${WORKERS[@]}"; do
    "${K[@]}" -n kube-system exec "$(driver_pod "$n")" -c driver -- bash -c '
vg=$1
lvcreate -vvvv -y --thin -V 64m -n e2e-preflight "$vg/pool" > /tmp/preflight.log 2>&1 &
pid=$!
for _ in $(seq 1 30); do kill -0 $pid 2>/dev/null || break; sleep 1; done
if kill -0 $pid 2>/dev/null; then
  echo "lvcreate on $vg still running after 30s; processes:"
  for d in /proc/[0-9]*; do
    p=${d#/proc/}
    echo "$p [$(cat $d/wchan 2>/dev/null)] $(tr "\0" " " < $d/cmdline 2>/dev/null)"
  done
  for d in /proc/[0-9]*; do
    case "$(tr "\0" " " < $d/cmdline 2>/dev/null)" in
      *lvcreate*|*dmeventd*|*thin_check*|*udevadm*|*modprobe*|*dmsetup*)
        echo "--- kernel stack of ${d#/proc/}: $(tr "\0" " " < $d/cmdline)"; cat $d/stack ;;
    esac
  done
  echo "--- last lines of lvcreate -vvvv"; tail -60 /tmp/preflight.log
  exit 1
fi
if ! wait $pid; then tail -60 /tmp/preflight.log; exit 1; fi
timeout 60 lvcreate -y -s -n e2e-preflight-snap "$vg/e2e-preflight"
timeout 60 lvchange -ay -K "$vg/e2e-preflight-snap"
test -b "/dev/$vg/e2e-preflight-snap"
lvs "$vg"
lvremove -y "$vg/e2e-preflight-snap" "$vg/e2e-preflight"
' _ "$n" || fail "LVM preflight failed on $n"
  done
}

# oidc_prep: let the S3 store's STS fetch the cluster's OpenID discovery
# document and JWKS anonymously, and save the cluster CA to $1. Sets CP_IP.
oidc_prep() {
  CP_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}' "$CLUSTER-control-plane" | awk '{print $1}')
  "${K[@]}" create clusterrolebinding hermit-e2e-oidc-discovery \
    --clusterrole=system:service-account-issuer-discovery --group=system:unauthenticated \
    --dry-run=client -o yaml | "${K[@]}" apply -f -
  docker cp "$CLUSTER-control-plane:/etc/kubernetes/pki/ca.crt" "$1"
  curl -sf --cacert "$1" "https://$CP_IP:6443/.well-known/openid-configuration" \
    || fail "OpenID discovery of the kind API server is not reachable"
  echo
}

# start_seaweedfs: SeaweedFS (weed server -s3) on the kind network, with an
# STS trusting the cluster's tokens for the hermit service accounts only
# (seaweedfs-iam.json). Sets ROLE_ARN.
start_seaweedfs() {
  log "SeaweedFS ($SEAWEEDFS_IMAGE) with STS trusting the cluster's service account issuer"
  rm -rf "$MINIO_DIR" && mkdir -p "$MINIO_DIR"
  oidc_prep "$MINIO_DIR/kind-ca.crt"
  sed "s#JWKS_URI#https://$CP_IP:6443/openid/v1/jwks#" seaweedfs-iam.json > "$MINIO_DIR/iam.json"
  # Static identity only for creating the bucket from the host.
  printf '{"identities":[{"name":"admin","credentials":[{"accessKey":"%s","secretKey":"%s"}],"actions":["Admin","Read","Write","List"]}]}' \
    "$MINIO_ROOT" "$MINIO_ROOT" > "$MINIO_DIR/s3.json"
  docker rm -f "$CLUSTER-seaweedfs" >/dev/null 2>&1 || true
  docker run -d --name "$CLUSTER-seaweedfs" --network kind -p "$S3_PORT:8333" \
    -v "$MINIO_DIR/s3.json:/etc/s3.json:ro" -v "$MINIO_DIR/iam.json:/etc/iam.json:ro" \
    -v "$MINIO_DIR/kind-ca.crt:/etc/kind-ca.crt:ro" \
    "$SEAWEEDFS_IMAGE" server -dir=/data -s3 -s3.config=/etc/s3.json -s3.iam.config=/etc/iam.json -ip.bind=0.0.0.0
  for _ in $(seq 1 60); do
    AWS_ACCESS_KEY_ID=$MINIO_ROOT AWS_SECRET_ACCESS_KEY=$MINIO_ROOT AWS_REGION=us-east-1 \
      aws --endpoint-url "http://127.0.0.1:$S3_PORT" s3 mb "s3://$S3_BUCKET" 2>/dev/null && break
    sleep 1
  done
  AWS_ACCESS_KEY_ID=$MINIO_ROOT AWS_SECRET_ACCESS_KEY=$MINIO_ROOT AWS_REGION=us-east-1 \
    aws --endpoint-url "http://127.0.0.1:$S3_PORT" s3 ls "s3://$S3_BUCKET" || fail "SeaweedFS is not serving S3"
  ROLE_ARN=arn:aws:iam::role/HermitRole
}

# start_minio: MinIO on the host, with the kind API server as OpenID provider
# for its STS. Sets ROLE_ARN to the role that maps those tokens to readwrite.
start_minio() {
  log "MinIO with STS trusting the cluster's service account issuer"
  rm -rf "$MINIO_DIR" && mkdir -p "$MINIO_DIR/data" "$MINIO_DIR/certs/CAs"
  oidc_prep "$MINIO_DIR/certs/CAs/kind-ca.crt"
  MINIO_ROOT_USER=$MINIO_ROOT MINIO_ROOT_PASSWORD=$MINIO_ROOT \
  MINIO_IDENTITY_OPENID_CONFIG_URL="https://$CP_IP:6443/.well-known/openid-configuration" \
  MINIO_IDENTITY_OPENID_CLIENT_ID=sts.amazonaws.com \
  MINIO_IDENTITY_OPENID_ROLE_POLICY=readwrite \
    nohup "$MINIO_BIN" server "$MINIO_DIR/data" --address "0.0.0.0:$S3_PORT" --certs-dir "$MINIO_DIR/certs" \
    > "$MINIO_DIR/minio.log" 2>&1 &
  echo $! > "$MINIO_DIR/pid"
  for _ in $(seq 1 30); do
    curl -sf "http://127.0.0.1:$S3_PORT/minio/health/ready" && break
    sleep 1
  done
  AWS_ACCESS_KEY_ID=$MINIO_ROOT AWS_SECRET_ACCESS_KEY=$MINIO_ROOT AWS_REGION=us-east-1 \
    aws --endpoint-url "http://127.0.0.1:$S3_PORT" s3 mb "s3://$S3_BUCKET"
  # MinIO names the role after the client ID, arn:minio:iam:<region>::role/<base64url(sha1(client ID))>,
  # and lists it at startup ("IAM Roles: ...").
  local id
  id=$(printf %s sts.amazonaws.com | python3 -c 'import sys,hashlib,base64;print(base64.urlsafe_b64encode(hashlib.sha1(sys.stdin.buffer.read()).digest()).rstrip(b"=").decode())')
  for _ in $(seq 1 20); do
    ROLE_ARN=$(sed 's/\x1b\[[0-9;]*m//g' "$MINIO_DIR/minio.log" | grep -oE "arn:minio:iam:[^: ]*::role/$id" | head -1 || true)
    [ -n "$ROLE_ARN" ] && break
    sleep 1
  done
  [ -n "$ROLE_ARN" ] || { tail -40 "$MINIO_DIR/minio.log"; fail "MinIO did not register an OpenID role for client ID sts.amazonaws.com"; }
  echo "MinIO OpenID role: $ROLE_ARN"
}

down() {
  [ -f "$MINIO_DIR/pid" ] && kill "$(cat "$MINIO_DIR/pid")" 2>/dev/null || true
  docker rm -f "$CLUSTER-seaweedfs" >/dev/null 2>&1 || true
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
  echo "----- S3 store ($E2E_S3)"
  if [ "$E2E_S3" = seaweedfs ]; then docker logs "$CLUSTER-seaweedfs" 2>&1 | tail -80; else tail -60 "$MINIO_DIR/minio.log"; fi
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
  "${K[@]}" -n kube-system logs "$(driver_pod "$1")" -c driver | grep -F "\"$2\"" | grep -cF "volume=\"$3\"" || true
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

# metrics POD: the driver's /metrics, through the API server's pod proxy.
metrics() { "${K[@]}" get --raw "/api/v1/namespaces/kube-system/pods/$1:9811/proxy/metrics"; }

# metric_value POD SERIES: value of the series whose name and labels start with SERIES.
# Empty if the series is missing (not an error, so callers can report it).
metric_value() { { metrics "$1" | grep -F "$2" | grep -v '^#' || true; } | awk 'NR == 1 {print $NF}'; }

# check_metrics NODE PV: the node reports a recent backup of the staged PV,
# the in-use snapshot's backup, and its thin pool; the controller reports GC.
check_metrics() {
  local pod last now age
  pod=$(driver_pod "$1")
  last=$(metric_value "$pod" "hermit_volume_last_backup_timestamp_seconds{volume=\"$2\"}")
  [ -n "$last" ] || { metrics "$pod" | grep '^hermit_' || true; fail "no last-backup metric for $2 on $1"; }
  now=$(date +%s)
  age=$(awk -v n="$now" -v l="$last" 'BEGIN{printf "%d", n - l}')
  [ "$age" -ge 0 ] && [ "$age" -lt 600 ] || fail "last backup of $2 is ${age}s old"
  echo "ok: last backup of $2 on $1 was ${age}s ago"
  expect_eq "consecutive failures of $2" "$(metric_value "$pod" "hermit_volume_backup_consecutive_failures{volume=\"$2\"}")" 0
  expect_eq "fenced $2" "$(metric_value "$pod" "hermit_volume_fenced{volume=\"$2\"}")" 0
  local snaps; snaps=$(metric_value "$pod" 'hermit_backups_total{result="success",trigger="snapshot"}')
  [ "${snaps:-0}" -ge 1 ] || fail "no successful snapshot backup counted on $1"
  echo "ok: snapshot backups on $1 = $snaps"
  local pool; pool=$(metric_value "$pod" "hermit_thin_pool_data_usage_ratio{")
  awk -v p="$pool" 'BEGIN{exit !(p > 0 && p < 1)}' || fail "thin pool usage on $1 = '$pool'"
  echo "ok: thin pool usage on $1 = $pool"
  local ctl; ctl=$("${K[@]}" -n kube-system get pod -l app=hermit-csi-controller -o jsonpath='{.items[0].metadata.name}')
  metrics "$ctl" | grep -q '^hermit_gc_last_success_timestamp_seconds ' || fail "controller lacks GC metrics"
  expect_eq "controller S3 errors" "$(metrics "$ctl" | grep -c '^hermit_s3_requests_total{.*result="error"' || true)" 0
}

# kubelet_stat NODE NAME PVC: kubelet's kubelet_volume_stats_NAME for PVC
# (from NodeGetVolumeStats), through the API server's node proxy. Empty if
# kubelet has not collected it yet.
kubelet_stat() {
  { "${K[@]}" get --raw "/api/v1/nodes/$1/proxy/metrics" | grep "^kubelet_volume_stats_$2{" |
    grep -F "namespace=\"$NS\"" | grep -F "persistentvolumeclaim=\"$3\"" || true; } | awk 'NR == 1 {print $NF}'
}

# check_volume_stats NODE PVC MIN_USED: kubelet reports the filesystem usage
# of the 1Gi PVC. kubelet refreshes volume stats about once a minute, so poll.
check_volume_stats() {
  local used="" cap="" inodes=""
  for _ in $(seq 1 60); do
    used=$(kubelet_stat "$1" used_bytes "$2")
    [ -n "$used" ] && break
    sleep 5
  done
  if [ -z "$used" ]; then
    "${K[@]}" get --raw "/api/v1/nodes/$1/proxy/metrics" | grep '^kubelet_volume_stats_' || true
    fail "kubelet on $1 reports no kubelet_volume_stats_used_bytes for $2"
  fi
  cap=$(kubelet_stat "$1" capacity_bytes "$2")
  inodes=$(kubelet_stat "$1" inodes_used "$2")
  awk -v u="$used" -v m="$3" -v c="$cap" 'BEGIN{exit !(u >= m && c >= u && c <= 1073741824 && c > 805306368)}' ||
    fail "volume stats of $2 on $1: used '$used' (want >= $3), capacity '$cap' (want about 1Gi)"
  awk -v i="$inodes" 'BEGIN{exit !(i > 0)}' || fail "volume stats of $2 on $1: inodes used '$inodes'"
  echo "ok: kubelet on $1 reports $2 used=$used capacity=$cap inodes_used=$inodes"
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

  log "metrics on $n2"
  check_metrics "$n2" "$pv"
  check_volume_stats "$n2" fs-data $((12 << 20)) # a.bin + b.bin
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
