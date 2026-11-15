# hermit-csi

A CSI driver that uses local LVM thin volumes as fast local disks, continuously backs up **only the changes** to S3,
and automatically restores a volume from S3 when it is mounted on a different node. Both Block and Filesystem modes are supported.

## How it works

```
chunks/<sha256>              chunks shared by all volumes (deduplicated by content hash)
volumes/<id>/latest.json.gz  latest manifest (chunk index -> hash), overwritten on every backup
volumes/<id>/owner.json      attached node and ownership epoch
snapshots/<id>.json.gz       manifest as of a VolumeSnapshot (immutable)
```

| Operation | What happens |
|---|---|
| CreateVolume | Writes an all-zero manifest (no data transfer). Creating from a snapshot or cloning a volume only copies the manifest |
| ControllerPublish | Points `owner.json` at the new node and bumps the epoch. Also writes the epoch into `latest.json`, which changes its ETag, so the old node's conditional writes (If-Match) fail (fencing) |
| NodeStage | If the local LV's generation matches `latest.json`, uses it as is. Otherwise recreates the thin LV, restores it from S3 in parallel, and takes a base snapshot right after |
| Periodic backup | At each volume's interval (see below): thin snapshot -> `thin_delta` against the previous base for the changed blocks -> read only those chunks, PUT only hashes S3 does not have yet -> replace `latest.json` with If-Match |
| NodeUnstage | Final backup after unmounting. The LV is kept (no restore needed if the volume comes back to the same node) |
| ControllerUnpublish | Releases ownership and bumps the epoch (a force detach after a node failure also locks out the old node) |
| CreateSnapshot | Detached: a copy of `latest.json`. In use: asks the owning node's agent to upload the changes up to that point, then freezes the manifest |
| GC (controller) | Deletes chunks that no manifest references and that are older than the grace period (default 24h) |

There are no full uploads. The first backup reads only the allocated blocks (`thin_dump`) and skips zero chunks.
If the base snapshot is lost, the node rereads the local volume and uploads only the chunks whose hash changed.

Consistency:
- Filesystem mode runs `fsfreeze` before each snapshot, so backups are filesystem-consistent
- Block mode is crash-consistent (equivalent to a power loss)
- **If a node dies suddenly**, writes since the last backup are lost (RPO = the volume's backup interval)

### Backup interval

The periodic backup interval can be set per volume with the StorageClass parameter `parameters.backupInterval` (Go duration, at least 10s).
Volumes without it use the node's `--backup-interval` (default 5m; `0` disables periodic backups for them).

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: hermit-1m
provisioner: hermit.csi.tsuzu.dev
volumeBindingMode: WaitForFirstConsumer
parameters:
  backupInterval: 1m
```

- The value is recorded in the PV's volume context at CreateVolume and passed to the node on every NodeStage. PVs are immutable, so changing the StorageClass later does not affect existing volumes
- The node wakes up every 10s and backs up, one at a time, the volumes whose interval has elapsed since their last periodic backup started. A failed backup is retried one interval later
- Right after NodeStage the volume matches S3, so the first periodic backup happens one interval later. After a node plugin restart, every volume is backed up on the first round
- Backups run one at a time, so with many volumes or large changes the actual interval can be longer than configured

## Scheduler plugin (prefer nodes that already hold a local copy)

When a volume is staged on a different node, the pod does not start until the restore from S3 finishes, so moving the pod back to the node that last used the volume makes it start much faster.
`scheduler/` is a second scheduler: kube-scheduler with the score plugin `HermitLocality` compiled in.

- On ControllerUnpublish, the controller records the node it detached from as `lastNode` in `owner.json` (no extra S3 write)
- In PreScore, the plugin reads `owner.json` for each bound hermit PVC of the pod and treats the attached node, or `lastNode` after a detach, as "the node with a local copy"
- The score is 100 x the share of the pod's hermit volume capacity whose copy is on that node. The plugin never filters, so if that node cannot take the pod it goes elsewhere as usual (and the volume is restored there)
- Unbound claims (new WaitForFirstConsumer volumes), volumes of other drivers, and missing or unreadable owner records give no preference. S3 reads are cut off after 2s per scheduling cycle by default

Install it with the release's `hermit-scheduler.yaml` (or `kubectl apply -k deploy/scheduler/`). It shares the driver's S3 settings and credentials (ConfigMap / Secret `hermit-csi`).

Pods opt in with `spec.schedulerName: hermit-scheduler` (for a StatefulSet, in its pod template).
The default scheduler keeps running. The plugin weight (default 10) can be tuned in `deploy/scheduler/config.yaml`.

## Layout

```
cmd/hermit-csi/     entry point (--mode=controller|node)
pkg/store/         S3 wrapper (conditional writes) and an in-memory implementation for tests
pkg/manifest/      S3 layout and manifests
pkg/backup/        incremental backup / parallel restore / GC (LVM-independent)
pkg/lvm/           wrappers for lvcreate / thin_delta / thin_dump and their XML parser
pkg/driver/        CSI Identity / Controller / Node, backup loop, snapshot agent
deploy/            CSIDriver, RBAC, controller Deployment, node DaemonSet, StorageClass
hack/it-setup.sh   creates thin pools on loop devices for the integration tests
scheduler/         HermitLocality scheduler plugin (separate Go module, depends on k8s.io/kubernetes)
deploy/scheduler/  hermit-scheduler configuration, RBAC, Deployment
test/e2e/          kind end-to-end test (run.sh, cluster config, kustomize overlay)
```

## Tests

```sh
go test -race ./...                      # unit + E2E (fake LVM and in-memory S3)
(cd scheduler && go test -race ./...)    # scheduler plugin
```

The E2E tests call the CSI RPCs in the order Kubernetes does and check that:
- data matches after moving a volume between nodes
- a small write uploads only one chunk
- the backup after a restore is incremental
- a zombie node is fenced after a force detach
- in-use snapshots work, and volumes can be restored from snapshots
- GC keeps chunks referenced by snapshots

Integration tests (root, real LVM, MinIO):

They run the E2E scenarios above on real LVM, and also test Filesystem mode (ext4 / xfs):
formatting and mounting in NodeStage, mounting a restored volume without reformatting it, `fsfreeze` during backups of a mounted filesystem (writes that were never fsynced are included),
unmounting on a fenced old node, and creating volumes from in-use snapshots. They need `mkfs.ext4`, `mkfs.xfs` and `fsfreeze`.

```sh
# Open-source MinIO is archived and no images or binaries are published anymore, so build the last release from source
git clone --depth 1 --branch RELEASE.2025-10-15T17-29-55Z https://github.com/minio/minio.git /tmp/minio-src
(cd /tmp/minio-src && CGO_ENABLED=0 go build -o /tmp/minio .)
MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin /tmp/minio server /tmp/minio-data --address 127.0.0.1:9000 &
aws --endpoint-url http://127.0.0.1:9000 s3 mb s3://hermit-it
sudo hack/it-setup.sh up
export HERMIT_IT_VG_A=it-a HERMIT_IT_VG_B=it-b HERMIT_IT_S3_ENDPOINT=http://127.0.0.1:9000 \
       HERMIT_IT_BUCKET=hermit-it AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin
go test -c -tags integration -o driver.test ./pkg/driver/ && sudo -E ./driver.test -test.run Integration -test.v
sudo hack/it-setup.sh down
```

CI (`.github/workflows/ci.yaml`) runs these jobs on every push and pull request:
- **unit**: gofmt / vet / `-race` tests / coverage
- **integration**: integration tests on two thin pools on loop devices, with MinIO
- **scheduler**: vet / tests of the scheduler plugin (runs the real kube-scheduler against a fake API server and checks where pods are bound)
- **e2e**: installs deploy/ and deploy/scheduler/ into a kind cluster (two workers, thin pools on loop devices) and runs them with real kubelets, CSI sidecars, snapshot-controller and hermit-scheduler (`test/e2e/run.sh`).
  It checks moving Filesystem / Block volumes between nodes with a restore, returning to the node with the local copy even when the other node is more attractive resource-wise, restoring from an in-use snapshot, and deletion
- **image**: builds the container images (driver and scheduler)

## Deploy

1. On each node, create the VG `hermit` with the thin pool `pool`, and label the node `hermit.csi.tsuzu.dev/enabled=true`.
   Containers cannot modprobe, so also load the kernel modules `dm_thin_pool` and `dm_snapshot` on the node (without them, the node plugin exits at startup with an error naming them).
   ```sh
   vgcreate hermit /dev/nvme1n1
   lvcreate -l 95%FREE --chunksize 64k -T hermit/pool
   printf 'dm_thin_pool\ndm_snapshot\n' > /etc/modules-load.d/hermit.conf && systemctl restart systemd-modules-load
   ```
2. To use snapshots, install the snapshot CRDs and snapshot-controller.
3. Edit and apply the S3 settings and credentials (ConfigMap / Secret `hermit-csi`), then install the driver.
   Release manifests pin that release's images by `tag@digest`.
   ```sh
   V=v0.1.0   # https://github.com/tsuzu/hermit-csi/releases
   curl -fsSLO https://github.com/tsuzu/hermit-csi/releases/download/$V/hermit-csi-config.yaml
   $EDITOR hermit-csi-config.yaml && kubectl apply -f hermit-csi-config.yaml
   kubectl apply -f https://github.com/tsuzu/hermit-csi/releases/download/$V/hermit-csi.yaml
   # Optional: the scheduler that prefers nodes with a local copy (see above)
   kubectl apply -f https://github.com/tsuzu/hermit-csi/releases/download/$V/hermit-scheduler.yaml
   ```
   From the repository: `kubectl apply -f deploy/secret.yaml` (after editing it), `kubectl apply -k deploy/` and `kubectl apply -k deploy/scheduler/` (images tagged `latest`).
4. See `examples/block-pvc.yaml` for an example.

The images are `ghcr.io/tsuzu/hermit-csi` and `ghcr.io/tsuzu/hermit-scheduler` (linux/amd64, linux/arm64).

### S3 credentials

The controller, node plugin and scheduler read S3 with one of the following (roles need v0.2.0 or later; `<version>` below is a release tag):

- **Static keys**: `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` in the `hermit-csi` Secret.
- **EKS IRSA** (IAM roles for service accounts): annotate the service accounts `hermit-csi-controller`,
  `hermit-csi-node` and `hermit-scheduler` (namespace `kube-system`) with `eks.amazonaws.com/role-arn`.
  The pod identity webhook injects `AWS_ROLE_ARN` / `AWS_WEB_IDENTITY_TOKEN_FILE`, and the driver assumes that
  role explicitly, so it is used even if static keys are still in the Secret.
  ```yaml
  # kustomization.yaml
  resources:
    - https://github.com/tsuzu/hermit-csi/releases/download/<version>/hermit-csi.yaml
  patches:
    - target: {kind: ServiceAccount, name: "hermit-csi-.*|hermit-scheduler"}
      patch: |-
        - {op: add, path: /metadata/annotations, value: {eks.amazonaws.com/role-arn: "arn:aws:iam::123456789012:role/hermit-csi"}}
  ```
- **EKS Pod Identity**: associate the role with the same service accounts. This goes through the SDK's default
  credential chain, where static keys take precedence, so remove `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`
  from the Secret.
- **Any cluster, web identity** (self-managed Kubernetes whose service account issuer is registered as an IAM
  OIDC provider, or an S3-compatible store with its own STS such as MinIO): add the kustomize component
  [`deploy/components/web-identity`](deploy/components/web-identity/kustomization.yaml), which projects a service
  account token with audience `sts.amazonaws.com`, and set `S3_ROLE_ARN` (plus `S3_STS_ENDPOINT` for a non-AWS
  STS) in the ConfigMap.
  ```yaml
  resources:
    - https://github.com/tsuzu/hermit-csi/releases/download/<version>/hermit-csi.yaml
  components:
    - https://github.com/tsuzu/hermit-csi/deploy/components/web-identity?ref=<version>
  ```

`S3_ROLE_ARN` without a token file assumes the role with whatever credentials the default chain finds (e.g. for a
bucket in another account). All settings are also flags: `--s3-role-arn`, `--s3-web-identity-token-file`,
`--s3-role-session-name`, `--sts-endpoint`. Temporary credentials are refreshed before they expire, rereading the
token file, so rotated projected tokens are picked up. The kind e2e test runs with web identity only (MinIO's STS
trusting the cluster's service account issuer) and no static keys at all.

Permissions on the bucket (and prefix): the controller and node plugin need `s3:GetObject`, `s3:PutObject`,
`s3:DeleteObject` and `s3:ListBucket`; the scheduler only needs `s3:GetObject` on `volumes/*/owner.json`.

### Releasing

Pushing a `vX.Y.Z` tag on a commit whose CI on `main` is green makes `.github/workflows/release.yaml` push both images to ghcr
as `X.Y.Z` / `X.Y` / `latest`, and create a GitHub release with manifests pinned to those images (`hermit-csi.yaml`, `hermit-scheduler.yaml`, `hermit-csi-config.yaml`).

## Known limitations / TODO

- The controller must run as a single replica (its locks are in-process)
- Volume expansion (ControllerExpandVolume) is not supported
- A pod stays in `ContainerCreating` until the full restore finishes. Lazy restore via ublk / NBD is a possible future extension
- The controller talks to node agents over HTTP with bearer-token authentication. Use a NetworkPolicy or mTLS in production
- dmeventd monitoring is disabled inside the container, so thin pool autoextend (`thin_pool_autoextend_threshold`) does not work. Monitor pool usage on the node
- Chunks are not compressed (the code is structured so that zstd compression is easy to add)
