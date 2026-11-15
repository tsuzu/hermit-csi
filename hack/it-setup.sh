#!/usr/bin/env bash
# Prepare two LVM thin pools on loop devices (one per simulated node).
# Usage: sudo hack/it-setup.sh [up|down]
set -euo pipefail

POOL=${S3LVM_IT_POOL:-pool}
VGS=(${S3LVM_IT_VG_A:-it-a} ${S3LVM_IT_VG_B:-it-b})
IMG_DIR=${IMG_DIR:-/var/tmp/s3lvm-it}

up() {
  modprobe dm_thin_pool
  mkdir -p "$IMG_DIR"
  for vg in "${VGS[@]}"; do
    img="$IMG_DIR/$vg.img"
    truncate -s 1G "$img"
    loop=$(losetup -f --show "$img")
    pvcreate -y "$loop"
    vgcreate "$vg" "$loop"
    # 64 KiB pool chunks (the default) => thin_delta granularity of 64 KiB.
    lvcreate -y -L 800M --chunksize 64k -T "$vg/$POOL"
  done
  vgs; lvs -a
}

down() {
  for vg in "${VGS[@]}"; do
    vgremove -ff -y "$vg" 2>/dev/null || true
    img="$IMG_DIR/$vg.img"
    for loop in $(losetup -j "$img" -O NAME --noheadings 2>/dev/null); do
      losetup -d "$loop"
    done
    rm -f "$img"
  done
}

"${1:-up}"
