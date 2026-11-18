package driver

import (
	"context"
	"errors"
	"io"
	"os"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NodeGetVolumeStats reports capacity and usage so kubelet can export
// kubelet_volume_stats_*.
//
// There is no volume condition (fenced, failing backups): CSI spec v1.13
// removed the alpha VolumeCondition field and the VOLUME_CONDITION
// capability. hermit_volume_fenced and
// hermit_volume_backup_consecutive_failures cover those cases.
//
// kubelet calls it for every volume about once a minute, so it only reads
// the local state file and the filesystem or device: no LVM command, no S3
// request and no per-volume lock (a running backup must not stall it; the
// state file is replaced atomically).
func (n *Node) NodeGetVolumeStats(_ context.Context, req *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	vol, path := req.GetVolumeId(), req.GetVolumePath()
	if err := checkID("volume id", vol); err != nil {
		return nil, err
	}
	if path == "" {
		return nil, status.Error(codes.InvalidArgument, "volume path missing")
	}
	st, err := n.states.load(vol)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if st == nil || !st.Staged {
		return nil, status.Errorf(codes.NotFound, "volume %s is not staged on this node", vol)
	}
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, status.Errorf(codes.NotFound, "volume path %s does not exist", path)
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	var usage []*csi.VolumeUsage
	if st.Block {
		// The LV device itself (kubelet's volume path is a bind mount of it).
		// Seeking to the end gives the size of a block device without an
		// ioctl or an LVM call.
		size, err := deviceSize(n.vg.DevicePath(vol))
		if err != nil {
			return nil, err
		}
		usage = []*csi.VolumeUsage{{Unit: csi.VolumeUsage_BYTES, Total: size}}
	} else {
		if usage, err = fsUsage(path); err != nil {
			return nil, err
		}
	}
	return &csi.NodeGetVolumeStatsResponse{Usage: usage}, nil
}

func deviceSize(dev string) (int64, error) {
	f, err := os.Open(dev)
	if errors.Is(err, os.ErrNotExist) {
		return 0, status.Errorf(codes.NotFound, "device %s does not exist", dev)
	}
	if err != nil {
		return 0, status.Error(codes.Internal, err.Error())
	}
	defer f.Close()
	size, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, status.Errorf(codes.Internal, "size of %s: %v", dev, err)
	}
	return size, nil
}

func fsUsage(path string) ([]*csi.VolumeUsage, error) {
	var s unix.Statfs_t
	if err := unix.Statfs(path, &s); err != nil {
		if errors.Is(err, unix.ENOENT) {
			return nil, status.Errorf(codes.NotFound, "volume path %s does not exist", path)
		}
		return nil, status.Errorf(codes.Internal, "statfs %s: %v", path, err)
	}
	bs := int64(s.Bsize)
	return []*csi.VolumeUsage{{
		Unit:      csi.VolumeUsage_BYTES,
		Total:     int64(s.Blocks) * bs,
		Available: int64(s.Bavail) * bs,
		Used:      int64(s.Blocks-s.Bfree) * bs,
	}, {
		Unit:      csi.VolumeUsage_INODES,
		Total:     int64(s.Files),
		Available: int64(s.Ffree),
		Used:      int64(s.Files - s.Ffree),
	}}, nil
}
