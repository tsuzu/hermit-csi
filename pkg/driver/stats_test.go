package driver

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNodeAdvertisesVolumeStats(t *testing.T) {
	c := newCluster(t, "node-a")
	resp, err := c.nodes["node-a"].NodeGetCapabilities(context.Background(), &csi.NodeGetCapabilitiesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	got := map[csi.NodeServiceCapability_RPC_Type]bool{}
	for _, cp := range resp.GetCapabilities() {
		got[cp.GetRpc().GetType()] = true
	}
	for _, want := range []csi.NodeServiceCapability_RPC_Type{
		csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
		csi.NodeServiceCapability_RPC_GET_VOLUME_STATS,
	} {
		if !got[want] {
			t.Errorf("capability %s not advertised (got %v)", want, got)
		}
	}
}

func usageOf(t *testing.T, resp *csi.NodeGetVolumeStatsResponse, unit csi.VolumeUsage_Unit) *csi.VolumeUsage {
	t.Helper()
	for _, u := range resp.GetUsage() {
		if u.GetUnit() == unit {
			return u
		}
	}
	return nil
}

func expectCode(t *testing.T, what string, err error, want codes.Code) {
	t.Helper()
	if got := status.Code(err); got != want {
		t.Fatalf("%s: code %s (err %v), want %s", what, got, err, want)
	}
}

// TestNodeGetVolumeStats checks validation, NotFound cases, the block size
// (from the device, without LVM calls) and the filesystem usage via statfs.
func TestNodeGetVolumeStats(t *testing.T) {
	ctx := context.Background()
	c := newCluster(t, "node-a")
	n := c.nodes["node-a"]
	stats := func(vol, path string) (*csi.NodeGetVolumeStatsResponse, error) {
		return n.NodeGetVolumeStats(ctx, &csi.NodeGetVolumeStatsRequest{VolumeId: vol, VolumePath: path})
	}

	_, err := stats("../etc", t.TempDir())
	expectCode(t, "invalid id", err, codes.InvalidArgument)
	_, err = stats("pvc-x", "")
	expectCode(t, "no path", err, codes.InvalidArgument)
	_, err = stats("pvc-unknown", t.TempDir())
	expectCode(t, "unknown volume", err, codes.NotFound)

	// Block: the total is the LV size, read from the device.
	blockVol := c.createVolume("pvc-stats-block", nil).VolumeId
	c.attach(blockVol, "node-a")
	c.write("node-a", blockVol, 0, 4096, 1)
	_, err = stats(blockVol, filepath.Join(t.TempDir(), "missing"))
	expectCode(t, "missing volume path", err, codes.NotFound)

	publish := filepath.Join(t.TempDir(), "dev") // kubelet passes the bind-mounted device file
	if err := os.WriteFile(publish, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	gets, lists := c.calls("node-a", "get"), c.calls("node-a", "list")
	resp, err := stats(blockVol, publish)
	if err != nil {
		t.Fatalf("block stats: %v", err)
	}
	if len(resp.GetUsage()) != 1 {
		t.Fatalf("block usage = %v, want BYTES only", resp.GetUsage())
	}
	if u := usageOf(t, resp, csi.VolumeUsage_BYTES); u == nil || u.Total != testSize || u.Used != 0 || u.Available != 0 {
		t.Fatalf("block BYTES = %v, want total %d only", u, testSize)
	}
	if c.calls("node-a", "get") != gets || c.calls("node-a", "list") != lists {
		t.Fatal("NodeGetVolumeStats called LVM; kubelet polls it for every volume")
	}

	// Filesystem: flip the staged volume to a mounted filesystem whose
	// staging path is a temp dir (the fake cluster stages block volumes).
	fsVol := c.createVolume("pvc-stats-fs", nil).VolumeId
	c.attach(fsVol, "node-a")
	st, err := n.states.load(fsVol)
	if err != nil {
		t.Fatal(err)
	}
	target := t.TempDir()
	st.Block, st.Staging = false, target
	if err := n.states.save(st); err != nil {
		t.Fatal(err)
	}
	resp, err = stats(fsVol, target)
	if err != nil {
		t.Fatalf("fs stats: %v", err)
	}
	b, in := usageOf(t, resp, csi.VolumeUsage_BYTES), usageOf(t, resp, csi.VolumeUsage_INODES)
	if b == nil || b.Total <= 0 || b.Used < 0 || b.Available < 0 || b.Used > b.Total || b.Available > b.Total {
		t.Fatalf("fs BYTES = %v", b)
	}
	if in == nil || in.Total <= 0 || in.Used < 0 || in.Available < 0 || in.Used > in.Total {
		t.Fatalf("fs INODES = %v", in)
	}
	if b.Total == testSize {
		t.Fatalf("fs stats reported the device size %d instead of statfs of %s", b.Total, target)
	}

	// Fenced (another node took the volume over): still staged here, so
	// stats still work.
	c.forceDetach(blockVol, "node-a")
	if _, err := n.BackupVolume(ctx, blockVol); !errors.Is(err, ErrFenced) {
		t.Fatalf("backup after takeover: %v, want fenced", err)
	}
	if _, err := stats(blockVol, publish); err != nil {
		t.Fatalf("stats of fenced volume: %v", err)
	}

	// Unstaged: no longer on this node.
	c.detach(fsVol, "node-a")
	_, err = stats(fsVol, target)
	expectCode(t, "unstaged volume", err, codes.NotFound)
	if _, err := n.NodeUnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{VolumeId: blockVol}); err != nil {
		t.Fatal(err)
	}
	_, err = stats(blockVol, publish)
	expectCode(t, "unstaged block volume", err, codes.NotFound)
}
