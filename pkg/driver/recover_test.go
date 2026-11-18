package driver

import (
	"bytes"
	"context"
	"errors"
	"os"
	"sort"
	"sync/atomic"
	"testing"

	"github.com/tsuzu/hermit-csi/pkg/manifest"
	"github.com/tsuzu/hermit-csi/pkg/store"
)

// localLVs lists the volume LV and its snapshots on a node.
func (c *cluster) localLVs(node, vol string) []string {
	c.t.Helper()
	lvs, err := c.vgs[node].List(context.Background())
	if err != nil {
		c.t.Fatal(err)
	}
	var out []string
	for _, lv := range lvs {
		if lv.Name == vol || isSnapOf(vol, lv.Name) {
			out = append(out, lv.Name)
		}
	}
	sort.Strings(out)
	return out
}

func (c *cluster) expectLVs(node, vol string, want ...string) {
	c.t.Helper()
	sort.Strings(want)
	if got := c.localLVs(node, vol); !equalStrings(got, want) {
		c.t.Fatalf("LVs of %s on %s = %v, want %v", vol, node, got, want)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestCrashAfterManifestWrite: the node dies after latest.json is written but
// before its state file is saved. On restart it must recognize the write as its
// own instead of fencing itself, and remove the snapshots the crash left behind.
func TestCrashAfterManifestWrite(t *testing.T) {
	ctx := context.Background()
	c := newCluster(t, "node-a", "node-b")
	n := c.nodes["node-a"]
	vol := c.createVolume("pvc-crash", nil).VolumeId
	c.attach(vol, "node-a")

	c.write("node-a", vol, 0, 4096, 1)
	if _, err := n.BackupVolume(ctx, vol); err != nil {
		t.Fatal(err)
	}
	gen := c.latest(vol).Generation
	oldBase := snapName(vol, gen)
	stateFile := n.states.path(vol)
	saved, err := os.ReadFile(stateFile)
	if err != nil {
		t.Fatal(err)
	}

	// Back up gen+1, then roll the node back to the moment of the crash: the
	// state file still describes gen and the old base snapshot still exists.
	c.write("node-a", vol, 5*testChunk, 4096, 2)
	if _, err := n.BackupVolume(ctx, vol); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stateFile, saved, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := c.vgs["node-a"].Snapshot(ctx, vol, oldBase); err != nil {
		t.Fatal(err)
	}
	c.expectLVs("node-a", vol, vol, oldBase, snapName(vol, gen+1))

	// kubelet re-stages after the plugin restarts: the local LV is reused, not
	// overwritten by a restore.
	creates := c.calls("node-a", "create")
	c.attach(vol, "node-a")
	if c.calls("node-a", "create") != creates {
		t.Fatal("re-stage after the crash restored over the local LV")
	}
	c.expectLVs("node-a", vol, vol, snapName(vol, gen+1))

	// The next backup is incremental from the adopted snapshot.
	before := c.chunkCount()
	c.write("node-a", vol, 9*testChunk, 4096, 3)
	if _, err := n.BackupVolume(ctx, vol); err != nil {
		t.Fatalf("backup after crash: %v", err)
	}
	if got := c.latest(vol).Generation; got != gen+2 {
		t.Fatalf("generation = %d, want %d", got, gen+2)
	}
	if c.calls("node-a", "dump") > 0 {
		t.Fatal("backup after crash should not need a full rescan")
	}
	if got := c.chunkCount() - before; got != 1 {
		t.Fatalf("uploaded %d chunks, want 1", got)
	}
	c.expectLVs("node-a", vol, vol, snapName(vol, gen+2))

	want := c.read("node-a", vol)
	c.detach(vol, "node-a")
	c.attach(vol, "node-b")
	if !bytes.Equal(c.read("node-b", vol), want) {
		t.Fatal("node-b content differs from node-a")
	}
}

// lostAckStore applies a latest.json PUT but reports it as failed, like a
// request that timed out after S3 committed it.
type lostAckStore struct {
	store.ObjectStore
	armed atomic.Bool
}

func (s *lostAckStore) Put(ctx context.Context, k string, d []byte, o store.PutOpts) (string, error) {
	etag, err := s.ObjectStore.Put(ctx, k, d, o)
	if err == nil && manifest.IsManifestKey(k) && s.armed.CompareAndSwap(true, false) {
		return "", errors.New("injected timeout after commit")
	}
	return etag, err
}

// TestLostManifestAck: a backup whose latest.json PUT succeeded but looked
// failed. Its snapshot was cleaned up; the next backup must adopt the
// manifest, diff from the old base, and lose no data.
func TestLostManifestAck(t *testing.T) {
	ctx := context.Background()
	s := &lostAckStore{ObjectStore: store.NewMem()}
	fakes := map[string]*fakeVG{}
	c := newClusterWith(t, s, func(id string) thinVG {
		f := newFakeVG(t.TempDir())
		fakes[id] = f
		return f
	}, "node-a", "node-b")
	c.fakes = fakes
	n := c.nodes["node-a"]
	vol := c.createVolume("pvc-lostack", nil).VolumeId
	c.attach(vol, "node-a")
	gen := c.latest(vol).Generation

	c.write("node-a", vol, 0, 4096, 1)
	s.armed.Store(true)
	if _, err := n.BackupVolume(ctx, vol); err == nil {
		t.Fatal("expected the injected failure")
	}
	if got := c.latest(vol).Generation; got != gen+1 {
		t.Fatalf("generation = %d, want %d (the PUT was applied)", got, gen+1)
	}
	c.expectLVs("node-a", vol, vol, snapName(vol, gen))

	c.write("node-a", vol, 6*testChunk, 4096, 2)
	if _, err := n.BackupVolume(ctx, vol); err != nil {
		t.Fatalf("backup after lost ack: %v", err)
	}
	if got := c.latest(vol).Generation; got != gen+2 {
		t.Fatalf("generation = %d, want %d", got, gen+2)
	}
	if c.calls("node-a", "dump") > 0 {
		t.Fatal("the old base is still valid; no full rescan needed")
	}
	c.expectLVs("node-a", vol, vol, snapName(vol, gen+2))

	want := c.read("node-a", vol)
	c.detach(vol, "node-a")
	c.attach(vol, "node-b")
	if !bytes.Equal(c.read("node-b", vol), want) {
		t.Fatal("node-b content differs from node-a")
	}
}
