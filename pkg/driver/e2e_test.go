package driver

import (
	"bytes"
	"context"
	"errors"
	"math/rand"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tsuzu/hermit-csi/pkg/backup"
	"github.com/tsuzu/hermit-csi/pkg/manifest"
	"github.com/tsuzu/hermit-csi/pkg/store"
)

const (
	testChunk = 1 << 20
	testSize  = 16 << 20
)

var blockCap = &csi.VolumeCapability{
	AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
	AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
}

type cluster struct {
	t     *testing.T
	s     store.ObjectStore
	ctl   *Controller
	nodes map[string]*Node
	vgs   map[string]thinVG
	fakes map[string]*fakeVG           // nil entries for real LVM
	vctx  map[string]map[string]string // volume context from CreateVolume, as kept in the PV
}

// newCluster builds a controller and nodes backed by fake LVM and in-memory S3.
func newCluster(t *testing.T, nodeIDs ...string) *cluster {
	t.Helper()
	fakes := map[string]*fakeVG{}
	c := newClusterWith(t, store.NewMem(), func(id string) thinVG {
		f := newFakeVG(t.TempDir())
		fakes[id] = f
		return f
	}, nodeIDs...)
	c.fakes = fakes
	return c
}

func newClusterWith(t *testing.T, s store.ObjectStore, mkVG func(node string) thinVG, nodeIDs ...string) *cluster {
	t.Helper()
	c := &cluster{t: t, s: s, nodes: map[string]*Node{}, vgs: map[string]thinVG{}, fakes: map[string]*fakeVG{}, vctx: map[string]map[string]string{}}
	base := Config{Name: DefaultName, Store: c.s, ChunkSize: testChunk, Parallel: 4, GCGrace: time.Hour, AgentToken: "secret"}
	c.ctl = NewController(base)
	for _, id := range nodeIDs {
		cfg := base
		cfg.NodeID = id
		cfg.StateDir = t.TempDir()
		vg := mkVG(id)
		n := newNode(cfg, vg)
		n.freeze = nil
		// Serve the snapshot agent so the controller can reach it.
		srv := httptest.NewServer(n.agentHandler())
		t.Cleanup(srv.Close)
		n.cfg.AgentAdvertise = strings.TrimPrefix(srv.URL, "http://")
		c.nodes[id], c.vgs[id] = n, vg
	}
	return c
}

func (c *cluster) createVolume(name string, src *csi.VolumeContentSource) *csi.Volume {
	c.t.Helper()
	return c.createVolumeWith(name, src, nil)
}

// createVolumeWith passes StorageClass parameters.
func (c *cluster) createVolumeWith(name string, src *csi.VolumeContentSource, params map[string]string) *csi.Volume {
	c.t.Helper()
	resp, err := c.ctl.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:                name,
		CapacityRange:       &csi.CapacityRange{RequiredBytes: testSize},
		VolumeCapabilities:  []*csi.VolumeCapability{blockCap},
		VolumeContentSource: src,
		Parameters:          params,
	})
	if err != nil {
		c.t.Fatalf("CreateVolume: %v", err)
	}
	c.vctx[resp.Volume.VolumeId] = resp.Volume.VolumeContext
	return resp.Volume
}

// attach = ControllerPublish + NodeStage (what Kubernetes does on pod start).
func (c *cluster) attach(vol, node string) {
	c.t.Helper()
	ctx := context.Background()
	pub, err := c.ctl.ControllerPublishVolume(ctx, &csi.ControllerPublishVolumeRequest{
		VolumeId: vol, NodeId: node, VolumeCapability: blockCap,
	})
	if err != nil {
		c.t.Fatalf("ControllerPublish %s -> %s: %v", vol, node, err)
	}
	if _, err := c.nodes[node].NodeStageVolume(ctx, &csi.NodeStageVolumeRequest{
		VolumeId: vol, StagingTargetPath: c.t.TempDir(), VolumeCapability: blockCap,
		PublishContext: pub.PublishContext, VolumeContext: c.vctx[vol],
	}); err != nil {
		c.t.Fatalf("NodeStage %s on %s: %v", vol, node, err)
	}
}

// detach = NodeUnstage + ControllerUnpublish (graceful pod stop).
func (c *cluster) detach(vol, node string) {
	c.t.Helper()
	ctx := context.Background()
	if _, err := c.nodes[node].NodeUnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{VolumeId: vol}); err != nil {
		c.t.Fatalf("NodeUnstage: %v", err)
	}
	c.forceDetach(vol, node)
}

func (c *cluster) forceDetach(vol, node string) {
	c.t.Helper()
	if _, err := c.ctl.ControllerUnpublishVolume(context.Background(), &csi.ControllerUnpublishVolumeRequest{
		VolumeId: vol, NodeId: node,
	}); err != nil {
		c.t.Fatalf("ControllerUnpublish: %v", err)
	}
}

func (c *cluster) write(node, vol string, off int64, n int, seed int64) {
	c.t.Helper()
	buf := make([]byte, n)
	rand.New(rand.NewSource(seed)).Read(buf)
	f, err := os.OpenFile(c.vgs[node].DevicePath(vol), os.O_WRONLY, 0)
	if err != nil {
		c.t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteAt(buf, off); err != nil {
		c.t.Fatal(err)
	}
	if err := f.Sync(); err != nil { // flush page cache before LVM snapshots
		c.t.Fatal(err)
	}
}

// calls returns the fake LVM call count, or -1 when running on real LVM.
func (c *cluster) calls(node, op string) int {
	if f := c.fakes[node]; f != nil {
		return f.calls[op]
	}
	return -1
}

func (c *cluster) read(node, vol string) []byte {
	c.t.Helper()
	b, err := os.ReadFile(c.vgs[node].DevicePath(vol))
	if err != nil {
		c.t.Fatal(err)
	}
	return b
}

func (c *cluster) chunkCount() int {
	n := 0
	_ = c.s.List(context.Background(), manifest.ChunkPrefix, func(store.ObjectInfo) error { n++; return nil })
	return n
}

func (c *cluster) latest(vol string) *manifest.Manifest {
	c.t.Helper()
	m, _, err := manifest.Load(context.Background(), c.s, manifest.LatestKey(vol))
	if err != nil {
		c.t.Fatal(err)
	}
	return m
}

func TestMoveVolumeBetweenNodes(t *testing.T) { runMoveScenario(t, newCluster(t, "node-a", "node-b")) }
func TestReattachSameNodeSkipsRestore(t *testing.T) {
	runReattachScenario(t, newCluster(t, "node-a"))
}
func TestFencingAfterForceDetach(t *testing.T) {
	runFencingScenario(t, newCluster(t, "node-a", "node-b"))
}
func TestSnapshotsAndRestore(t *testing.T) { runSnapshotScenario(t, newCluster(t, "node-a", "node-b")) }

func runMoveScenario(t *testing.T, c *cluster) {
	ctx := context.Background()
	vol := c.createVolume("pvc-move", nil).VolumeId

	c.attach(vol, "node-a")
	if n := c.calls("node-a", "create"); n != -1 && n != 1 {
		t.Fatal("LV should be created on node-a")
	}

	// Write into 3 chunks (0, 1 and 10) and back up.
	c.write("node-a", vol, 0, testChunk+100, 1)
	c.write("node-a", vol, 10*testChunk+5, 10, 2)
	if _, err := c.nodes["node-a"].BackupVolume(ctx, vol); err != nil {
		t.Fatal(err)
	}
	if got := c.chunkCount(); got != 3 {
		t.Fatalf("uploaded %d chunks, want 3", got)
	}

	// A small write uploads exactly one more chunk, not the whole volume.
	c.write("node-a", vol, 7*testChunk, 4096, 3)
	if _, err := c.nodes["node-a"].BackupVolume(ctx, vol); err != nil {
		t.Fatal(err)
	}
	if got := c.chunkCount(); got != 4 {
		t.Fatalf("after incremental: %d chunks, want 4", got)
	}
	// No change => no new generation.
	gen := c.latest(vol).Generation
	if _, err := c.nodes["node-a"].BackupVolume(ctx, vol); err != nil {
		t.Fatal(err)
	}
	if c.latest(vol).Generation != gen {
		t.Fatal("backup without changes must not bump generation")
	}

	// Unflushed write, then graceful move: NodeUnstage flushes it.
	c.write("node-a", vol, 12*testChunk, 512, 4)
	want := c.read("node-a", vol)
	c.detach(vol, "node-a")

	// Publishing to another node while attached elsewhere must fail.
	c.attach(vol, "node-b")
	if _, err := c.ctl.ControllerPublishVolume(ctx, &csi.ControllerPublishVolumeRequest{VolumeId: vol, NodeId: "node-a"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("double attach: got %v, want FailedPrecondition", err)
	}
	if got := c.read("node-b", vol); !bytes.Equal(got, want) {
		t.Fatal("node-b content differs from node-a")
	}

	// After restore, node-b has a base snapshot: the next backup is incremental
	// (thin_delta), not a full rescan.
	before := c.chunkCount()
	c.write("node-b", vol, 3*testChunk, 100, 5)
	if _, err := c.nodes["node-b"].BackupVolume(ctx, vol); err != nil {
		t.Fatal(err)
	}
	if c.calls("node-b", "dump") > 0 {
		t.Fatal("node-b should not need a full rescan after restore")
	}
	if got := c.chunkCount() - before; got != 1 {
		t.Fatalf("node-b incremental uploaded %d chunks, want 1", got)
	}

	// Moving back to node-a: its local copy is stale and gets replaced.
	want = c.read("node-b", vol)
	c.detach(vol, "node-b")
	c.attach(vol, "node-a")
	if got := c.read("node-a", vol); !bytes.Equal(got, want) {
		t.Fatal("node-a content after move-back differs")
	}
}

func runReattachScenario(t *testing.T, c *cluster) {
	vol := c.createVolume("pvc-reuse", nil).VolumeId
	c.attach(vol, "node-a")
	c.write("node-a", vol, 0, 1000, 1)
	c.detach(vol, "node-a")
	before, _ := c.vgs["node-a"].Get(context.Background(), vol)
	creates := c.calls("node-a", "create")
	c.attach(vol, "node-a")
	after, _ := c.vgs["node-a"].Get(context.Background(), vol)
	if c.calls("node-a", "create") != creates || before == nil || after == nil || before.ThinID != after.ThinID {
		t.Fatal("re-attach on the same node should reuse the local LV")
	}
}

func runFencingScenario(t *testing.T, c *cluster) {
	ctx := context.Background()
	vol := c.createVolume("pvc-fence", nil).VolumeId

	c.attach(vol, "node-a")
	c.write("node-a", vol, 0, 100, 1)
	if _, err := c.nodes["node-a"].BackupVolume(ctx, vol); err != nil {
		t.Fatal(err)
	}
	backedUp := c.read("node-a", vol)

	// node-a becomes unreachable; Kubernetes force-detaches and moves the pod.
	c.write("node-a", vol, testChunk, 100, 2) // lost: never backed up
	c.forceDetach(vol, "node-a")
	c.attach(vol, "node-b")
	if got := c.read("node-b", vol); !bytes.Equal(got, backedUp) {
		t.Fatal("node-b should get the last backup")
	}

	// node-a comes back as a zombie and tries to back up: it must be fenced.
	_, err := c.nodes["node-a"].BackupVolume(ctx, vol)
	if !errors.Is(err, ErrFenced) {
		t.Fatalf("zombie backup: got %v, want ErrFenced", err)
	}
	if got := c.latest(vol); got.Epoch != 3 {
		t.Fatalf("epoch = %d, want 3", got.Epoch)
	}
	// node-b keeps working.
	c.write("node-b", vol, 2*testChunk, 100, 3)
	if _, err := c.nodes["node-b"].BackupVolume(ctx, vol); err != nil {
		t.Fatal(err)
	}
	// The zombie's unstage succeeds without writing.
	if _, err := c.nodes["node-a"].NodeUnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{VolumeId: vol}); err != nil {
		t.Fatal(err)
	}
}

func runSnapshotScenario(t *testing.T, c *cluster) {
	ctx := context.Background()
	vol := c.createVolume("pvc-snap", nil).VolumeId

	// In-use snapshot goes through the node agent.
	c.attach(vol, "node-a")
	c.write("node-a", vol, 0, 3*testChunk, 1)
	atSnap := c.read("node-a", vol)
	sresp, err := c.ctl.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{Name: "snap-1", SourceVolumeId: vol})
	if err != nil {
		t.Fatal(err)
	}
	if !sresp.Snapshot.ReadyToUse || sresp.Snapshot.SourceVolumeId != vol {
		t.Fatalf("bad snapshot %+v", sresp.Snapshot)
	}
	// Idempotent.
	if _, err := c.ctl.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{Name: "snap-1", SourceVolumeId: vol}); err != nil {
		t.Fatal(err)
	}

	// Changes after the snapshot do not affect it.
	c.write("node-a", vol, 0, 100, 2)
	c.detach(vol, "node-a")

	// Detached snapshot is just a manifest copy.
	if _, err := c.ctl.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{Name: "snap-2", SourceVolumeId: vol}); err != nil {
		t.Fatal(err)
	}
	list, err := c.ctl.ListSnapshots(ctx, &csi.ListSnapshotsRequest{SourceVolumeId: vol})
	if err != nil || len(list.Entries) != 2 {
		t.Fatalf("ListSnapshots = %v, %v", list, err)
	}

	// New volume from snap-1 on another node has exactly the snapshot content.
	before := c.chunkCount()
	restored := c.createVolume("pvc-from-snap", &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{
		Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: "snap-1"},
	}}).VolumeId
	if c.chunkCount() != before {
		t.Fatal("creating a volume from a snapshot must not copy chunks")
	}
	c.attach(restored, "node-b")
	if got := c.read("node-b", restored); !bytes.Equal(got, atSnap) {
		t.Fatal("volume from snapshot differs")
	}

	// GC: with the source volume and snap-2 gone, snap-1 still protects its chunks.
	if _, err := c.ctl.DeleteSnapshot(ctx, &csi.DeleteSnapshotRequest{SnapshotId: "snap-2"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ctl.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: vol}); err != nil {
		t.Fatal(err)
	}
	if _, err := backup.GC(ctx, c.s, 0, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	c.detach(restored, "node-b")
	c.attach(restored, "node-a")
	if got := c.read("node-a", restored); !bytes.Equal(got, atSnap) {
		t.Fatal("chunks referenced by a snapshot were garbage-collected")
	}

	// node-a still has a local copy of the deleted volume; cleanup removes it.
	if lv, _ := c.vgs["node-a"].Get(ctx, vol); lv == nil {
		t.Fatal("expected leftover LV before cleanup")
	}
	c.nodes["node-a"].cleanupOrphans(ctx)
	if lv, _ := c.vgs["node-a"].Get(ctx, vol); lv != nil {
		t.Fatal("orphan LV of deleted volume was not removed")
	}
}

func TestDeleteAttachedVolumeFails(t *testing.T) {
	c := newCluster(t, "node-a")
	vol := c.createVolume("pvc-del", nil).VolumeId
	c.attach(vol, "node-a")
	_, err := c.ctl.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: vol})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("got %v, want FailedPrecondition", err)
	}
}

func TestCreateVolumeIdempotentAndAligned(t *testing.T) {
	c := newCluster(t)
	ctx := context.Background()
	req := &csi.CreateVolumeRequest{
		Name: "pvc-idem", CapacityRange: &csi.CapacityRange{RequiredBytes: 5<<20 + 1},
		VolumeCapabilities: []*csi.VolumeCapability{blockCap},
	}
	a, err := c.ctl.CreateVolume(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if a.Volume.CapacityBytes != 8<<20 {
		t.Fatalf("capacity = %d, want 8MiB (4MiB aligned)", a.Volume.CapacityBytes)
	}
	if _, err := c.ctl.CreateVolume(ctx, req); err != nil {
		t.Fatalf("second CreateVolume: %v", err)
	}
	req.CapacityRange.RequiredBytes = 100 << 20
	if _, err := c.ctl.CreateVolume(ctx, req); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("size mismatch: got %v, want AlreadyExists", err)
	}
}

func TestAgentRejectsBadToken(t *testing.T) {
	c := newCluster(t, "node-a")
	c.nodes["node-a"].cfg.AgentToken = "other"
	vol := c.createVolume("pvc-tok", nil).VolumeId
	c.attach(vol, "node-a")
	_, err := c.ctl.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{Name: "s", SourceVolumeId: vol})
	if err == nil || !strings.Contains(err.Error(), "unauthorized") {
		t.Fatalf("got %v, want unauthorized", err)
	}
}
