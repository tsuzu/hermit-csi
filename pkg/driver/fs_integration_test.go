//go:build integration

// Filesystem-mode integration tests: the volume is formatted and mounted by
// NodeStage, files are written through the NodePublish bind mount, and
// backups of the mounted filesystem go through fsfreeze. Needs root, real
// LVM, mkfs.<fstype> and fsfreeze (see realCluster).
//
// All "nodes" share one kernel, and XFS refuses to mount two filesystems with
// the same UUID at once. The scenario therefore never has two copies of the
// same filesystem mounted at the same time, which on real, separate nodes
// would be fine.
package driver

import (
	"bytes"
	"context"
	"errors"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	mount "k8s.io/mount-utils"
)

func fsCap(fsType string) *csi.VolumeCapability {
	return &csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{FsType: fsType}},
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
	}
}

// freezeLog wraps the real fsfreeze and records every call, so the test can
// check that backups of a mounted filesystem really froze it.
type freezeLog struct {
	mu    sync.Mutex
	calls []string // "freeze <path>" / "thaw <path>"
	errs  []error
}

func (l *freezeLog) wrap() func(ctx context.Context, path string, freeze bool) error {
	return func(ctx context.Context, path string, freeze bool) error {
		err := fsfreeze(ctx, path, freeze)
		l.mu.Lock()
		defer l.mu.Unlock()
		op := "thaw "
		if freeze {
			op = "freeze "
		}
		l.calls = append(l.calls, op+path)
		if err != nil {
			l.errs = append(l.errs, err)
		}
		return err
	}
}

// take returns and clears the recorded calls, failing on any fsfreeze error.
func (l *freezeLog) take(t *testing.T) []string {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.errs) > 0 {
		t.Fatalf("fsfreeze failed: %v", errors.Join(l.errs...))
	}
	c := l.calls
	l.calls = nil
	return c
}

// fsMount is one staged + published volume on one node.
type fsMount struct {
	node, vol       string
	staging, target string
}

type fsCluster struct {
	*cluster
	fsType string
	size   int64
	cap    *csi.VolumeCapability
	freeze *freezeLog
}

func realFSCluster(t *testing.T, fsType string, size int64) *fsCluster {
	t.Helper()
	for _, bin := range []string{"mkfs." + fsType, "fsfreeze", "fstrim", "blkid"} {
		if _, err := exec.LookPath(bin); err != nil {
			t.Skipf("%s not installed", bin)
		}
	}
	c := &fsCluster{cluster: realCluster(t), fsType: fsType, size: size, cap: fsCap(fsType), freeze: &freezeLog{}}
	for _, n := range c.nodes {
		n.freeze = c.freeze.wrap()
	}
	return c
}

func (c *fsCluster) createVolume(name string, src *csi.VolumeContentSource) string {
	c.t.Helper()
	resp, err := c.ctl.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:                name,
		CapacityRange:       &csi.CapacityRange{RequiredBytes: c.size},
		VolumeCapabilities:  []*csi.VolumeCapability{c.cap},
		VolumeContentSource: src,
	})
	if err != nil {
		c.t.Fatalf("CreateVolume: %v", err)
	}
	c.vctx[resp.Volume.VolumeId] = resp.Volume.VolumeContext
	return resp.Volume.VolumeId
}

// attach = ControllerPublish + NodeStage (format + mount) + NodePublish (bind mount).
func (c *fsCluster) attach(vol, node string) *fsMount {
	c.t.Helper()
	ctx := context.Background()
	dir := c.t.TempDir()
	m := &fsMount{node: node, vol: vol, staging: filepath.Join(dir, "staging"), target: filepath.Join(dir, "target")}
	// Registered after TempDir, so it runs first: never RemoveAll a mounted fs.
	c.t.Cleanup(func() {
		for _, p := range []string{m.target, m.staging} {
			_ = exec.Command("umount", "-l", p).Run()
		}
	})
	pub, err := c.ctl.ControllerPublishVolume(ctx, &csi.ControllerPublishVolumeRequest{
		VolumeId: vol, NodeId: node, VolumeCapability: c.cap,
	})
	if err != nil {
		c.t.Fatalf("ControllerPublish %s -> %s: %v", vol, node, err)
	}
	stage := &csi.NodeStageVolumeRequest{
		VolumeId: vol, StagingTargetPath: m.staging, VolumeCapability: c.cap, PublishContext: pub.PublishContext,
		VolumeContext: c.vctx[vol],
	}
	if _, err := c.nodes[node].NodeStageVolume(ctx, stage); err != nil {
		c.t.Fatalf("NodeStage %s on %s: %v", vol, node, err)
	}
	// kubelet retries NodeStage; a second call on a mounted staging path is a no-op.
	if _, err := c.nodes[node].NodeStageVolume(ctx, stage); err != nil {
		c.t.Fatalf("NodeStage retry %s on %s: %v", vol, node, err)
	}
	if _, err := c.nodes[node].NodePublishVolume(ctx, &csi.NodePublishVolumeRequest{
		VolumeId: vol, StagingTargetPath: m.staging, TargetPath: m.target, VolumeCapability: c.cap,
	}); err != nil {
		c.t.Fatalf("NodePublish %s on %s: %v", vol, node, err)
	}
	c.checkFSType(m)
	return m
}

func (c *fsCluster) checkFSType(m *fsMount) {
	c.t.Helper()
	out, err := exec.Command("blkid", "-o", "value", "-s", "TYPE", c.vgs[m.node].DevicePath(m.vol)).Output()
	if err != nil {
		c.t.Fatalf("blkid %s: %v", m.vol, err)
	}
	if got := string(bytes.TrimSpace(out)); got != c.fsType {
		c.t.Fatalf("%s on %s has fs %q, want %q", m.vol, m.node, got, c.fsType)
	}
}

// unmount = NodeUnpublish + NodeUnstage (unmount + final backup).
func (c *fsCluster) unmount(m *fsMount) {
	c.t.Helper()
	ctx := context.Background()
	if _, err := c.nodes[m.node].NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{
		VolumeId: m.vol, TargetPath: m.target,
	}); err != nil {
		c.t.Fatalf("NodeUnpublish: %v", err)
	}
	if _, err := c.nodes[m.node].NodeUnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{
		VolumeId: m.vol, StagingTargetPath: m.staging,
	}); err != nil {
		c.t.Fatalf("NodeUnstage: %v", err)
	}
	for _, p := range []string{m.staging, m.target} {
		if mnt, err := mount.New("").IsMountPoint(p); err == nil && mnt {
			c.t.Fatalf("%s is still mounted after unstage", p)
		}
	}
}

// detach = unmount + ControllerUnpublish (graceful pod stop).
func (c *fsCluster) detach(m *fsMount) {
	c.t.Helper()
	c.unmount(m)
	c.forceDetach(m.vol, m.node)
}

// writeFile writes random data through the pod-visible mount. Without
// fsync the data may still sit in the page cache when a backup runs.
func (c *fsCluster) writeFile(m *fsMount, name string, n int, seed int64, fsync bool) []byte {
	c.t.Helper()
	p := filepath.Join(m.target, name)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		c.t.Fatal(err)
	}
	buf := make([]byte, n)
	rand.New(rand.NewSource(seed)).Read(buf)
	f, err := os.Create(p)
	if err != nil {
		c.t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Write(buf); err != nil {
		c.t.Fatal(err)
	}
	if fsync {
		if err := f.Sync(); err != nil {
			c.t.Fatal(err)
		}
	}
	return buf
}

// expectFiles checks that the mount holds exactly the wanted content for the
// given names; a nil value means the file must not exist.
func (c *fsCluster) expectFiles(m *fsMount, want map[string][]byte) {
	c.t.Helper()
	for name, w := range want {
		got, err := os.ReadFile(filepath.Join(m.target, name))
		switch {
		case w == nil && !errors.Is(err, os.ErrNotExist):
			c.t.Errorf("%s on %s: %s should not exist (err=%v)", m.vol, m.node, name, err)
		case w != nil && err != nil:
			c.t.Errorf("%s on %s: read %s: %v", m.vol, m.node, name, err)
		case w != nil && !bytes.Equal(got, w):
			c.t.Errorf("%s on %s: %s differs (%d bytes, want %d)", m.vol, m.node, name, len(got), len(w))
		}
	}
	if c.t.Failed() {
		c.t.FailNow()
	}
}

func (c *fsCluster) backup(m *fsMount) {
	c.t.Helper()
	if _, err := c.nodes[m.node].BackupVolume(context.Background(), m.vol); err != nil {
		c.t.Fatalf("backup %s on %s: %v", m.vol, m.node, err)
	}
	// A mounted filesystem is frozen around the LVM snapshot, then thawed.
	calls := c.freeze.take(c.t)
	if len(calls) != 2 || calls[0] != "freeze "+m.staging || calls[1] != "thaw "+m.staging {
		c.t.Fatalf("fsfreeze calls = %q, want freeze+thaw of %s", calls, m.staging)
	}
}

// stats calls NodeGetVolumeStats on the pod-visible mount, as kubelet does,
// and checks the usage is consistent with a filesystem of about c.size.
func (c *fsCluster) stats(m *fsMount) (b, in *csi.VolumeUsage) {
	c.t.Helper()
	resp, err := c.nodes[m.node].NodeGetVolumeStats(context.Background(), &csi.NodeGetVolumeStatsRequest{
		VolumeId: m.vol, VolumePath: m.target, StagingTargetPath: m.staging,
	})
	if err != nil {
		c.t.Fatalf("NodeGetVolumeStats %s on %s: %v", m.vol, m.node, err)
	}
	for _, u := range resp.GetUsage() {
		switch u.GetUnit() {
		case csi.VolumeUsage_BYTES:
			b = u
		case csi.VolumeUsage_INODES:
			in = u
		}
	}
	if b == nil || in == nil {
		c.t.Fatalf("%s stats lack BYTES or INODES: %v", c.fsType, resp.GetUsage())
	}
	// statfs, like df, leaves out the filesystem's own metadata (journal,
	// inode tables, log): on the 16 MiB ext4 volume that is about 30%.
	if b.Total > c.size || b.Total <= c.size/2 {
		c.t.Fatalf("%s total = %d bytes, want about the volume size %d", c.fsType, b.Total, c.size)
	}
	// Cross-check the arithmetic with df on the same mount (nothing writes
	// to it in between).
	out, err := exec.Command("df", "-B1", "--output=size,used,avail,itotal,iused", m.target).Output()
	if err != nil {
		c.t.Fatalf("df %s: %v", m.target, err)
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	var df []int64
	for _, f := range strings.Fields(lines[len(lines)-1]) {
		v, err := strconv.ParseInt(f, 10, 64)
		if err != nil {
			c.t.Fatalf("df output %q: %v", out, err)
		}
		df = append(df, v)
	}
	if want := []int64{b.Total, b.Used, b.Available, in.Total, in.Used}; !slices.Equal(df, want) {
		c.t.Fatalf("%s stats (size, used, avail, inodes, iused) = %v, df reports %v", c.fsType, want, df)
	}
	if b.Used < 0 || b.Available <= 0 || b.Used+b.Available > b.Total {
		c.t.Fatalf("%s bytes inconsistent: %v", c.fsType, b)
	}
	if in.Total <= 0 || in.Used <= 0 || in.Used > in.Total {
		c.t.Fatalf("%s inodes inconsistent: %v", c.fsType, in)
	}
	return b, in
}

func changedChunks(a, b []string) int {
	n := 0
	for i := range b {
		if i >= len(a) || a[i] != b[i] {
			n++
		}
	}
	return n
}

func usedChunks(cs []string) int {
	n := 0
	for _, h := range cs {
		if h != "" {
			n++
		}
	}
	return n
}

func runFilesystemScenario(t *testing.T, c *fsCluster) {
	ctx := context.Background()
	vol := c.createVolume("pvc-fs", nil)
	files := map[string][]byte{}

	// First stage formats the empty volume.
	a := c.attach(vol, "node-a")
	empty, emptyInodes := c.stats(a)
	files["big.bin"] = c.writeFile(a, "big.bin", 3*testChunk+123, 1, true)
	files["dir/small.txt"] = c.writeFile(a, "dir/small.txt", 100, 2, true)
	// kubelet_volume_stats_*: used space and inodes grow with the files.
	written, writtenInodes := c.stats(a)
	if grown := written.Used - empty.Used; grown < 3*testChunk {
		t.Fatalf("%s used grew by %d bytes after writing %d, want at least %d", c.fsType, grown, 3*testChunk+223, 3*testChunk)
	}
	if written.Available >= empty.Available || written.Total != empty.Total {
		t.Fatalf("%s stats after write = %v, before = %v", c.fsType, written, empty)
	}
	if writtenInodes.Used < emptyInodes.Used+3 { // big.bin, dir, dir/small.txt
		t.Fatalf("%s inodes used %d -> %d, want +3", c.fsType, emptyInodes.Used, writtenInodes.Used)
	}
	c.backup(a)

	// Written but never fsynced: only fsfreeze flushes it before the snapshot.
	files["unsynced.bin"] = c.writeFile(a, "unsynced.bin", 2*testChunk, 3, false)
	c.backup(a)

	// node-a dies: the controller force-detaches it. Its later writes are lost,
	// and its unstage (once it comes back) is fenced and uploads nothing.
	c.writeFile(a, "lost.bin", 4096, 4, true)
	c.forceDetach(vol, "node-a")
	// Until it is unstaged, the fenced node still reports the mounted fs.
	c.stats(a)
	gen := c.latest(vol).Generation
	c.unmount(a)
	if got := c.latest(vol).Generation; got != gen {
		t.Fatalf("fenced node-a advanced latest.json to generation %d (was %d)", got, gen)
	}
	c.freeze.take(t) // unstage of an unmounted fs does not freeze

	// node-b restores from S3 and must mount the existing filesystem, not reformat it.
	files["lost.bin"] = nil
	b := c.attach(vol, "node-b")
	c.expectFiles(b, files)

	// After the restore, a small change is an incremental backup.
	before := c.latest(vol)
	files["dir/small.txt"] = c.writeFile(b, "dir/small.txt", 200, 5, true)
	c.backup(b)
	after := c.latest(vol)
	if ch, used := changedChunks(before.Chunks, after.Chunks), usedChunks(after.Chunks); ch == 0 || ch >= used {
		t.Fatalf("small change re-uploaded %d of %d used chunks, want an incremental backup", ch, used)
	} else {
		t.Logf("small change on %s: %d of %d used chunks changed", c.fsType, ch, used)
	}

	// Deleting a file does not free its blocks in the thin pool (no discard
	// mount option); the periodic fstrim does, and the next backup drops the
	// trimmed chunks from latest.json.
	if err := os.Remove(filepath.Join(b.target, "big.bin")); err != nil {
		t.Fatal(err)
	}
	files["big.bin"] = nil
	syscall.Sync()
	nb := c.nodes["node-b"]
	nb.cfg.TrimInterval = time.Hour
	nb.trimDue(ctx)
	nb.cfg.TrimInterval = 0
	if got := mustMetric(t, nb.m.reg, "hermit_fstrim_total", map[string]string{"result": "success"}); got != 1 {
		t.Fatalf("fstrim runs = %v, want 1", got)
	}
	trimmed := mustMetric(t, nb.m.reg, "hermit_fstrim_trimmed_bytes_total", nil)
	before = c.latest(vol)
	c.backup(b)
	after = c.latest(vol)
	if u0, u1 := usedChunks(before.Chunks), usedChunks(after.Chunks); u1 > u0-2 {
		t.Fatalf("after deleting a %d-chunk file and fstrim (%v bytes trimmed): %d -> %d used chunks", 3, trimmed, u0, u1)
	} else {
		t.Logf("fstrim on %s: %v bytes trimmed, used chunks %d -> %d", c.fsType, trimmed, u0, u1)
	}
	c.expectFiles(b, files)

	// In-use snapshot: the node agent freezes the mounted filesystem.
	files["at-snap.bin"] = c.writeFile(b, "at-snap.bin", testChunk, 6, false)
	atSnap := map[string][]byte{}
	for k, v := range files {
		atSnap[k] = v
	}
	if _, err := c.ctl.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{Name: "snap-fs", SourceVolumeId: vol}); err != nil {
		t.Fatal(err)
	}
	if calls := c.freeze.take(t); len(calls) != 2 {
		t.Fatalf("in-use snapshot fsfreeze calls = %q, want freeze+thaw", calls)
	}
	files["after-snap.bin"] = c.writeFile(b, "after-snap.bin", 4096, 7, false)
	atSnap["after-snap.bin"] = nil

	// Graceful move back: unstage unmounts, then uploads the last changes
	// (no fsfreeze, the filesystem is no longer mounted).
	c.detach(b)
	c.freeze.take(t)
	a = c.attach(vol, "node-a") // node-a's local copy is stale: restored from S3
	c.expectFiles(a, files)
	c.detach(a)

	// A volume created from the snapshot has the files as of the snapshot.
	clone := c.createVolume("pvc-fs-clone", &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{
		Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: "snap-fs"},
	}})
	cm := c.attach(clone, "node-b")
	c.expectFiles(cm, atSnap)
	c.detach(cm)
}

func TestIntegrationFilesystemExt4(t *testing.T) {
	runFilesystemScenario(t, realFSCluster(t, "ext4", testSize))
}

// mkfs.xfs refuses filesystems smaller than 300 MiB.
func TestIntegrationFilesystemXFS(t *testing.T) {
	runFilesystemScenario(t, realFSCluster(t, "xfs", 320<<20))
}
