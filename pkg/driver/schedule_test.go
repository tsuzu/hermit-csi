package driver

import (
	"context"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestBackupIntervalParameter(t *testing.T) {
	c := newCluster(t, "node-a")
	ctx := context.Background()

	v := c.createVolumeWith("pvc-param", nil, map[string]string{ParamBackupInterval: "90s"})
	if got := v.VolumeContext[ParamBackupInterval]; got != "1m30s" {
		t.Fatalf("volume context %s = %q, want normalized 1m30s", ParamBackupInterval, got)
	}
	// Idempotent retry returns the same context.
	if again := c.createVolumeWith("pvc-param", nil, map[string]string{ParamBackupInterval: "90s"}); again.VolumeContext[ParamBackupInterval] != "1m30s" {
		t.Fatalf("retry volume context = %v", again.VolumeContext)
	}
	if v := c.createVolume("pvc-noparam", nil); len(v.VolumeContext) != 0 {
		t.Fatalf("no parameter should give no volume context, got %v", v.VolumeContext)
	}

	for _, bad := range []string{"soon", "5s", "-1m", "0"} {
		_, err := c.ctl.CreateVolume(ctx, &csi.CreateVolumeRequest{
			Name: "pvc-bad", VolumeCapabilities: []*csi.VolumeCapability{blockCap},
			Parameters: map[string]string{ParamBackupInterval: bad},
		})
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s=%q: got %v, want InvalidArgument", ParamBackupInterval, bad, err)
		}
	}

	// A hand-written PV with a bad volume context is rejected at stage time.
	pub, err := c.ctl.ControllerPublishVolume(ctx, &csi.ControllerPublishVolumeRequest{VolumeId: "pvc-noparam", NodeId: "node-a", VolumeCapability: blockCap})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.nodes["node-a"].NodeStageVolume(ctx, &csi.NodeStageVolumeRequest{
		VolumeId: "pvc-noparam", StagingTargetPath: t.TempDir(), VolumeCapability: blockCap,
		PublishContext: pub.PublishContext, VolumeContext: map[string]string{ParamBackupInterval: "1s"},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("NodeStage with bad interval: got %v, want InvalidArgument", err)
	}
}

func TestPerVolumeBackupSchedule(t *testing.T) {
	c := newCluster(t, "node-a", "node-b")
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	now := t0
	for _, n := range c.nodes {
		n.now = func() time.Time { return now }
	}
	a, b := c.nodes["node-a"], c.nodes["node-b"]
	a.cfg.BackupInterval = 5 * time.Minute // node default
	b.cfg.BackupInterval = 0               // periodic backups off by default

	fast := c.createVolumeWith("pvc-fast", nil, map[string]string{ParamBackupInterval: "1m"}).VolumeId
	slow := c.createVolume("pvc-slow", nil).VolumeId
	c.attach(fast, "node-a")
	c.attach(slow, "node-a")

	gens := func() (int64, int64) { return c.latest(fast).Generation, c.latest(slow).Generation }
	tick := func(n *Node, d time.Duration, wantFast, wantSlow int64) {
		t.Helper()
		now = t0.Add(d)
		n.backupDue(context.Background())
		if f, s := gens(); f != wantFast || s != wantSlow {
			t.Fatalf("at +%s: generations fast=%d slow=%d, want %d %d", d, f, s, wantFast, wantSlow)
		}
	}
	write := func(node string, seed int64) {
		c.write(node, fast, 0, 100, seed)
		c.write(node, slow, 0, 100, seed)
	}

	if st, _ := a.states.load(fast); st.BackupInterval != "1m0s" {
		t.Fatalf("state backup interval = %q, want 1m0s", st.BackupInterval)
	}
	write("node-a", 1)
	tick(a, 30*time.Second, 1, 1) // nothing due yet (stage counts as a backup)
	tick(a, time.Minute, 2, 1)    // fast: its own 1m interval
	write("node-a", 2)
	tick(a, 2*time.Minute-time.Second, 2, 1) // 59s since fast's last backup
	tick(a, 2*time.Minute, 3, 1)             // fast again
	tick(a, 5*time.Minute, 3, 2)             // slow: node default 5m (fast has no changes)

	// Moving to a node whose default is "off": only the volume with its own
	// interval keeps being backed up periodically.
	c.detach(fast, "node-a")
	c.detach(slow, "node-a")
	now = t0 // stage on node-b happens at t0
	c.attach(fast, "node-b")
	c.attach(slow, "node-b")
	write("node-b", 3)
	f0, s0 := gens()
	tick(b, time.Hour, f0+1, s0)

	// After a node plugin restart (schedule lost), the first tick backs up
	// every staged volume that has a periodic interval.
	b.forgetBackup(fast)
	write("node-b", 4)
	tick(b, time.Hour+time.Second, f0+2, s0)
}
