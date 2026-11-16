package driver

import (
	"context"
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tsuzu/hermit-csi/pkg/store"
)

// metricValue returns the value of the series name{labels}, if present.
func metricValue(t *testing.T, reg prometheus.Gatherer, name string, labels map[string]string) (float64, bool) {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
	series:
		for _, m := range mf.GetMetric() {
			got := map[string]string{}
			for _, lp := range m.GetLabel() {
				got[lp.GetName()] = lp.GetValue()
			}
			for k, v := range labels {
				if got[k] != v {
					continue series
				}
			}
			switch mf.GetType() {
			case dto.MetricType_COUNTER:
				return m.GetCounter().GetValue(), true
			case dto.MetricType_GAUGE:
				return m.GetGauge().GetValue(), true
			case dto.MetricType_HISTOGRAM:
				return float64(m.GetHistogram().GetSampleCount()), true
			}
		}
	}
	return 0, false
}

func mustMetric(t *testing.T, reg prometheus.Gatherer, name string, labels map[string]string) float64 {
	t.Helper()
	v, ok := metricValue(t, reg, name, labels)
	if !ok {
		t.Fatalf("metric %s%v missing", name, labels)
	}
	return v
}

// flakyStore fails every Put while failing is set.
type flakyStore struct {
	store.ObjectStore
	failing atomic.Bool
}

func (f *flakyStore) Put(ctx context.Context, k string, d []byte, o store.PutOpts) (string, error) {
	if f.failing.Load() {
		return "", errors.New("injected S3 outage")
	}
	return f.ObjectStore.Put(ctx, k, d, o)
}

func TestBackupMetrics(t *testing.T) {
	s := &flakyStore{ObjectStore: store.NewMem()}
	c := newClusterWith(t, s, func(string) thinVG { return newFakeVG(t.TempDir()) }, "node-a", "node-b")
	ctx := context.Background()
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	now := t0
	for _, n := range c.nodes {
		n.now = func() time.Time { return now }
	}
	a := c.nodes["node-a"]
	reg := a.m.reg

	vol := c.createVolumeWith("pvc-metrics", nil, map[string]string{ParamBackupInterval: "2m"}).VolumeId
	if _, ok := metricValue(t, reg, "hermit_volume_last_backup_timestamp_seconds", map[string]string{"volume": vol}); ok {
		t.Fatal("unstaged volume has a last-backup series")
	}

	// Staging counts as synced: the local LV equals latest.json.
	c.attach(vol, "node-a")
	lv := map[string]string{"volume": vol}
	if got := mustMetric(t, reg, "hermit_volume_last_backup_timestamp_seconds", lv); got != float64(t0.Unix()) {
		t.Fatalf("last backup after stage = %v, want %d", got, t0.Unix())
	}
	if got := mustMetric(t, reg, "hermit_volume_backup_interval_seconds", lv); got != 120 {
		t.Fatalf("interval = %v, want 120", got)
	}
	if got := mustMetric(t, reg, "hermit_stages_total", map[string]string{"source": "restore"}); got != 1 {
		t.Fatalf("stages{restore} = %v", got)
	}

	// A backup that uploads moves the timestamp to the snapshot time.
	now = t0.Add(time.Minute)
	c.write("node-a", vol, 0, 100, 1)
	if _, err := a.BackupVolume(ctx, vol); err != nil {
		t.Fatal(err)
	}
	if got := mustMetric(t, reg, "hermit_volume_last_backup_timestamp_seconds", lv); got != float64(now.Unix()) {
		t.Fatalf("last backup = %v, want %d", got, now.Unix())
	}
	if got := mustMetric(t, reg, "hermit_backups_total", map[string]string{"trigger": "periodic", "result": "success"}); got != 1 {
		t.Fatalf("backups{success} = %v", got)
	}
	if got := mustMetric(t, reg, "hermit_backup_uploaded_chunks_total", nil); got != 1 {
		t.Fatalf("uploaded chunks = %v, want 1", got)
	}
	if got := mustMetric(t, reg, "hermit_backup_uploaded_bytes_total", nil); got != testChunk {
		t.Fatalf("uploaded bytes = %v, want %d", got, testChunk)
	}
	if got := mustMetric(t, reg, "hermit_volume_generation", lv); got != float64(c.latest(vol).Generation) {
		t.Fatalf("generation = %v, want %d", got, c.latest(vol).Generation)
	}

	// Nothing changed: still a confirmed sync, so the timestamp advances.
	now = t0.Add(2 * time.Minute)
	if _, err := a.BackupVolume(ctx, vol); err != nil {
		t.Fatal(err)
	}
	if got := mustMetric(t, reg, "hermit_volume_last_backup_timestamp_seconds", lv); got != float64(now.Unix()) {
		t.Fatalf("last backup after unchanged = %v, want %d", got, now.Unix())
	}
	if got := mustMetric(t, reg, "hermit_backups_total", map[string]string{"trigger": "periodic", "result": "unchanged"}); got != 1 {
		t.Fatalf("backups{unchanged} = %v", got)
	}

	// S3 outage: failures count up, the timestamp stays at the last sync.
	synced := now
	s.failing.Store(true)
	for i := 1; i <= 2; i++ {
		now = now.Add(2 * time.Minute)
		c.write("node-a", vol, 0, 100, int64(10+i))
		if _, err := a.BackupVolume(ctx, vol); err == nil {
			t.Fatal("backup succeeded during the outage")
		}
		if got := mustMetric(t, reg, "hermit_volume_backup_consecutive_failures", lv); got != float64(i) {
			t.Fatalf("consecutive failures = %v, want %d", got, i)
		}
	}
	if got := mustMetric(t, reg, "hermit_volume_last_backup_timestamp_seconds", lv); got != float64(synced.Unix()) {
		t.Fatalf("last backup moved during the outage: %v, want %d", got, synced.Unix())
	}
	if got := mustMetric(t, reg, "hermit_backups_total", map[string]string{"trigger": "periodic", "result": "failure"}); got != 2 {
		t.Fatalf("backups{failure} = %v", got)
	}
	if got := mustMetric(t, reg, "hermit_s3_requests_total", map[string]string{"operation": "put", "result": "error"}); got < 2 {
		t.Fatalf("s3 put errors = %v", got)
	}
	s.failing.Store(false)
	now = now.Add(2 * time.Minute)
	if _, err := a.BackupVolume(ctx, vol); err != nil {
		t.Fatal(err)
	}
	if got := mustMetric(t, reg, "hermit_volume_backup_consecutive_failures", lv); got != 0 {
		t.Fatalf("failures after recovery = %v", got)
	}

	// The state is persisted: a restarted node reports the same sync time.
	restarted := newNode(a.cfg, a.vg)
	if got := mustMetric(t, restarted.m.reg, "hermit_volume_last_backup_timestamp_seconds", lv); got != float64(now.Unix()) {
		t.Fatalf("after restart last backup = %v, want %d", got, now.Unix())
	}

	// Snapshots and unstage are separate triggers; unstaged volumes drop out.
	if _, err := c.ctl.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{SourceVolumeId: vol, Name: "snap-metrics"}); err != nil {
		t.Fatal(err)
	}
	if _, ok := metricValue(t, reg, "hermit_backups_total", map[string]string{"trigger": "snapshot"}); !ok {
		t.Fatal("no snapshot-triggered backup counted")
	}
	c.detach(vol, "node-a")
	if _, ok := metricValue(t, reg, "hermit_backups_total", map[string]string{"trigger": "unstage"}); !ok {
		t.Fatal("no unstage backup counted")
	}
	if _, ok := metricValue(t, reg, "hermit_volume_last_backup_timestamp_seconds", lv); ok {
		t.Fatal("unstaged volume still reported")
	}

	// Moving to node-b restores; coming back to node-a would reuse.
	c.attach(vol, "node-b")
	breg := c.nodes["node-b"].m.reg
	if got := mustMetric(t, breg, "hermit_restores_total", map[string]string{"result": "success"}); got != 1 {
		t.Fatalf("restores = %v", got)
	}
	if got := mustMetric(t, breg, "hermit_restore_bytes_total", nil); got != testChunk {
		t.Fatalf("restored bytes = %v, want %d", got, testChunk)
	}

	// Thin pool usage and CSI RPC counts.
	if got := mustMetric(t, reg, "hermit_thin_pool_size_bytes", map[string]string{"vg": "fake", "pool": "pool"}); got != fakePoolSize {
		t.Fatalf("pool size = %v", got)
	}
}

func TestCSIOperationMetrics(t *testing.T) {
	m := newMetrics("node", "v1")
	info := &grpc.UnaryServerInfo{FullMethod: "/csi.v1.Node/NodeStageVolume"}
	ok := func(context.Context, any) (any, error) { return nil, nil }
	busy := func(context.Context, any) (any, error) { return nil, status.Error(codes.Aborted, "in progress") }
	for _, h := range []grpc.UnaryHandler{ok, busy, busy} {
		_, _ = m.interceptor(context.Background(), nil, info, h)
	}
	for code, want := range map[string]float64{"OK": 1, "Aborted": 2} {
		if got := mustMetric(t, m.reg, "hermit_csi_operations_total", map[string]string{"method": info.FullMethod, "code": code}); got != want {
			t.Errorf("calls{code=%s} = %v, want %v", code, got, want)
		}
	}
	if got := mustMetric(t, m.reg, "hermit_csi_operation_duration_seconds", map[string]string{"method": info.FullMethod}); got != 3 {
		t.Errorf("duration samples = %v, want 3", got)
	}
}

func TestFencedVolumeMetric(t *testing.T) {
	c := newCluster(t, "node-a", "node-b")
	ctx := context.Background()
	vol := c.createVolume("pvc-fenced", nil).VolumeId
	c.attach(vol, "node-a")
	c.forceDetach(vol, "node-a") // node-a died; its stage is still recorded
	c.attach(vol, "node-b")
	a := c.nodes["node-a"]
	if _, err := a.BackupVolume(ctx, vol); !errors.Is(err, ErrFenced) {
		t.Fatalf("backup on the old node: %v, want fenced", err)
	}
	if got := mustMetric(t, a.m.reg, "hermit_volume_fenced", map[string]string{"volume": vol}); got != 1 {
		t.Fatalf("fenced = %v", got)
	}
	if got := mustMetric(t, a.m.reg, "hermit_backups_total", map[string]string{"result": "fenced"}); got != 1 {
		t.Fatalf("backups{fenced} = %v", got)
	}
	if got := mustMetric(t, a.m.reg, "hermit_volume_backup_consecutive_failures", map[string]string{"volume": vol}); got != 0 {
		t.Fatalf("fencing counted as a failure: %v", got)
	}
}

func TestControllerGCMetricsAndHandler(t *testing.T) {
	c := newCluster(t)
	before := mustMetric(t, c.ctl.m.reg, "hermit_gc_last_success_timestamp_seconds", nil)
	if before == 0 {
		t.Fatal("GC timestamp should start at process start")
	}
	c.ctl.runGC(context.Background())
	if got := mustMetric(t, c.ctl.m.reg, "hermit_gc_runs_total", map[string]string{"result": "success"}); got != 1 {
		t.Fatalf("gc runs = %v", got)
	}
	if _, ok := metricValue(t, c.ctl.m.reg, "hermit_backups_total", nil); ok {
		t.Fatal("controller exports node metrics")
	}

	srv := httptest.NewServer(c.ctl.m.Handler())
	defer srv.Close()
	resp, err := srv.Client().Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	for _, want := range []string{`hermit_build_info{mode="controller",version=""} 1`, "hermit_gc_runs_total", "go_goroutines"} {
		if !strings.Contains(string(body), want) {
			t.Errorf("/metrics lacks %q", want)
		}
	}
}
