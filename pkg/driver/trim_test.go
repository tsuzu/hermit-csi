package driver

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestParseTrimmed(t *testing.T) {
	for out, want := range map[string]int64{
		"/var/lib/kubelet/x/globalmount: 5.2 GiB (5583457280 bytes) trimmed\n": 5583457280,
		"/mnt: 0 B (0 bytes) trimmed\n":                                        0,
		"unexpected":                                                           -1,
	} {
		if got := parseTrimmed(out); got != want {
			t.Errorf("parseTrimmed(%q) = %d, want %d", out, got, want)
		}
	}
}

// TestPeriodicFstrim checks the trim schedule: mounted filesystem volumes are
// trimmed once per interval (persisted across restarts, retried one interval
// after a failure), block and fenced volumes never.
func TestPeriodicFstrim(t *testing.T) {
	ctx := context.Background()
	c := newCluster(t, "node-a")
	n := c.nodes["node-a"]
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	now := t0
	n.now = func() time.Time { return now }
	n.cfg.TrimInterval = 24 * time.Hour

	var calls []string
	var fail error
	n.trim = func(_ context.Context, path string) (int64, error) {
		calls = append(calls, path)
		return 1 << 20, fail
	}

	fsVol := c.createVolume("pvc-trim-fs", nil).VolumeId
	blockVol := c.createVolume("pvc-trim-block", nil).VolumeId
	fencedVol := c.createVolume("pvc-trim-fenced", nil).VolumeId
	for _, v := range []string{fsVol, blockVol, fencedVol} {
		c.attach(v, "node-a")
	}
	// The fake cluster stages block volumes; turn two into mounted filesystems.
	staging := map[string]string{fsVol: "/staging/fs", fencedVol: "/staging/fenced"}
	for vol, path := range staging {
		st, err := n.states.load(vol)
		if err != nil {
			t.Fatal(err)
		}
		st.Block, st.Staging, st.Fenced = false, path, vol == fencedVol
		if err := n.states.save(st); err != nil {
			t.Fatal(err)
		}
	}

	tick := func(d time.Duration, want ...string) {
		t.Helper()
		now = t0.Add(d)
		calls = nil
		n.trimDue(ctx)
		if len(calls) != len(want) || (len(want) == 1 && calls[0] != want[0]) {
			t.Fatalf("at +%s: fstrim calls %q, want %q", d, calls, want)
		}
	}
	tick(0, "/staging/fs") // never trimmed yet: right away
	tick(time.Hour)        // not due
	tick(24*time.Hour, "/staging/fs")

	// A restarted node keeps the schedule (it is in the state file).
	n2 := newNode(n.cfg, c.vgs["node-a"])
	n2.now, n2.trim = n.now, n.trim
	now, calls = t0.Add(30*time.Hour), nil
	n2.trimDue(ctx)
	if len(calls) != 0 {
		t.Fatalf("restarted node trimmed again before the interval: %q", calls)
	}

	fail = errors.New("injected fstrim failure")
	tick(48*time.Hour, "/staging/fs")
	tick(49 * time.Hour) // a failure is retried one interval later, not every tick
	fail = nil
	tick(72*time.Hour, "/staging/fs")

	if got := mustMetric(t, n.m.reg, "hermit_fstrim_total", map[string]string{"result": "success"}); got != 3 {
		t.Errorf("fstrim success = %v, want 3", got)
	}
	if got := mustMetric(t, n.m.reg, "hermit_fstrim_total", map[string]string{"result": "failure"}); got != 1 {
		t.Errorf("fstrim failure = %v, want 1", got)
	}
	if got := mustMetric(t, n.m.reg, "hermit_fstrim_trimmed_bytes_total", nil); got != 3<<20 {
		t.Errorf("trimmed bytes = %v, want %d", got, 3<<20)
	}

	// Disabled: nothing runs.
	n.cfg.TrimInterval = 0
	tick(200 * time.Hour)
}
