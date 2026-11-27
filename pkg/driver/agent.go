package driver

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	"golang.org/x/sync/errgroup"
	"k8s.io/klog/v2"

	"github.com/tsuzu/hermit-csi/pkg/backup"
	"github.com/tsuzu/hermit-csi/pkg/lvm"
	"github.com/tsuzu/hermit-csi/pkg/manifest"
	"github.com/tsuzu/hermit-csi/pkg/store"
)

const agentSnapshotPath = "/v1/snapshot"

type agentSnapshotRequest struct {
	VolumeID   string `json:"volumeId"`
	SnapshotID string `json:"snapshotId"`
}

// BackupVolume runs one incremental backup of a staged volume (counted as a
// periodic backup in the metrics).
func (n *Node) BackupVolume(ctx context.Context, vol string) (*manifest.Manifest, error) {
	return n.backupVolume(ctx, vol, triggerPeriodic)
}

func (n *Node) backupVolume(ctx context.Context, vol, trigger string) (*manifest.Manifest, error) {
	defer n.locks.lock(vol)()
	st, err := n.states.load(vol)
	if err != nil {
		return nil, err
	}
	if st == nil || !st.Staged {
		return nil, fmt.Errorf("volume %s is not staged on this node", vol)
	}
	m, err := n.backupLocked(ctx, st, trigger)
	if serr := n.states.save(st); err == nil {
		err = serr
	}
	return m, err
}

// backupLocked uploads everything that changed since st.BaseSnap and moves the
// base forward. It updates st in place (including the sync time and failure
// count reported as metrics); the caller persists it.
// Returns the manifest that now equals the volume content.
func (n *Node) backupLocked(ctx context.Context, st *volState, trigger string) (*manifest.Manifest, error) {
	start := n.now()
	r, err := n.doBackup(ctx, st)
	result := resultFailure
	switch {
	case err == nil && r.uploaded:
		result = resultSuccess
	case err == nil:
		result = resultUnchanged
	case errors.Is(err, ErrFenced):
		result = resultFenced
	}
	n.m.observeBackup(trigger, result, n.now().Sub(start))
	if err != nil {
		if result == resultFailure {
			st.BackupFailures++
		}
		return nil, err
	}
	// S3 now holds (or already held) the volume as of the snapshot.
	st.LastBackup, st.BackupFailures = r.snapAt, 0
	if r.uploaded {
		n.m.uploadedBytes.Add(float64(r.stats.BytesUploaded))
		n.m.uploadedChunks.Add(float64(r.stats.ChunksUploaded))
		n.m.changedChunks.Add(float64(r.stats.ChunksChanged))
	}
	if r.rescan {
		n.m.rescans.Inc()
	}
	return r.m, nil
}

type backupResult struct {
	m        *manifest.Manifest
	snapAt   time.Time // when the snapshot that m equals was taken
	uploaded bool      // false: nothing changed, m is the old manifest
	rescan   bool      // no usable base snapshot
	stats    backup.Stats
}

func (n *Node) doBackup(ctx context.Context, st *volState) (backupResult, error) {
	var r backupResult
	vol := st.VolumeID
	if st.Fenced {
		return r, ErrFenced
	}
	base, etag, err := manifest.Load(ctx, n.s, manifest.LatestKey(vol))
	if err != nil {
		return r, err
	}
	if etag != st.ETag || base.Epoch != st.Epoch {
		ours, err := n.adoptOwnWrite(ctx, st, base, etag)
		if err != nil {
			return r, err
		}
		if !ours {
			st.Fenced = true
			return r, fmt.Errorf("%w (epoch %d -> %d)", ErrFenced, st.Epoch, base.Epoch)
		}
	}

	newGen := base.Generation + 1
	snap := snapName(vol, newGen)
	if err := n.vg.Remove(ctx, snap); err != nil { // leftover of a failed attempt
		return r, err
	}
	r.snapAt = n.now()
	if err := n.takeSnapshot(ctx, st, snap); err != nil {
		return r, err
	}
	committed := false
	defer func() {
		if !committed {
			if err := n.vg.Remove(context.Background(), snap); err != nil {
				klog.ErrorS(err, "cleanup snapshot", "lv", snap)
			}
		}
	}()

	newLV, err := n.vg.Get(ctx, snap)
	if err != nil || newLV == nil {
		return r, fmt.Errorf("snapshot %s vanished: %v", snap, err)
	}
	var baseLV *lvm.LV
	if st.BaseSnap != "" {
		if baseLV, err = n.vg.Get(ctx, st.BaseSnap); err != nil {
			return r, err
		}
	}

	var changed []int64
	if baseLV != nil && st.Generation == base.Generation {
		ranges, err := n.vg.ChangedRanges(ctx, baseLV.ThinID, newLV.ThinID)
		if err != nil {
			return r, err
		}
		changed = backup.ChunksForRanges(ranges, base.ChunkSize, base.NumChunks())
	} else {
		// No usable base: scan what is allocated locally plus everything the
		// manifest says is non-zero. Reads more, but uploads only differences.
		klog.InfoS("no base snapshot, rescanning allocated blocks", "volume", vol)
		r.rescan = true
		ranges, err := n.vg.MappedRanges(ctx, newLV.ThinID)
		if err != nil {
			return r, err
		}
		for i, h := range base.Chunks {
			if h != "" {
				ranges = append(ranges, backup.Range{Off: int64(i) * base.ChunkSize, Len: base.ChunkLen(int64(i))})
			}
		}
		changed = backup.ChunksForRanges(ranges, base.ChunkSize, base.NumChunks())
	}

	next := base
	if len(changed) > 0 || baseLV == nil {
		f, err := os.Open(n.vg.DevicePath(snap))
		if err != nil {
			return r, err
		}
		var stats backup.Stats
		next, stats, err = n.eng.Backup(ctx, f, base, changed)
		r.stats = stats
		f.Close()
		if err != nil {
			return r, err
		}
		next.Generation, next.Epoch = newGen, st.Epoch
		newETag, err := manifest.Save(ctx, n.s, manifest.LatestKey(vol), next, store.PutOpts{IfMatch: st.ETag})
		if err != nil {
			if errors.Is(err, store.ErrPrecondition) {
				st.Fenced = true
				return r, fmt.Errorf("%w: latest.json changed under us", ErrFenced)
			}
			return r, err
		}
		st.ETag, st.Generation = newETag, newGen
		r.uploaded = true
		klog.InfoS("backup done", "volume", vol, "generation", newGen,
			"scanned", stats.ChunksScanned, "changed", stats.ChunksChanged,
			"uploaded", stats.ChunksUploaded, "bytes", stats.BytesUploaded)
	} else {
		// Nothing changed: keep the old base and generation.
		r.m = base
		return r, nil
	}

	committed = true
	n.setBase(ctx, st, snap)
	r.m = next
	return r, nil
}

// setBase makes snap the base snapshot of st and removes every other snapshot
// of the volume: the previous base, and any left behind by a crash.
func (n *Node) setBase(ctx context.Context, st *volState, snap string) {
	if err := n.vg.AddTag(ctx, snap, lvm.TagBase); err != nil {
		klog.ErrorS(err, "tag base snapshot", "lv", snap)
	}
	st.BaseSnap = snap
	lvs, err := n.vg.List(ctx)
	if err != nil {
		klog.ErrorS(err, "list LVs to prune snapshots", "volume", st.VolumeID)
		return
	}
	for _, lv := range lvs {
		if lv.Name != snap && isSnapOf(st.VolumeID, lv.Name) {
			if err := n.vg.Remove(ctx, lv.Name); err != nil {
				klog.ErrorS(err, "remove stale snapshot", "lv", lv.Name)
			}
		}
	}
}

// adoptOwnWrite recognizes a latest.json that this node wrote but never
// recorded in its state file: the process died between the manifest PUT and
// the state save, or a PUT reported as failed had in fact been applied. Such
// a manifest has our epoch and is exactly one generation ahead. Only the node
// granted an epoch writes with it (the controller bumps the epoch whenever it
// fences), so the write is ours; treating it as foreign would fence the volume
// and stop its backups for good.
//
// On a match st catches up with m. If the snapshot that m was taken from
// survived, it becomes the base; otherwise the old base stays, which is still
// correct: everything that changed since m's snapshot also changed since the
// old base, so the next diff is a superset.
func (n *Node) adoptOwnWrite(ctx context.Context, st *volState, m *manifest.Manifest, etag string) (bool, error) {
	if m.Epoch != st.Epoch || m.Generation != st.Generation+1 {
		return false, nil
	}
	snap := snapName(st.VolumeID, m.Generation)
	lv, err := n.vg.Get(ctx, snap)
	if err != nil {
		return false, err
	}
	klog.InfoS("latest.json is our own unrecorded write, catching up", "volume", st.VolumeID,
		"generation", m.Generation, "snapshotKept", lv != nil)
	st.ETag, st.Generation = etag, m.Generation
	if lv != nil {
		n.setBase(ctx, st, snap)
	}
	return true, nil
}

// takeSnapshot creates a thin snapshot, freezing a mounted filesystem around
// it so the snapshot is filesystem-consistent rather than crash-consistent.
func (n *Node) takeSnapshot(ctx context.Context, st *volState, snap string) error {
	if st.Staged && !st.Block && st.Staging != "" && n.freeze != nil {
		if err := n.freeze(ctx, st.Staging, true); err != nil {
			klog.ErrorS(err, "fsfreeze failed, snapshot will be crash-consistent", "volume", st.VolumeID)
		} else {
			defer func() {
				if err := n.freeze(context.Background(), st.Staging, false); err != nil {
					klog.ErrorS(err, "fsfreeze -u failed", "volume", st.VolumeID)
				}
			}()
		}
	}
	return n.vg.Snapshot(ctx, st.VolumeID, snap)
}

// SnapshotVolume backs up a staged volume and records the result as an
// immutable snapshot manifest.
func (n *Node) SnapshotVolume(ctx context.Context, vol, snapID string) error {
	m, err := n.backupVolume(ctx, vol, triggerSnapshot)
	if err != nil {
		return err
	}
	sm := m.Clone()
	sm.SnapshotID, sm.SourceVolumeID, sm.CreatedAt = snapID, vol, time.Now().UTC()
	if _, err := manifest.Save(ctx, n.s, manifest.SnapshotKey(snapID), sm, store.PutOpts{IfNoneMatch: true}); err != nil &&
		!errors.Is(err, store.ErrPrecondition) {
		return err
	}
	return nil
}

// ---------------------------------------------------------------- loops

// orphanCleanupInterval is how often local LVs that can never be reused are removed.
const orphanCleanupInterval = 5 * time.Minute

// backupLoop wakes every MinBackupInterval (or the node default, if shorter)
// and backs up each staged volume whose own interval has elapsed.
func (n *Node) backupLoop(ctx context.Context) {
	tick := MinBackupInterval
	if d := n.cfg.BackupInterval; d > 0 && d < tick {
		tick = d
	}
	t := time.NewTicker(tick)
	defer t.Stop()
	lastCleanup := n.now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		n.backupDue(ctx)
		n.trimDue(ctx)
		if n.now().Sub(lastCleanup) >= orphanCleanupInterval {
			n.cleanupOrphans(ctx)
			lastCleanup = n.now()
		}
	}
}

// intervalFor is the volume's StorageClass interval, else the node default
// (--backup-interval; 0 disables periodic backups for such volumes).
func (n *Node) intervalFor(st *volState) time.Duration {
	if st.BackupInterval != "" {
		d, err := parseBackupInterval(st.BackupInterval)
		if err == nil {
			return d
		}
		klog.ErrorS(err, "bad backup interval in state, using node default", "volume", st.VolumeID)
	}
	return n.cfg.BackupInterval
}

func (n *Node) markBackup(vol string, t time.Time) {
	n.schedMu.Lock()
	defer n.schedMu.Unlock()
	n.lastBackup[vol] = t
}

func (n *Node) forgetBackup(vol string) {
	n.schedMu.Lock()
	defer n.schedMu.Unlock()
	delete(n.lastBackup, vol)
}

func (n *Node) backupDueAt(vol string, interval time.Duration, now time.Time) bool {
	n.schedMu.Lock()
	defer n.schedMu.Unlock()
	last, ok := n.lastBackup[vol]
	return !ok || now.Sub(last) >= interval
}

// maxConcurrentBackups bounds how many volumes the periodic loop backs up
// at once. Volumes are independent (each backup takes only its own volume
// lock), so one volume's slow or stuck backup must not delay the others —
// with a single sequential loop it once could, indefinitely.
const maxConcurrentBackups = 4

// backupDue backs up every staged volume whose interval has elapsed since
// its last periodic backup started, each in its own goroutine and under
// its own --op-timeout deadline. A failed backup is retried one interval
// later.
func (n *Node) backupDue(ctx context.Context) {
	vols, err := n.states.list()
	if err != nil {
		klog.ErrorS(err, "list states")
		return
	}
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(maxConcurrentBackups)
	for _, vol := range vols {
		st, err := n.states.load(vol)
		if err != nil || st == nil || !st.Staged || st.Fenced {
			continue
		}
		interval := n.intervalFor(st)
		now := n.now()
		if interval <= 0 || !n.backupDueAt(vol, interval, now) {
			continue
		}
		n.markBackup(vol, now)
		g.Go(func() error {
			bctx, cancel := context.WithTimeout(gctx, n.opTimeout())
			defer cancel()
			if _, err := n.BackupVolume(bctx, vol); err != nil {
				klog.ErrorS(err, "periodic backup failed", "volume", vol, "interval", interval)
			}
			return nil
		})
	}
	_ = g.Wait()
}

// trimDue runs fstrim on every mounted filesystem volume whose last trim is
// at least --fstrim-interval old. Filesystems are mounted without the discard
// option, so without this, blocks of deleted files stay allocated in the thin
// pool forever and keep being backed up and restored. The trimmed blocks are
// freed in the pool once the next backup replaces the base snapshot, which
// also records them as zero chunks in latest.json.
func (n *Node) trimDue(ctx context.Context) {
	interval := n.cfg.TrimInterval
	if interval <= 0 || n.trim == nil {
		return
	}
	vols, err := n.states.list()
	if err != nil {
		return
	}
	for _, vol := range vols {
		if ctx.Err() != nil {
			return
		}
		n.trimVolume(ctx, vol, interval)
	}
}

func (n *Node) trimVolume(ctx context.Context, vol string, interval time.Duration) {
	// Under the volume lock: never overlaps the fsfreeze of a backup.
	defer n.locks.lock(vol)()
	st, err := n.states.load(vol)
	if err != nil || st == nil || !st.Staged || st.Block || st.Fenced || st.Staging == "" {
		return
	}
	now := n.now()
	if now.Sub(st.LastTrim) < interval {
		return
	}
	start := time.Now()
	trimmed, err := n.trim(ctx, st.Staging)
	if ctx.Err() != nil {
		return // shutting down: try again after the restart
	}
	// Also on failure: retry one interval later rather than on every tick.
	st.LastTrim = now
	if serr := n.states.save(st); serr != nil {
		klog.ErrorS(serr, "save state", "volume", vol)
	}
	if err != nil {
		n.m.trims.WithLabelValues(resultFailure).Inc()
		klog.ErrorS(err, "fstrim failed", "volume", vol, "path", st.Staging)
		return
	}
	n.m.trims.WithLabelValues(resultSuccess).Inc()
	if trimmed > 0 {
		n.m.trimmedBytes.Add(float64(trimmed))
	}
	klog.InfoS("fstrim done", "volume", vol, "trimmedBytes", trimmed, "took", time.Since(start).Round(time.Millisecond))
}

// cleanupOrphans removes local copies that can never be reused: the volume was
// deleted, or another node has since written a newer generation.
func (n *Node) cleanupOrphans(ctx context.Context) {
	vols, err := n.states.list()
	if err != nil {
		return
	}
	for _, vol := range vols {
		func() {
			defer n.locks.lock(vol)()
			st, err := n.states.load(vol)
			if err != nil || st == nil || st.Staged {
				return
			}
			m, _, err := manifest.Load(ctx, n.s, manifest.LatestKey(vol))
			switch {
			case errors.Is(err, store.ErrNotFound):
				klog.InfoS("volume deleted, removing local copy", "volume", vol)
			case err != nil:
				return
			case m.Generation != st.Generation:
				klog.InfoS("local copy is stale, removing", "volume", vol, "local", st.Generation, "remote", m.Generation)
			default:
				return
			}
			if err := n.removeLocal(ctx, vol); err != nil {
				klog.ErrorS(err, "remove local copy", "volume", vol)
				return
			}
			_ = n.states.remove(vol)
		}()
	}
}

// ---------------------------------------------------------------- agent HTTP

func (n *Node) agentHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(agentSnapshotPath, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if tok := n.cfg.AgentToken; tok != "" {
			got := r.Header.Get("Authorization")
			if subtle.ConstantTimeCompare([]byte(got), []byte("Bearer "+tok)) != 1 {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
		}
		var req agentSnapshotRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if checkID("volume", req.VolumeID) != nil || checkID("snapshot", req.SnapshotID) != nil {
			http.Error(w, "invalid id", http.StatusBadRequest)
			return
		}
		st, err := n.states.load(req.VolumeID)
		if err != nil || st == nil || !st.Staged {
			http.Error(w, "volume not staged on this node", http.StatusConflict)
			return
		}
		if err := n.SnapshotVolume(r.Context(), req.VolumeID, req.SnapshotID); err != nil {
			code := http.StatusInternalServerError
			if errors.Is(err, ErrFenced) {
				code = http.StatusConflict
			}
			http.Error(w, err.Error(), code)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	return mux
}

func (n *Node) serveAgent(ctx context.Context) error {
	if n.cfg.AgentListen == "" {
		return nil
	}
	srv := &http.Server{Addr: n.cfg.AgentListen, Handler: n.agentHandler(), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()
	klog.InfoS("snapshot agent listening", "addr", n.cfg.AgentListen)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
