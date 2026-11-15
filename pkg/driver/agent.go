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

	"k8s.io/klog/v2"

	"github.com/tsuzu/s3lvm-csi/pkg/backup"
	"github.com/tsuzu/s3lvm-csi/pkg/lvm"
	"github.com/tsuzu/s3lvm-csi/pkg/manifest"
	"github.com/tsuzu/s3lvm-csi/pkg/store"
)

const agentSnapshotPath = "/v1/snapshot"

type agentSnapshotRequest struct {
	VolumeID   string `json:"volumeId"`
	SnapshotID string `json:"snapshotId"`
}

// BackupVolume runs one incremental backup of a staged volume.
func (n *Node) BackupVolume(ctx context.Context, vol string) (*manifest.Manifest, error) {
	defer n.locks.lock(vol)()
	st, err := n.states.load(vol)
	if err != nil {
		return nil, err
	}
	if st == nil || !st.Staged {
		return nil, fmt.Errorf("volume %s is not staged on this node", vol)
	}
	m, err := n.backupLocked(ctx, st)
	if serr := n.states.save(st); err == nil {
		err = serr
	}
	return m, err
}

// backupLocked uploads everything that changed since st.BaseSnap and moves the
// base forward. It updates st in place; the caller persists it.
// Returns the manifest that now equals the volume content.
func (n *Node) backupLocked(ctx context.Context, st *volState) (*manifest.Manifest, error) {
	vol := st.VolumeID
	if st.Fenced {
		return nil, ErrFenced
	}
	base, etag, err := manifest.Load(ctx, n.s, manifest.LatestKey(vol))
	if err != nil {
		return nil, err
	}
	if etag != st.ETag || base.Epoch != st.Epoch {
		st.Fenced = true
		return nil, fmt.Errorf("%w (epoch %d -> %d)", ErrFenced, st.Epoch, base.Epoch)
	}

	newGen := base.Generation + 1
	snap := snapName(vol, newGen)
	if err := n.vg.Remove(ctx, snap); err != nil { // leftover of a failed attempt
		return nil, err
	}
	if err := n.takeSnapshot(ctx, st, snap); err != nil {
		return nil, err
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
		return nil, fmt.Errorf("snapshot %s vanished: %v", snap, err)
	}
	var baseLV *lvm.LV
	if st.BaseSnap != "" {
		if baseLV, err = n.vg.Get(ctx, st.BaseSnap); err != nil {
			return nil, err
		}
	}

	var changed []int64
	if baseLV != nil && st.Generation == base.Generation {
		ranges, err := n.vg.ChangedRanges(ctx, baseLV.ThinID, newLV.ThinID)
		if err != nil {
			return nil, err
		}
		changed = backup.ChunksForRanges(ranges, base.ChunkSize, base.NumChunks())
	} else {
		// No usable base: scan what is allocated locally plus everything the
		// manifest says is non-zero. Reads more, but uploads only differences.
		klog.InfoS("no base snapshot, rescanning allocated blocks", "volume", vol)
		ranges, err := n.vg.MappedRanges(ctx, newLV.ThinID)
		if err != nil {
			return nil, err
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
			return nil, err
		}
		var stats backup.Stats
		next, stats, err = n.eng.Backup(ctx, f, base, changed)
		f.Close()
		if err != nil {
			return nil, err
		}
		next.Generation, next.Epoch = newGen, st.Epoch
		newETag, err := manifest.Save(ctx, n.s, manifest.LatestKey(vol), next, store.PutOpts{IfMatch: st.ETag})
		if err != nil {
			if errors.Is(err, store.ErrPrecondition) {
				st.Fenced = true
				return nil, fmt.Errorf("%w: latest.json changed under us", ErrFenced)
			}
			return nil, err
		}
		st.ETag, st.Generation = newETag, newGen
		klog.InfoS("backup done", "volume", vol, "generation", newGen,
			"scanned", stats.ChunksScanned, "changed", stats.ChunksChanged,
			"uploaded", stats.ChunksUploaded, "bytes", stats.BytesUploaded)
	} else {
		// Nothing changed: keep the old base and generation.
		return base, nil
	}

	committed = true
	if st.BaseSnap != "" && st.BaseSnap != snap {
		if err := n.vg.Remove(ctx, st.BaseSnap); err != nil {
			klog.ErrorS(err, "remove old base snapshot", "lv", st.BaseSnap)
		}
	}
	if err := n.vg.AddTag(ctx, snap, lvm.TagBase); err != nil {
		klog.ErrorS(err, "tag base snapshot", "lv", snap)
	}
	st.BaseSnap = snap
	return next, nil
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
	m, err := n.BackupVolume(ctx, vol)
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

func (n *Node) backupLoop(ctx context.Context) {
	if n.cfg.BackupInterval <= 0 {
		return
	}
	t := time.NewTicker(n.cfg.BackupInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		n.backupAll(ctx)
		n.cleanupOrphans(ctx)
	}
}

func (n *Node) backupAll(ctx context.Context) {
	vols, err := n.states.list()
	if err != nil {
		klog.ErrorS(err, "list states")
		return
	}
	for _, vol := range vols {
		st, err := n.states.load(vol)
		if err != nil || st == nil || !st.Staged || st.Fenced {
			continue
		}
		if _, err := n.BackupVolume(ctx, vol); err != nil {
			klog.ErrorS(err, "periodic backup failed", "volume", vol)
		}
	}
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
