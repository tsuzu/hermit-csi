package driver

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
	mount "k8s.io/mount-utils"
	utilexec "k8s.io/utils/exec"

	"github.com/tsuzu/hermit-csi/pkg/backup"
	"github.com/tsuzu/hermit-csi/pkg/lvm"
	"github.com/tsuzu/hermit-csi/pkg/manifest"
	"github.com/tsuzu/hermit-csi/pkg/store"
)

// ErrFenced means another node now owns the volume; this node must not write.
var ErrFenced = errors.New("fenced: volume is owned by another node")

// thinVG is the LVM surface the node uses (*lvm.VG implements it).
type thinVG interface {
	CreateThin(ctx context.Context, name string, size int64) error
	Snapshot(ctx context.Context, origin, name string, tags ...string) error
	Activate(ctx context.Context, name string) error
	Deactivate(ctx context.Context, name string) error
	Remove(ctx context.Context, name string) error
	AddTag(ctx context.Context, name, tag string) error
	Get(ctx context.Context, name string) (*lvm.LV, error)
	List(ctx context.Context) ([]lvm.LV, error)
	DevicePath(name string) string
	ChangedRanges(ctx context.Context, oldID, newID int) ([]backup.Range, error)
	MappedRanges(ctx context.Context, id int) ([]backup.Range, error)
}

var _ thinVG = (*lvm.VG)(nil)

// Node implements csi.NodeServer plus the backup loop and snapshot agent.
type Node struct {
	csi.UnimplementedNodeServer
	cfg     Config
	s       store.ObjectStore
	vg      thinVG
	eng     *backup.Engine
	states  stateStore
	mounter *mount.SafeFormatAndMount
	locks   keyLock
	freeze  func(ctx context.Context, path string, freeze bool) error
	now     func() time.Time
	m       *metrics

	opMu sync.Mutex
	ops  map[string]*op

	// Start of the last periodic backup per volume (in memory: after a
	// restart every staged volume is backed up on the first tick).
	schedMu    sync.Mutex
	lastBackup map[string]time.Time
}

type op struct {
	done chan struct{}
	err  error
}

func NewNode(cfg Config) (*Node, error) {
	if cfg.NodeID == "" {
		return nil, errors.New("node id is required")
	}
	if err := os.MkdirAll(cfg.StateDir, 0o700); err != nil {
		return nil, err
	}
	return newNode(cfg, cfg.VG), nil
}

func newNode(cfg Config, vg thinVG) *Node {
	m := newMetrics("node", cfg.Version)
	s := instrumentStore(cfg.Store, m)
	n := &Node{
		cfg:     cfg,
		s:       s,
		vg:      vg,
		m:       m,
		eng:     &backup.Engine{S: s, Parallel: cfg.Parallel, GCGrace: cfg.GCGrace},
		states:  stateStore{dir: cfg.StateDir},
		mounter: &mount.SafeFormatAndMount{Interface: mount.New(""), Exec: utilexec.New()},
		freeze:  fsfreeze,
		now:     time.Now,
		ops:     map[string]*op{},

		lastBackup: map[string]time.Time{},
	}
	m.reg.MustRegister(nodeCollector{n})
	return n
}

func fsfreeze(ctx context.Context, path string, freeze bool) error {
	flag := "-u"
	if freeze {
		flag = "-f"
	}
	_, err := lvm.DefaultRunner(ctx, "fsfreeze", flag, path)
	return err
}

func snapName(vol string, gen int64) string { return vol + "-b" + strconv.FormatInt(gen, 10) }

func isSnapOf(vol, lv string) bool {
	rest, ok := strings.CutPrefix(lv, vol+"-b")
	if !ok || rest == "" {
		return false
	}
	_, err := strconv.ParseInt(rest, 10, 64)
	return err == nil
}

func (n *Node) NodeGetCapabilities(context.Context, *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{Capabilities: []*csi.NodeServiceCapability{{
		Type: &csi.NodeServiceCapability_Rpc{Rpc: &csi.NodeServiceCapability_RPC{
			Type: csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
		}},
	}}}, nil
}

func (n *Node) NodeGetInfo(context.Context, *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	// No AccessibleTopology: any node can host any volume.
	return &csi.NodeGetInfoResponse{NodeId: n.cfg.NodeID}, nil
}

// ---------------------------------------------------------------- stage

// runOp runs fn detached from the RPC context so a long restore survives
// kubelet timeouts. Callers that time out get Aborted and simply retry; a
// retry while fn is still running waits for the same operation.
func (n *Node) runOp(ctx context.Context, vol string, fn func(context.Context) error) error {
	n.opMu.Lock()
	o, ok := n.ops[vol]
	if !ok {
		o = &op{done: make(chan struct{})}
		n.ops[vol] = o
		go func() {
			o.err = fn(context.Background())
			n.opMu.Lock()
			delete(n.ops, vol)
			n.opMu.Unlock()
			close(o.done)
		}()
	}
	n.opMu.Unlock()
	select {
	case <-o.done:
		return o.err
	case <-ctx.Done():
		return status.Errorf(codes.Aborted, "operation on %s still in progress", vol)
	}
}

func (n *Node) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	vol, staging := req.GetVolumeId(), req.GetStagingTargetPath()
	if err := checkID("volume id", vol); err != nil {
		return nil, err
	}
	if staging == "" {
		return nil, status.Error(codes.InvalidArgument, "staging target path missing")
	}
	if err := validateCaps([]*csi.VolumeCapability{req.GetVolumeCapability()}); err != nil {
		return nil, err
	}
	epoch, err := strconv.ParseInt(req.GetPublishContext()[publishContextEpoch], 10, 64)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "publish context lacks %s", publishContextEpoch)
	}
	block := req.GetVolumeCapability().GetBlock() != nil
	interval, err := backupIntervalFrom(req.GetVolumeContext())
	if err != nil {
		return nil, err
	}

	if err := n.runOp(ctx, vol, func(octx context.Context) error {
		return n.stage(octx, vol, epoch, block, staging, interval)
	}); err != nil {
		return nil, toStatus(err)
	}

	if !block {
		mnt := req.GetVolumeCapability().GetMount()
		fsType := mnt.GetFsType()
		if fsType == "" {
			fsType = "ext4"
		}
		if err := os.MkdirAll(staging, 0o750); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		notMnt, err := n.mounter.IsLikelyNotMountPoint(staging)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		if notMnt {
			if err := n.mounter.FormatAndMount(n.vg.DevicePath(vol), staging, fsType, mnt.GetMountFlags()); err != nil {
				return nil, status.Errorf(codes.Internal, "format/mount: %v", err)
			}
		}
	}
	return &csi.NodeStageVolumeResponse{}, nil
}

// stage makes the local LV match latest.json, restoring from S3 if needed.
func (n *Node) stage(ctx context.Context, vol string, epoch int64, block bool, staging string, interval time.Duration) error {
	defer n.locks.lock(vol)()

	o, oetag, err := manifest.LoadOwner(ctx, n.s, vol)
	if err != nil {
		return err
	}
	if o.NodeID != n.cfg.NodeID || o.Epoch != epoch {
		return status.Errorf(codes.FailedPrecondition, "volume %s is owned by %q epoch %d, not us (epoch %d)", vol, o.NodeID, o.Epoch, epoch)
	}
	m, etag, err := manifest.Load(ctx, n.s, manifest.LatestKey(vol))
	if err != nil {
		return err
	}
	if m.Epoch != epoch {
		return status.Errorf(codes.Aborted, "latest.json epoch %d != %d, controller has not fenced yet", m.Epoch, epoch)
	}

	st, err := n.states.load(vol)
	if err != nil {
		return err
	}
	reuse, err := n.canReuse(ctx, st, m, etag)
	if err != nil {
		return err
	}
	source := "local"
	if reuse {
		klog.InfoS("local copy is current, skipping restore", "volume", vol, "generation", m.Generation)
		if err := n.vg.Activate(ctx, vol); err != nil {
			return err
		}
	} else {
		if err := n.restore(ctx, vol, m); err != nil {
			return err
		}
		st = &volState{VolumeID: vol, Generation: m.Generation, BaseSnap: snapName(vol, m.Generation)}
		source = "restore"
	}
	st.Epoch, st.ETag, st.Fenced = epoch, etag, false
	st.Staged, st.Block, st.Staging = true, block, staging
	st.BackupInterval = ""
	if interval > 0 {
		st.BackupInterval = interval.String()
	}
	// The local LV now equals latest.json: S3 is in sync as of now, and the
	// next periodic backup is due one interval from now.
	now := n.now()
	st.LastBackup, st.BackupFailures = now, 0
	if err := n.states.save(st); err != nil {
		return err
	}
	n.markBackup(vol, now)
	n.m.stages.WithLabelValues(source).Inc()

	// Advertise the snapshot agent address for in-use snapshots.
	if n.cfg.AgentAdvertise != "" && o.Address != n.cfg.AgentAdvertise {
		o.Address = n.cfg.AgentAdvertise
		if _, err := manifest.SaveOwner(ctx, n.s, vol, o, store.PutOpts{IfMatch: oetag}); err != nil {
			klog.ErrorS(err, "could not record agent address", "volume", vol)
		}
	}
	return nil
}

func (n *Node) canReuse(ctx context.Context, st *volState, m *manifest.Manifest, etag string) (bool, error) {
	if st == nil || st.BaseSnap == "" {
		return false, nil
	}
	if st.Generation != m.Generation {
		if ours, err := n.adoptOwnWrite(ctx, st, m, etag); err != nil || !ours {
			return false, err
		}
	}
	lv, err := n.vg.Get(ctx, m.VolumeID)
	if err != nil || lv == nil || lv.Size != m.SizeBytes {
		return false, err
	}
	base, err := n.vg.Get(ctx, st.BaseSnap)
	return base != nil, err
}

// removeLocal deletes the volume LV and all its snapshots.
func (n *Node) removeLocal(ctx context.Context, vol string) error {
	lvs, err := n.vg.List(ctx)
	if err != nil {
		return err
	}
	for _, lv := range lvs {
		if lv.Name == vol || isSnapOf(vol, lv.Name) {
			if err := n.vg.Remove(ctx, lv.Name); err != nil {
				return err
			}
		}
	}
	return nil
}

func (n *Node) restore(ctx context.Context, vol string, m *manifest.Manifest) error {
	start := time.Now()
	if err := n.restoreLV(ctx, vol, m); err != nil {
		n.m.restores.WithLabelValues(resultFailure).Inc()
		return err
	}
	n.m.restores.WithLabelValues(resultSuccess).Inc()
	n.m.restoreDur.Observe(time.Since(start).Seconds())
	n.m.restoredBytes.Add(float64(m.UsedBytes()))
	return nil
}

func (n *Node) restoreLV(ctx context.Context, vol string, m *manifest.Manifest) error {
	start := time.Now()
	klog.InfoS("restoring volume from S3", "volume", vol, "generation", m.Generation, "used", m.UsedBytes())
	if err := n.removeLocal(ctx, vol); err != nil {
		return err
	}
	if err := n.states.remove(vol); err != nil {
		return err
	}
	if err := n.vg.CreateThin(ctx, vol, m.SizeBytes); err != nil {
		return err
	}
	f, err := os.OpenFile(n.vg.DevicePath(vol), os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	last := time.Now()
	err = n.eng.Restore(ctx, f, m, func(done, total int64) {
		if time.Since(last) > 10*time.Second {
			last = time.Now()
			klog.InfoS("restore progress", "volume", vol, "chunks", done, "total", total)
		}
	})
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = n.vg.Remove(context.Background(), vol) // do not leave a half-restored LV around
		return fmt.Errorf("restore %s: %w", vol, err)
	}
	// The restored LV equals generation m.Generation: snapshot it as the
	// base, so the next backup is incremental instead of a full rescan.
	if err := n.vg.Snapshot(ctx, vol, snapName(vol, m.Generation), lvm.TagBase); err != nil {
		return err
	}
	klog.InfoS("restore complete", "volume", vol, "took", time.Since(start).Round(time.Second))
	return nil
}

func (n *Node) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	vol, staging := req.GetVolumeId(), req.GetStagingTargetPath()
	if err := checkID("volume id", vol); err != nil {
		return nil, err
	}
	err := n.runOp(ctx, vol, func(octx context.Context) error { return n.unstage(octx, vol, staging) })
	if err != nil {
		return nil, toStatus(err)
	}
	return &csi.NodeUnstageVolumeResponse{}, nil
}

func (n *Node) unstage(ctx context.Context, vol, staging string) error {
	defer n.locks.lock(vol)()
	st, err := n.states.load(vol)
	if err != nil {
		return err
	}
	if staging != "" && (st == nil || !st.Block) {
		if err := mount.CleanupMountPoint(staging, n.mounter, false); err != nil {
			return err
		}
	}
	if st == nil || !st.Staged {
		return nil
	}
	n.forgetBackup(vol)
	st.Staged = false // no fsfreeze for the final backup: the fs is unmounted
	if _, err := n.backupLocked(ctx, st, triggerUnstage); err != nil {
		if !errors.Is(err, ErrFenced) {
			return fmt.Errorf("final backup of %s: %w", vol, err) // kubelet retries
		}
		klog.InfoS("volume fenced, skipping final backup", "volume", vol)
	}
	if err := n.states.save(st); err != nil {
		return err
	}
	// Keep the LV (fast path if the volume comes back here) but deactivate it.
	if err := n.vg.Deactivate(ctx, vol); err != nil {
		klog.ErrorS(err, "deactivate failed", "volume", vol)
	}
	return nil
}

// ---------------------------------------------------------------- publish

func (n *Node) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	vol, target, staging := req.GetVolumeId(), req.GetTargetPath(), req.GetStagingTargetPath()
	if err := checkID("volume id", vol); err != nil {
		return nil, err
	}
	if target == "" {
		return nil, status.Error(codes.InvalidArgument, "target path missing")
	}
	if err := validateCaps([]*csi.VolumeCapability{req.GetVolumeCapability()}); err != nil {
		return nil, err
	}
	opts := []string{"bind"}
	if req.GetReadonly() {
		opts = append(opts, "ro")
	}

	var source string
	if req.GetVolumeCapability().GetBlock() != nil {
		// Raw block: bind-mount the device node onto a file.
		source = n.vg.DevicePath(vol)
		if err := os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		f, err := os.OpenFile(target, os.O_CREATE, 0o660)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		f.Close()
	} else {
		if staging == "" {
			return nil, status.Error(codes.InvalidArgument, "staging target path missing")
		}
		source = staging
		if err := os.MkdirAll(target, 0o750); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	notMnt, err := n.mounter.IsLikelyNotMountPoint(target)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if notMnt {
		if err := n.mounter.Mount(source, target, "", opts); err != nil {
			return nil, status.Errorf(codes.Internal, "bind mount %s -> %s: %v", source, target, err)
		}
	}
	return &csi.NodePublishVolumeResponse{}, nil
}

func (n *Node) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	if req.GetTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "target path missing")
	}
	if err := mount.CleanupMountPoint(req.GetTargetPath(), n.mounter, true); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &csi.NodeUnpublishVolumeResponse{}, nil
}
