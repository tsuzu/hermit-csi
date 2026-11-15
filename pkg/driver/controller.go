package driver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/klog/v2"

	"github.com/tsuzu/s3lvm-csi/pkg/backup"
	"github.com/tsuzu/s3lvm-csi/pkg/manifest"
	"github.com/tsuzu/s3lvm-csi/pkg/store"
)

// Controller implements csi.ControllerServer. It never touches block devices:
// volumes and snapshots are just manifests in S3, and attach/detach is an
// ownership record with a fencing epoch.
type Controller struct {
	csi.UnimplementedControllerServer
	cfg   Config
	s     store.ObjectStore
	locks keyLock
	http  *http.Client
}

func NewController(cfg Config) *Controller {
	return &Controller{cfg: cfg, s: cfg.Store, http: &http.Client{Timeout: 30 * time.Minute}}
}

func (c *Controller) ControllerGetCapabilities(context.Context, *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	var caps []*csi.ControllerServiceCapability
	for _, t := range []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
		csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS,
		csi.ControllerServiceCapability_RPC_CLONE_VOLUME,
	} {
		caps = append(caps, &csi.ControllerServiceCapability{Type: &csi.ControllerServiceCapability_Rpc{
			Rpc: &csi.ControllerServiceCapability_RPC{Type: t},
		}})
	}
	return &csi.ControllerGetCapabilitiesResponse{Capabilities: caps}, nil
}

// ---------------------------------------------------------------- volumes

func (c *Controller) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	vol := req.GetName()
	if err := checkID("volume name", vol); err != nil {
		return nil, err
	}
	if err := validateCaps(req.GetVolumeCapabilities()); err != nil {
		return nil, err
	}
	defer c.locks.lock(vol)()

	size := req.GetCapacityRange().GetRequiredBytes()
	if size <= 0 {
		size = gib
	}
	limit := req.GetCapacityRange().GetLimitBytes()

	// Idempotency: the volume may already exist from a previous attempt.
	if m, _, err := manifest.Load(ctx, c.s, manifest.LatestKey(vol)); err == nil {
		if m.SizeBytes < size || (limit > 0 && m.SizeBytes > limit) {
			return nil, status.Errorf(codes.AlreadyExists, "volume %s exists with size %d", vol, m.SizeBytes)
		}
		return c.volumeResponse(m, req.GetVolumeContentSource()), nil
	} else if !errors.Is(err, store.ErrNotFound) {
		return nil, toStatus(err)
	}

	var m *manifest.Manifest
	src := req.GetVolumeContentSource()
	switch {
	case src.GetSnapshot() != nil:
		id := src.GetSnapshot().GetSnapshotId()
		sm, _, err := manifest.Load(ctx, c.s, manifest.SnapshotKey(id))
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return nil, status.Errorf(codes.NotFound, "snapshot %s not found", id)
			}
			return nil, toStatus(err)
		}
		m = sm.Clone()
	case src.GetVolume() != nil:
		// Clone = the source volume as of its last backup (crash-consistent).
		id := src.GetVolume().GetVolumeId()
		lm, _, err := manifest.Load(ctx, c.s, manifest.LatestKey(id))
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return nil, status.Errorf(codes.NotFound, "volume %s not found", id)
			}
			return nil, toStatus(err)
		}
		m = lm.Clone()
	default:
		m = manifest.New(vol, alignSize(size, c.cfg.ChunkSize), c.cfg.ChunkSize)
	}

	if src != nil {
		want := alignSize(max(size, m.SizeBytes), m.ChunkSize)
		if err := m.Resize(want); err != nil {
			return nil, status.Error(codes.OutOfRange, err.Error())
		}
	}
	if limit > 0 && m.SizeBytes > limit {
		return nil, status.Errorf(codes.OutOfRange, "aligned size %d exceeds limit %d", m.SizeBytes, limit)
	}
	m.VolumeID, m.SnapshotID, m.SourceVolumeID = vol, "", ""
	m.Generation, m.Epoch = 1, 0
	m.CreatedAt = time.Now().UTC()

	// Owner record first, then latest.json as the commit point.
	if _, err := manifest.SaveOwner(ctx, c.s, vol, &manifest.Owner{}, store.PutOpts{IfNoneMatch: true}); err != nil &&
		!errors.Is(err, store.ErrPrecondition) {
		return nil, toStatus(err)
	}
	if _, err := manifest.Save(ctx, c.s, manifest.LatestKey(vol), m, store.PutOpts{IfNoneMatch: true}); err != nil {
		if errors.Is(err, store.ErrPrecondition) {
			return nil, status.Errorf(codes.Aborted, "volume %s is being created concurrently", vol)
		}
		return nil, toStatus(err)
	}
	klog.InfoS("volume created", "volume", vol, "size", m.SizeBytes, "chunks", m.NumChunks(), "used", m.UsedBytes())
	return c.volumeResponse(m, src), nil
}

func (c *Controller) volumeResponse(m *manifest.Manifest, src *csi.VolumeContentSource) *csi.CreateVolumeResponse {
	return &csi.CreateVolumeResponse{Volume: &csi.Volume{
		VolumeId:      m.VolumeID,
		CapacityBytes: m.SizeBytes,
		ContentSource: src,
		// No AccessibleTopology: the volume can be staged on any node.
	}}
}

func (c *Controller) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	vol := req.GetVolumeId()
	if err := checkID("volume id", vol); err != nil {
		return nil, err
	}
	defer c.locks.lock(vol)()
	if o, _, err := manifest.LoadOwner(ctx, c.s, vol); err == nil && o.NodeID != "" {
		return nil, status.Errorf(codes.FailedPrecondition, "volume %s is still attached to %s", vol, o.NodeID)
	}
	// Chunks are reclaimed by GC; node agents remove their local LVs when they
	// notice latest.json is gone.
	if err := c.s.Delete(ctx, manifest.LatestKey(vol)); err != nil {
		return nil, toStatus(err)
	}
	if err := c.s.Delete(ctx, manifest.OwnerKey(vol)); err != nil {
		return nil, toStatus(err)
	}
	klog.InfoS("volume deleted", "volume", vol)
	return &csi.DeleteVolumeResponse{}, nil
}

func (c *Controller) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	if _, err := c.s.Head(ctx, manifest.LatestKey(req.GetVolumeId())); err != nil {
		return nil, toStatus(err)
	}
	if err := validateCaps(req.GetVolumeCapabilities()); err != nil {
		return &csi.ValidateVolumeCapabilitiesResponse{Message: err.Error()}, nil
	}
	return &csi.ValidateVolumeCapabilitiesResponse{Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
		VolumeCapabilities: req.GetVolumeCapabilities(),
	}}, nil
}

// ---------------------------------------------------------------- attach / detach (ownership + fencing)

// ControllerPublishVolume makes nodeID the single owner of the volume and
// bumps the epoch. Bumping the epoch rewrites latest.json, which changes its
// ETag, so any previous owner's conditional write (If-Match) now fails.
func (c *Controller) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	vol, node := req.GetVolumeId(), req.GetNodeId()
	if err := checkID("volume id", vol); err != nil {
		return nil, err
	}
	if node == "" {
		return nil, status.Error(codes.InvalidArgument, "node id missing")
	}
	defer c.locks.lock(vol)()

	o, etag, err := manifest.LoadOwner(ctx, c.s, vol)
	if err != nil {
		return nil, toStatus(err)
	}
	switch o.NodeID {
	case node:
		if err := c.fence(ctx, vol, o.Epoch); err != nil {
			return nil, toStatus(err)
		}
		return publishResponse(o.Epoch), nil
	case "":
	default:
		return nil, status.Errorf(codes.FailedPrecondition, "volume %s is attached to node %s", vol, o.NodeID)
	}
	next := &manifest.Owner{NodeID: node, Epoch: o.Epoch + 1}
	if _, err := manifest.SaveOwner(ctx, c.s, vol, next, store.PutOpts{IfMatch: etag}); err != nil {
		return nil, toStatus(err)
	}
	if err := c.fence(ctx, vol, next.Epoch); err != nil {
		return nil, toStatus(err)
	}
	klog.InfoS("volume attached", "volume", vol, "node", node, "epoch", next.Epoch)
	return publishResponse(next.Epoch), nil
}

func publishResponse(epoch int64) *csi.ControllerPublishVolumeResponse {
	return &csi.ControllerPublishVolumeResponse{PublishContext: map[string]string{
		publishContextEpoch: strconv.FormatInt(epoch, 10),
	}}
}

// ControllerUnpublishVolume releases ownership. It is also what Kubernetes
// calls on force-detach after a node failure; bumping the epoch fences the
// (possibly still running) old node.
func (c *Controller) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	vol, node := req.GetVolumeId(), req.GetNodeId()
	if err := checkID("volume id", vol); err != nil {
		return nil, err
	}
	defer c.locks.lock(vol)()
	o, etag, err := manifest.LoadOwner(ctx, c.s, vol)
	if errors.Is(err, store.ErrNotFound) {
		return &csi.ControllerUnpublishVolumeResponse{}, nil
	}
	if err != nil {
		return nil, toStatus(err)
	}
	if o.NodeID == "" || (node != "" && o.NodeID != node) {
		return &csi.ControllerUnpublishVolumeResponse{}, nil
	}
	next := &manifest.Owner{Epoch: o.Epoch + 1}
	if _, err := manifest.SaveOwner(ctx, c.s, vol, next, store.PutOpts{IfMatch: etag}); err != nil {
		return nil, toStatus(err)
	}
	if err := c.fence(ctx, vol, next.Epoch); err != nil {
		return nil, toStatus(err)
	}
	klog.InfoS("volume detached", "volume", vol, "node", o.NodeID, "epoch", next.Epoch)
	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

// fence makes sure latest.json carries at least the given epoch.
func (c *Controller) fence(ctx context.Context, vol string, epoch int64) error {
	for i := 0; i < 10; i++ {
		m, etag, err := manifest.Load(ctx, c.s, manifest.LatestKey(vol))
		if err != nil {
			return err
		}
		if m.Epoch >= epoch {
			return nil
		}
		m.Epoch = epoch
		if _, err := manifest.Save(ctx, c.s, manifest.LatestKey(vol), m, store.PutOpts{IfMatch: etag}); err == nil {
			return nil
		} else if !errors.Is(err, store.ErrPrecondition) {
			return err
		}
		// A backup from the old owner raced us; re-read and retry.
	}
	return fmt.Errorf("fence %s: too many conflicts", vol)
}

// ---------------------------------------------------------------- snapshots

func (c *Controller) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	snap, vol := req.GetName(), req.GetSourceVolumeId()
	if err := checkID("snapshot name", snap); err != nil {
		return nil, err
	}
	if err := checkID("source volume id", vol); err != nil {
		return nil, err
	}
	defer c.locks.lock(vol)()

	if sm, _, err := manifest.Load(ctx, c.s, manifest.SnapshotKey(snap)); err == nil {
		if sm.SourceVolumeID != vol {
			return nil, status.Errorf(codes.AlreadyExists, "snapshot %s exists for volume %s", snap, sm.SourceVolumeID)
		}
		return snapshotResponse(sm), nil
	} else if !errors.Is(err, store.ErrNotFound) {
		return nil, toStatus(err)
	}

	o, _, err := manifest.LoadOwner(ctx, c.s, vol)
	if err != nil {
		return nil, toStatus(err)
	}
	if o.NodeID == "" {
		// Detached: NodeUnstage already flushed everything, latest.json is exact.
		lm, _, err := manifest.Load(ctx, c.s, manifest.LatestKey(vol))
		if err != nil {
			return nil, toStatus(err)
		}
		sm := lm.Clone()
		sm.SnapshotID, sm.SourceVolumeID, sm.CreatedAt = snap, vol, time.Now().UTC()
		if _, err := manifest.Save(ctx, c.s, manifest.SnapshotKey(snap), sm, store.PutOpts{IfNoneMatch: true}); err != nil &&
			!errors.Is(err, store.ErrPrecondition) {
			return nil, toStatus(err)
		}
	} else {
		// In use: ask the owning node to take an LVM snapshot and upload the delta.
		if o.Address == "" {
			return nil, status.Errorf(codes.Unavailable, "volume %s is attached to %s but not staged yet", vol, o.NodeID)
		}
		if err := c.requestNodeSnapshot(ctx, o.Address, vol, snap); err != nil {
			return nil, err
		}
	}
	sm, _, err := manifest.Load(ctx, c.s, manifest.SnapshotKey(snap))
	if err != nil {
		return nil, toStatus(err)
	}
	klog.InfoS("snapshot created", "snapshot", snap, "volume", vol, "generation", sm.Generation)
	return snapshotResponse(sm), nil
}

func (c *Controller) requestNodeSnapshot(ctx context.Context, addr, vol, snap string) error {
	body, _ := json.Marshal(agentSnapshotRequest{VolumeID: vol, SnapshotID: snap})
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+agentSnapshotPath, bytes.NewReader(body))
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	r.Header.Set("Content-Type", "application/json")
	if c.cfg.AgentToken != "" {
		r.Header.Set("Authorization", "Bearer "+c.cfg.AgentToken)
	}
	resp, err := c.http.Do(r)
	if err != nil {
		return status.Errorf(codes.Unavailable, "node agent %s: %v", addr, err)
	}
	defer resp.Body.Close()
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	switch resp.StatusCode {
	case http.StatusOK:
		return nil
	case http.StatusConflict:
		return status.Errorf(codes.Unavailable, "node agent %s: %s", addr, msg)
	default:
		return status.Errorf(codes.Internal, "node agent %s: %s: %s", addr, resp.Status, msg)
	}
}

func snapshotResponse(m *manifest.Manifest) *csi.CreateSnapshotResponse {
	return &csi.CreateSnapshotResponse{Snapshot: csiSnapshot(m)}
}

func csiSnapshot(m *manifest.Manifest) *csi.Snapshot {
	return &csi.Snapshot{
		SnapshotId:     m.SnapshotID,
		SourceVolumeId: m.SourceVolumeID,
		SizeBytes:      m.SizeBytes, // minimum size of a volume restored from it
		CreationTime:   timestamppb.New(m.CreatedAt),
		ReadyToUse:     true,
	}
}

func (c *Controller) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	if err := checkID("snapshot id", req.GetSnapshotId()); err != nil {
		return nil, err
	}
	if err := c.s.Delete(ctx, manifest.SnapshotKey(req.GetSnapshotId())); err != nil {
		return nil, toStatus(err)
	}
	return &csi.DeleteSnapshotResponse{}, nil
}

func (c *Controller) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	var entries []*csi.ListSnapshotsResponse_Entry
	add := func(key string) error {
		m, _, err := manifest.Load(ctx, c.s, key)
		if errors.Is(err, store.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if src := req.GetSourceVolumeId(); src != "" && m.SourceVolumeID != src {
			return nil
		}
		entries = append(entries, &csi.ListSnapshotsResponse_Entry{Snapshot: csiSnapshot(m)})
		return nil
	}
	var err error
	if id := req.GetSnapshotId(); id != "" {
		err = add(manifest.SnapshotKey(id))
	} else {
		err = c.s.List(ctx, manifest.SnapshotPrefix, func(o store.ObjectInfo) error {
			if !manifest.IsManifestKey(o.Key) {
				return nil
			}
			return add(o.Key)
		})
	}
	if err != nil {
		return nil, toStatus(err)
	}
	return &csi.ListSnapshotsResponse{Entries: entries}, nil
}

// ---------------------------------------------------------------- GC

func (c *Controller) gcLoop(ctx context.Context) {
	if c.cfg.GCInterval <= 0 {
		return
	}
	t := time.NewTicker(c.cfg.GCInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		n, err := backup.GC(ctx, c.s, c.cfg.GCGrace, time.Now())
		if err != nil {
			klog.ErrorS(err, "chunk GC failed")
			continue
		}
		klog.InfoS("chunk GC done", "deleted", n)
	}
}
