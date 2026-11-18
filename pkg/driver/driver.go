// Package driver implements the CSI Identity, Controller and Node services.
package driver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"

	"github.com/tsuzu/hermit-csi/pkg/lvm"
	"github.com/tsuzu/hermit-csi/pkg/store"
)

const (
	DefaultName = "hermit.csi.tsuzu.dev"
	mib         = int64(1) << 20
	gib         = int64(1) << 30

	// publishContextEpoch carries the ownership epoch from ControllerPublish to NodeStage.
	publishContextEpoch = "hermit.csi.tsuzu.dev/epoch"

	// ParamBackupInterval is the StorageClass parameter (and volume context
	// key) that overrides the node's --backup-interval for one volume.
	ParamBackupInterval = "backupInterval"
	// MinBackupInterval is the shortest per-volume interval, and the
	// granularity of the node's backup scheduler.
	MinBackupInterval = 10 * time.Second
)

// parseBackupInterval validates a per-volume backup interval ("30s", "2m", ...).
func parseBackupInterval(v string) (time.Duration, error) {
	d, err := time.ParseDuration(strings.TrimSpace(v))
	if err != nil {
		return 0, fmt.Errorf("%s %q: %w", ParamBackupInterval, v, err)
	}
	if d < MinBackupInterval {
		return 0, fmt.Errorf("%s %q: must be at least %s", ParamBackupInterval, v, MinBackupInterval)
	}
	return d, nil
}

// backupIntervalFrom reads the per-volume interval from StorageClass
// parameters or a volume context; 0 means "use the node default".
func backupIntervalFrom(m map[string]string) (time.Duration, error) {
	v, ok := m[ParamBackupInterval]
	if !ok {
		return 0, nil
	}
	d, err := parseBackupInterval(v)
	if err != nil {
		return 0, status.Error(codes.InvalidArgument, err.Error())
	}
	return d, nil
}

// Config is shared by both modes; fields irrelevant to a mode are ignored.
type Config struct {
	Name     string
	Version  string
	Endpoint string // unix:///csi/csi.sock
	Store    store.ObjectStore

	ChunkSize int64 // bytes, for new volumes
	Parallel  int   // concurrent chunk transfers
	GCGrace   time.Duration

	// controller
	GCInterval time.Duration
	AgentToken string

	// node
	NodeID         string
	VG             *lvm.VG
	StateDir       string
	BackupInterval time.Duration
	// TrimInterval is how often fstrim runs on each mounted filesystem
	// volume, so blocks of deleted files go back to the thin pool (0 = off).
	TrimInterval   time.Duration
	AgentListen    string // e.g. ":9810"
	AgentAdvertise string // e.g. "10.0.0.12:9810" (pod IP)

	// MetricsListen serves Prometheus metrics on /metrics, e.g. ":9811" ("" = off).
	MetricsListen string
}

var validID = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{0,95}$`)

func checkID(kind, id string) error {
	if !validID.MatchString(id) {
		return status.Errorf(codes.InvalidArgument, "invalid %s %q", kind, id)
	}
	return nil
}

// toStatus maps internal errors to gRPC codes, keeping existing status errors.
func toStatus(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	switch {
	case errors.Is(err, store.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, store.ErrPrecondition):
		return status.Error(codes.Aborted, err.Error())
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return status.Error(codes.DeadlineExceeded, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}

func logInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
	klog.V(4).InfoS("gRPC call", "method", info.FullMethod)
	resp, err := h(ctx, req)
	if err != nil {
		klog.ErrorS(err, "gRPC error", "method", info.FullMethod)
	} else {
		klog.V(4).InfoS("gRPC ok", "method", info.FullMethod)
	}
	return resp, err
}

func listen(endpoint string) (net.Listener, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	switch u.Scheme {
	case "unix":
		p := u.Path
		if p == "" {
			p = u.Host
		}
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			return nil, err
		}
		return net.Listen("unix", p)
	case "tcp":
		return net.Listen("tcp", u.Host)
	}
	return nil, fmt.Errorf("unsupported endpoint %q", endpoint)
}

func serve(ctx context.Context, endpoint string, m *metrics, register func(*grpc.Server)) error {
	l, err := listen(endpoint)
	if err != nil {
		return err
	}
	srv := grpc.NewServer(grpc.UnaryInterceptor(m.interceptor))
	register(srv)
	go func() {
		<-ctx.Done()
		srv.GracefulStop()
	}()
	klog.InfoS("CSI gRPC server listening", "endpoint", endpoint)
	return srv.Serve(l)
}

// RunController runs the controller service (and the chunk GC loop).
func RunController(ctx context.Context, cfg Config) error {
	c := NewController(cfg)
	go c.gcLoop(ctx)
	go serveMetrics(ctx, cfg.MetricsListen, c.m)
	return serve(ctx, cfg.Endpoint, c.m, func(s *grpc.Server) {
		csi.RegisterIdentityServer(s, &identity{cfg: cfg})
		csi.RegisterControllerServer(s, c)
	})
}

// RunNode runs the node service, the periodic backup loop and the snapshot agent.
func RunNode(ctx context.Context, cfg Config) error {
	n, err := NewNode(cfg)
	if err != nil {
		return err
	}
	if cfg.VG != nil {
		if err := cfg.VG.CheckTargets(ctx); err != nil {
			return err
		}
	}
	go n.backupLoop(ctx)
	go serveMetrics(ctx, cfg.MetricsListen, n.m)
	go func() {
		if err := n.serveAgent(ctx); err != nil {
			klog.ErrorS(err, "snapshot agent stopped")
		}
	}()
	return serve(ctx, cfg.Endpoint, n.m, func(s *grpc.Server) {
		csi.RegisterIdentityServer(s, &identity{cfg: cfg})
		csi.RegisterNodeServer(s, n)
	})
}

// keyLock serializes operations per volume.
type keyLock struct {
	mu sync.Mutex
	m  map[string]*sync.Mutex
}

func (k *keyLock) lock(key string) func() {
	k.mu.Lock()
	if k.m == nil {
		k.m = map[string]*sync.Mutex{}
	}
	l, ok := k.m[key]
	if !ok {
		l = &sync.Mutex{}
		k.m[key] = l
	}
	k.mu.Unlock()
	l.Lock()
	return l.Unlock
}

// alignSize rounds size up to a multiple of both the chunk size and the LVM
// extent size (4 MiB), so the LV size equals the manifest size exactly.
func alignSize(size, chunk int64) int64 {
	a := lcm(chunk, 4*mib)
	return (size + a - 1) / a * a
}

func gcd(a, b int64) int64 {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

func lcm(a, b int64) int64 { return a / gcd(a, b) * b }

// validateCaps accepts single-node access modes with block or mount access.
func validateCaps(caps []*csi.VolumeCapability) error {
	if len(caps) == 0 {
		return status.Error(codes.InvalidArgument, "volume capabilities missing")
	}
	for _, c := range caps {
		if c.GetBlock() == nil && c.GetMount() == nil {
			return status.Error(codes.InvalidArgument, "access type must be block or mount")
		}
		switch c.GetAccessMode().GetMode() {
		case csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER,
			csi.VolumeCapability_AccessMode_SINGLE_NODE_MULTI_WRITER,
			csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY:
		default:
			return status.Errorf(codes.InvalidArgument, "unsupported access mode %v", c.GetAccessMode().GetMode())
		}
	}
	return nil
}
