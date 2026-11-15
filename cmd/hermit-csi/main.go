// Command hermit-csi is a CSI driver that keeps volumes on local LVM thin
// volumes, continuously backs them up to S3 (incrementally, via thin_delta),
// and restores them automatically when a volume is staged on another node.
package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"k8s.io/klog/v2"

	"github.com/tsuzu/hermit-csi/pkg/driver"
	"github.com/tsuzu/hermit-csi/pkg/lvm"
	"github.com/tsuzu/hermit-csi/pkg/store"
)

var version = "dev"

func main() {
	klog.InitFlags(nil)
	var (
		mode       = flag.String("mode", "", "controller or node")
		name       = flag.String("driver-name", driver.DefaultName, "CSI driver name")
		endpoint   = flag.String("endpoint", "unix:///csi/csi.sock", "CSI endpoint")
		nodeID     = flag.String("node-id", os.Getenv("NODE_NAME"), "node id (node mode)")
		bucket     = flag.String("s3-bucket", "", "S3 bucket")
		prefix     = flag.String("s3-prefix", "", "S3 key prefix")
		s3Endpoint = flag.String("s3-endpoint", "", "S3 endpoint URL (MinIO, Ceph RGW, ...)")
		region     = flag.String("s3-region", "us-east-1", "S3 region")
		pathStyle  = flag.Bool("s3-path-style", false, "use path-style S3 URLs")
		chunkMiB   = flag.Int64("chunk-size-mib", 4, "chunk size for new volumes (MiB)")
		parallel   = flag.Int("parallel", 8, "concurrent chunk transfers")
		gcInterval = flag.Duration("gc-interval", time.Hour, "chunk GC interval (controller)")
		gcGrace    = flag.Duration("gc-grace", 24*time.Hour, "minimum age before an unreferenced chunk is deleted")
		vgName     = flag.String("vg", "hermit", "LVM volume group (node)")
		pool       = flag.String("thin-pool", "pool", "LVM thin pool in the VG (node)")
		stateDir   = flag.String("state-dir", "/var/lib/hermit", "local state directory (node)")
		interval   = flag.Duration("backup-interval", 5*time.Minute, "default periodic backup interval (node); the StorageClass parameter backupInterval overrides it per volume, 0 disables it for volumes without one")
		agentAddr  = flag.String("agent-listen", ":9810", "snapshot agent listen address (node)")
		advertise  = flag.String("agent-advertise", "", "snapshot agent address the controller should use, host:port (node)")
	)
	flag.Parse()

	if *bucket == "" {
		klog.Fatal("--s3-bucket is required")
	}
	if *chunkMiB <= 0 {
		klog.Fatal("--chunk-size-mib must be positive")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	s, err := store.New(ctx, store.Config{
		Bucket: *bucket, Prefix: *prefix, Endpoint: *s3Endpoint, Region: *region, PathStyle: *pathStyle,
	})
	if err != nil {
		klog.Fatal(err)
	}
	cfg := driver.Config{
		Name:           *name,
		Version:        version,
		Endpoint:       *endpoint,
		Store:          s,
		ChunkSize:      *chunkMiB << 20,
		Parallel:       *parallel,
		GCGrace:        *gcGrace,
		GCInterval:     *gcInterval,
		AgentToken:     os.Getenv("HERMIT_AGENT_TOKEN"),
		NodeID:         *nodeID,
		VG:             &lvm.VG{Name: *vgName, Pool: *pool},
		StateDir:       *stateDir,
		BackupInterval: *interval,
		AgentListen:    *agentAddr,
		AgentAdvertise: *advertise,
	}

	switch *mode {
	case "controller":
		err = driver.RunController(ctx, cfg)
	case "node":
		err = driver.RunNode(ctx, cfg)
	default:
		klog.Fatalf("unknown --mode %q (want controller or node)", *mode)
	}
	if err != nil {
		klog.Fatal(err)
	}
}
