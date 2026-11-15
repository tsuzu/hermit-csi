//go:build integration

// Integration tests: the same scenarios as the fake tests, but on real LVM
// thin pools (one VG per simulated node) and a real S3 endpoint (MinIO).
//
// Required environment (see .github/workflows/ci.yaml and hack/it-setup.sh):
//
//	S3LVM_IT_VG_A, S3LVM_IT_VG_B   volume groups, each with a thin pool
//	S3LVM_IT_POOL                  thin pool name (default "pool")
//	S3LVM_IT_S3_ENDPOINT           e.g. http://127.0.0.1:9000
//	S3LVM_IT_BUCKET                bucket name
//	AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
//
// Must run as root.
package driver

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/example/s3lvm-csi/pkg/lvm"
	"github.com/example/s3lvm-csi/pkg/store"
)

func itEnv(t *testing.T, k string) string {
	t.Helper()
	v := os.Getenv(k)
	if v == "" {
		t.Skipf("%s not set", k)
	}
	return v
}

func realStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.New(context.Background(), store.Config{
		Bucket:    itEnv(t, "S3LVM_IT_BUCKET"),
		Endpoint:  itEnv(t, "S3LVM_IT_S3_ENDPOINT"),
		Region:    "us-east-1",
		PathStyle: true,
		Prefix:    fmt.Sprintf("it/%s-%d", strings.ToLower(t.Name()), time.Now().UnixNano()),
	})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func realCluster(t *testing.T) *cluster {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Skip("integration tests need root")
	}
	pool := os.Getenv("S3LVM_IT_POOL")
	if pool == "" {
		pool = "pool"
	}
	vgNames := map[string]string{
		"node-a": itEnv(t, "S3LVM_IT_VG_A"),
		"node-b": itEnv(t, "S3LVM_IT_VG_B"),
	}
	s := realStore(t)
	var vgs []*lvm.VG
	c := newClusterWith(t, s, func(node string) thinVG {
		v := &lvm.VG{Name: vgNames[node], Pool: pool}
		vgs = append(vgs, v)
		return v
	}, "node-a", "node-b")
	t.Cleanup(func() {
		ctx := context.Background()
		for _, v := range vgs {
			lvs, err := v.List(ctx)
			if err != nil {
				t.Logf("cleanup list %s: %v", v.Name, err)
				continue
			}
			for _, lv := range lvs {
				if lv.Name != pool {
					if err := v.Remove(ctx, lv.Name); err != nil {
						t.Logf("cleanup %s/%s: %v", v.Name, lv.Name, err)
					}
				}
			}
		}
	})
	return c
}

func TestIntegrationStoreConditionalWrites(t *testing.T) {
	s := realStore(t)
	ctx := context.Background()
	e1, err := s.Put(ctx, "k", []byte("a"), store.PutOpts{IfNoneMatch: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Put(ctx, "k", []byte("b"), store.PutOpts{IfNoneMatch: true}); !errors.Is(err, store.ErrPrecondition) {
		t.Fatalf("If-None-Match on existing: got %v", err)
	}
	e2, err := s.Put(ctx, "k", []byte("c"), store.PutOpts{IfMatch: e1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Put(ctx, "k", []byte("d"), store.PutOpts{IfMatch: e1}); !errors.Is(err, store.ErrPrecondition) {
		t.Fatalf("stale If-Match: got %v", err)
	}
	b, et, err := s.Get(ctx, "k")
	if err != nil || string(b) != "c" || et != e2 {
		t.Fatalf("Get = %q %q %v (want etag %q)", b, et, err, e2)
	}
	if _, _, err := s.Get(ctx, "missing"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Get missing: %v", err)
	}
	if _, err := s.Head(ctx, "missing"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Head missing: %v", err)
	}
	var keys []string
	if err := s.List(ctx, "", func(o store.ObjectInfo) error { keys = append(keys, o.Key); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 || keys[0] != "k" {
		t.Fatalf("List = %v", keys)
	}
	if err := s.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, "k"); err != nil {
		t.Fatalf("delete missing: %v", err)
	}
}

func TestIntegrationMoveVolume(t *testing.T) { runMoveScenario(t, realCluster(t)) }
func TestIntegrationReattach(t *testing.T)   { runReattachScenario(t, realCluster(t)) }
func TestIntegrationFencing(t *testing.T)    { runFencingScenario(t, realCluster(t)) }
func TestIntegrationSnapshots(t *testing.T)  { runSnapshotScenario(t, realCluster(t)) }
