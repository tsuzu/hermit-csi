//go:build integration

// Integration tests: the same scenarios as the fake tests, but on real LVM
// thin pools (one VG per simulated node) and a real S3 endpoint (MinIO).
//
// Required environment (see .github/workflows/ci.yaml and hack/it-setup.sh):
//
//	HERMIT_IT_VG_A, HERMIT_IT_VG_B   volume groups, each with a thin pool
//	HERMIT_IT_POOL                  thin pool name (default "pool")
//	HERMIT_IT_S3_ENDPOINT           e.g. http://127.0.0.1:9000
//	HERMIT_IT_BUCKET                bucket name
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
	"sync"
	"testing"
	"time"

	"github.com/tsuzu/hermit-csi/pkg/lvm"
	"github.com/tsuzu/hermit-csi/pkg/store"
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
		Bucket:    itEnv(t, "HERMIT_IT_BUCKET"),
		Endpoint:  itEnv(t, "HERMIT_IT_S3_ENDPOINT"),
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
	pool := os.Getenv("HERMIT_IT_POOL")
	if pool == "" {
		pool = "pool"
	}
	vgNames := map[string]string{
		"node-a": itEnv(t, "HERMIT_IT_VG_A"),
		"node-b": itEnv(t, "HERMIT_IT_VG_B"),
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

// TestIntegrationStoreConditionalRace checks that conditional writes are
// atomic, which fencing relies on: of many concurrent PUTs with the same
// If-Match ETag (or If-None-Match on a new key), exactly one may succeed.
// A backend that checks the precondition and then writes without a lock
// lets several through.
func TestIntegrationStoreConditionalRace(t *testing.T) {
	s := realStore(t)
	ctx := context.Background()
	const writers, rounds = 16, 5

	race := func(t *testing.T, key string, opts store.PutOpts) {
		t.Helper()
		var wg sync.WaitGroup
		start := make(chan struct{})
		errs := make([]error, writers)
		for i := range writers {
			wg.Go(func() {
				<-start
				_, errs[i] = s.Put(ctx, key, []byte(fmt.Sprintf("writer-%d", i)), opts)
			})
		}
		close(start)
		wg.Wait()
		won := 0
		for _, err := range errs {
			switch {
			case err == nil:
				won++
			case !errors.Is(err, store.ErrPrecondition):
				t.Fatalf("unexpected error (want success or ErrPrecondition): %v", err)
			}
		}
		if won != 1 {
			t.Fatalf("%d of %d concurrent conditional PUTs succeeded, want exactly 1", won, writers)
		}
	}

	for r := range rounds {
		key := fmt.Sprintf("race-%d", r)
		race(t, key, store.PutOpts{IfNoneMatch: true})
		_, etag, err := s.Get(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		race(t, key, store.PutOpts{IfMatch: etag})
	}
}
