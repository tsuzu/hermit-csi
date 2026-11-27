package driver

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tsuzu/hermit-csi/pkg/store"
)

var errOpSentinel = errors.New("sentinel op error")

func testOpNode(t *testing.T, opTimeout time.Duration) *Node {
	t.Helper()
	cfg := Config{
		Name: DefaultName, NodeID: "node-a", Store: store.NewMem(),
		ChunkSize: testChunk, Parallel: 4, GCGrace: time.Hour,
		StateDir: t.TempDir(), OpTimeout: opTimeout,
	}
	return newNode(cfg, newFakeVG(t.TempDir()))
}

func TestRunOpResultAndReuse(t *testing.T) {
	n := testOpNode(t, time.Minute)
	var calls atomic.Int32
	fn := func(context.Context) error {
		calls.Add(1)
		return errOpSentinel
	}
	for want := int32(1); want <= 2; want++ {
		if err := n.runOp(context.Background(), "vol-1", opStage, fn); !errors.Is(err, errOpSentinel) {
			t.Fatalf("runOp = %v, want the operation's error", err)
		}
		if got := calls.Load(); got != want {
			t.Fatalf("fn ran %d times, want %d (a finished op must not be joined)", got, want)
		}
	}
	n.opMu.Lock()
	left := len(n.ops)
	n.opMu.Unlock()
	if left != 0 {
		t.Fatalf("%d operations left in the registry", left)
	}
}

func TestRunOpCallerTimeoutJoinsSameOperation(t *testing.T) {
	n := testOpNode(t, time.Minute)
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	fn := func(context.Context) error {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-release
		return nil
	}
	errCh := make(chan error, 1)
	go func() { errCh <- n.runOp(context.Background(), "vol-1", opStage, fn) }()
	<-started

	// A caller whose RPC times out gets Aborted; the operation keeps
	// running detached and a retry joins it instead of starting over.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := n.runOp(ctx, "vol-1", opStage, fn); status.Code(err) != codes.Aborted {
		t.Fatalf("timed-out caller: err = %v, want Aborted", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("fn ran %d times, want 1", got)
	}
	close(release)
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

// TestRunOpExpires is the regression test for the 21h wedged unstage: an
// operation that outlives --op-timeout is failed with DeadlineExceeded and
// counted, and the next retry starts a fresh operation instead of joining
// the stuck one.
func TestRunOpExpires(t *testing.T) {
	n := testOpNode(t, 200*time.Millisecond)
	fn := func(ctx context.Context) error {
		<-ctx.Done()                       // notices the cap...
		time.Sleep(300 * time.Millisecond) // ...but does not return at once
		return nil
	}
	start := time.Now()
	err := n.runOp(context.Background(), "vol-1", opUnstage, fn)
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("runOp took %v, want it bounded by the 200ms op timeout", elapsed)
	}
	if got := mustMetric(t, n.m.reg, "hermit_operations_expired_total", map[string]string{"operation": opUnstage}); got != 1 {
		t.Fatalf("operations_expired_total = %v, want 1", got)
	}

	// The expired operation left the registry even though its fn is still
	// winding down: a retry runs a fresh operation.
	if err := n.runOp(context.Background(), "vol-1", opUnstage, func(context.Context) error { return errOpSentinel }); !errors.Is(err, errOpSentinel) {
		t.Fatalf("retry after expiry = %v, want the fresh operation's error", err)
	}
}

func TestRunOpInFlightAgeMetric(t *testing.T) {
	n := testOpNode(t, time.Minute)
	release := make(chan struct{})
	errCh := make(chan error, 1)
	go func() {
		errCh <- n.runOp(context.Background(), "vol-1", opStage, func(context.Context) error {
			<-release
			return nil
		})
	}()
	labels := map[string]string{"volume": "vol-1", "operation": opStage}
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, ok := metricValue(t, n.m.reg, "hermit_operation_in_flight_age_seconds", labels); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("in-flight age series never appeared")
		}
		time.Sleep(10 * time.Millisecond)
	}
	close(release)
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	if _, ok := metricValue(t, n.m.reg, "hermit_operation_in_flight_age_seconds", labels); ok {
		t.Fatal("in-flight age series outlived the operation")
	}
}
