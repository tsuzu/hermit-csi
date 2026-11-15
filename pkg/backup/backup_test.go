package backup

import (
	"bytes"
	"context"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tsuzu/hermit-csi/pkg/manifest"
	"github.com/tsuzu/hermit-csi/pkg/store"
)

const cs = 64 << 10 // 64 KiB chunks for tests

// memDev is an in-memory block device.
type memDev struct{ b []byte }

func (d *memDev) ReadAt(p []byte, off int64) (int, error)  { return copy(p, d.b[off:]), nil }
func (d *memDev) WriteAt(p []byte, off int64) (int, error) { return copy(d.b[off:], p), nil }

func fill(d *memDev, off, n int64, seed int64) {
	r := rand.New(rand.NewSource(seed))
	r.Read(d.b[off : off+n])
}

func countChunks(t *testing.T, s *store.Mem) int {
	t.Helper()
	n := 0
	_ = s.List(context.Background(), manifest.ChunkPrefix, func(store.ObjectInfo) error { n++; return nil })
	return n
}

func TestChunksForRanges(t *testing.T) {
	got := ChunksForRanges([]Range{{0, 1}, {cs - 1, 2}, {5 * cs, 3 * cs}, {100 * cs, 1}, {9 * cs, 0}}, cs, 10)
	want := []int64{0, 1, 5, 6, 7}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestBackupIncrementalAndRestore(t *testing.T) {
	ctx := context.Background()
	s := store.NewMem()
	e := &Engine{S: s, Parallel: 4, GCGrace: time.Hour}
	size := int64(16*cs + 1000) // last chunk is short
	dev := &memDev{b: make([]byte, size)}
	base := manifest.New("v", size, cs)

	// First backup: two regions written, scan everything.
	fill(dev, 0, 2*cs, 1)
	fill(dev, 16*cs, 1000, 2)
	all := ChunksForRanges([]Range{{0, size}}, cs, base.NumChunks())
	m1, st, err := e.Backup(ctx, dev, base, all)
	if err != nil {
		t.Fatal(err)
	}
	if st.ChunksUploaded != 3 || countChunks(t, s) != 3 {
		t.Fatalf("first backup uploaded %d chunks (store has %d), want 3", st.ChunksUploaded, countChunks(t, s))
	}
	if m1.UsedBytes() != 2*cs+1000 {
		t.Fatalf("used = %d", m1.UsedBytes())
	}

	// Second backup: one small write; only that chunk is read and uploaded.
	fill(dev, 5*cs+10, 20, 3)
	m2, st, err := e.Backup(ctx, dev, m1, ChunksForRanges([]Range{{5*cs + 10, 20}}, cs, m1.NumChunks()))
	if err != nil {
		t.Fatal(err)
	}
	if st.ChunksScanned != 1 || st.ChunksUploaded != 1 || st.BytesUploaded != cs {
		t.Fatalf("incremental stats = %+v", st)
	}

	// Duplicate content in another chunk is not uploaded twice.
	copy(dev.b[8*cs:9*cs], dev.b[0:cs])
	m3, st, err := e.Backup(ctx, dev, m2, []int64{8})
	if err != nil {
		t.Fatal(err)
	}
	if st.ChunksUploaded != 0 || m3.Chunks[8] != m3.Chunks[0] {
		t.Fatalf("dedup failed: %+v", st)
	}

	// Discarding a chunk makes it zero again.
	for i := int64(0); i < cs; i++ {
		dev.b[cs+i] = 0
	}
	m4, _, err := e.Backup(ctx, dev, m3, []int64{1})
	if err != nil {
		t.Fatal(err)
	}
	if m4.Chunks[1] != "" {
		t.Fatal("zeroed chunk should have empty hash")
	}

	// Restore to a fresh device reproduces the content exactly.
	out := &memDev{b: make([]byte, size)}
	if err := e.Restore(ctx, out, m4, nil); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.b, dev.b) {
		t.Fatal("restored content differs")
	}
}

func TestRestoreDetectsCorruption(t *testing.T) {
	ctx := context.Background()
	s := store.NewMem()
	e := &Engine{S: s}
	dev := &memDev{b: make([]byte, 2*cs)}
	fill(dev, 0, cs, 1)
	m, _, err := e.Backup(ctx, dev, manifest.New("v", 2*cs, cs), []int64{0, 1})
	if err != nil {
		t.Fatal(err)
	}
	bad := make([]byte, cs)
	if _, err := s.Put(ctx, manifest.ChunkKey(m.Chunks[0]), bad, store.PutOpts{}); err != nil {
		t.Fatal(err)
	}
	err = e.Restore(ctx, &memDev{b: make([]byte, 2*cs)}, m, nil)
	if err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("want checksum error, got %v", err)
	}
}

func TestEnsureChunkRefreshesOldCopies(t *testing.T) {
	ctx := context.Background()
	s := store.NewMem()
	now := time.Now()
	e := &Engine{S: s, GCGrace: 10 * time.Hour, Now: func() time.Time { return now }}
	data := []byte("hello")
	if up, _ := e.ensureChunk(ctx, "h", data); !up {
		t.Fatal("first upload expected")
	}
	if up, _ := e.ensureChunk(ctx, "h", data); up {
		t.Fatal("fresh copy should be reused")
	}
	s.SetModTime(manifest.ChunkKey("h"), now.Add(-6*time.Hour)) // older than grace/2
	if up, _ := e.ensureChunk(ctx, "h", data); !up {
		t.Fatal("old copy should be re-uploaded to protect it from GC")
	}
}

func TestGC(t *testing.T) {
	ctx := context.Background()
	s := store.NewMem()
	e := &Engine{S: s}
	now := time.Now()

	dev := &memDev{b: make([]byte, 4*cs)}
	fill(dev, 0, 4*cs, 1)
	m, _, err := e.Backup(ctx, dev, manifest.New("v", 4*cs, cs), []int64{0, 1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	// Snapshot pins chunk 0..3. Then latest changes chunk 3.
	snap := m.Clone()
	snap.SnapshotID = "s1"
	if _, err := manifest.Save(ctx, s, manifest.SnapshotKey("s1"), snap, store.PutOpts{}); err != nil {
		t.Fatal(err)
	}
	fill(dev, 3*cs, cs, 9)
	m2, _, err := e.Backup(ctx, dev, m, []int64{3})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manifest.Save(ctx, s, manifest.LatestKey("v"), m2, store.PutOpts{}); err != nil {
		t.Fatal(err)
	}
	// An orphan chunk: old one is deleted, a young one is kept.
	_, _ = s.Put(ctx, manifest.ChunkKey("orphan-old"), []byte("x"), store.PutOpts{})
	s.SetModTime(manifest.ChunkKey("orphan-old"), now.Add(-48*time.Hour))
	_, _ = s.Put(ctx, manifest.ChunkKey("orphan-young"), []byte("y"), store.PutOpts{})

	n, err := GC(ctx, s, 24*time.Hour, now)
	if err != nil || n != 1 {
		t.Fatalf("GC deleted %d (%v), want 1", n, err)
	}
	if _, err := s.Head(ctx, manifest.ChunkKey("orphan-young")); err != nil {
		t.Fatal("young orphan must survive grace period")
	}

	// Deleting the snapshot releases the old chunk 3.
	_ = s.Delete(ctx, manifest.SnapshotKey("s1"))
	for _, k := range []string{m.Chunks[3]} {
		s.SetModTime(manifest.ChunkKey(k), now.Add(-48*time.Hour))
	}
	n, err = GC(ctx, s, 24*time.Hour, now)
	if err != nil || n != 1 {
		t.Fatalf("GC after snapshot delete deleted %d (%v), want 1", n, err)
	}
	if _, err := s.Head(ctx, manifest.ChunkKey(m2.Chunks[3])); err != nil {
		t.Fatal("chunk referenced by latest was deleted")
	}
}
