// Package backup uploads changed chunks of a block device to S3 and restores
// a device from a manifest. It knows nothing about LVM: callers pass the list
// of changed byte ranges (from thin_delta) and a device to read/write.
package backup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/tsuzu/s3lvm-csi/pkg/manifest"
	"github.com/tsuzu/s3lvm-csi/pkg/store"
)

// Range is a byte range [Off, Off+Len).
type Range struct{ Off, Len int64 }

// Engine performs backups and restores.
type Engine struct {
	S        store.ObjectStore
	Parallel int           // concurrent chunk transfers
	GCGrace  time.Duration // must match the controller's GC grace period
	Now      func() time.Time
}

// Stats describes the work a backup did.
type Stats struct {
	ChunksScanned  int64
	ChunksChanged  int64
	ChunksUploaded int64
	BytesUploaded  int64
}

func (e *Engine) parallel() int {
	if e.Parallel <= 0 {
		return 8
	}
	return e.Parallel
}

func (e *Engine) now() time.Time {
	if e.Now != nil {
		return e.Now()
	}
	return time.Now()
}

// ChunksForRanges converts byte ranges to the sorted set of chunk indices they touch.
func ChunksForRanges(ranges []Range, chunkSize, numChunks int64) []int64 {
	set := map[int64]struct{}{}
	for _, r := range ranges {
		if r.Len <= 0 {
			continue
		}
		first := r.Off / chunkSize
		last := (r.Off + r.Len - 1) / chunkSize
		for i := first; i <= last && i < numChunks; i++ {
			set[i] = struct{}{}
		}
	}
	out := make([]int64, 0, len(set))
	for i := range set {
		out = append(out, i)
	}
	sort.Slice(out, func(a, b int) bool { return out[a] < out[b] })
	return out
}

func isZero(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}

// Backup produces a new manifest from base by re-reading only the chunks in
// `changed` from dev. Chunks not listed keep their hash from base. Only chunk
// hashes that are not already referenced by base are uploaded (and even those
// are skipped if a fresh copy already exists in S3).
//
// The returned manifest has the same Generation/Epoch as base; the caller bumps them.
func (e *Engine) Backup(ctx context.Context, dev io.ReaderAt, base *manifest.Manifest, changed []int64) (*manifest.Manifest, Stats, error) {
	next := base.Clone()
	next.CreatedAt = e.now().UTC()
	next.SnapshotID, next.SourceVolumeID = "", ""

	known := make(map[string]struct{}, len(base.Chunks))
	for _, h := range base.Chunks {
		if h != "" {
			known[h] = struct{}{}
		}
	}

	var st Stats
	var mu sync.Mutex // guards next.Chunks writes & uploading map
	uploading := map[string]chan struct{}{}

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(e.parallel())
	for _, idx := range changed {
		idx := idx
		g.Go(func() error {
			n := base.ChunkLen(idx)
			buf := make([]byte, n)
			if _, err := dev.ReadAt(buf, idx*base.ChunkSize); err != nil && !errors.Is(err, io.EOF) {
				return fmt.Errorf("read chunk %d: %w", idx, err)
			}
			atomic.AddInt64(&st.ChunksScanned, 1)
			h := ""
			if !isZero(buf) {
				sum := sha256.Sum256(buf)
				h = hex.EncodeToString(sum[:])
			}
			mu.Lock()
			if next.Chunks[idx] != h {
				st.ChunksChanged++
			}
			next.Chunks[idx] = h
			if h == "" {
				mu.Unlock()
				return nil
			}
			if _, ok := known[h]; ok { // referenced by base => GC will not delete it
				mu.Unlock()
				return nil
			}
			if ch, ok := uploading[h]; ok { // same content in another chunk of this run
				mu.Unlock()
				select {
				case <-ch:
					return nil
				case <-gctx.Done():
					return gctx.Err()
				}
			}
			done := make(chan struct{})
			uploading[h] = done
			mu.Unlock()
			defer close(done)

			up, err := e.ensureChunk(gctx, h, buf)
			if err != nil {
				return err
			}
			if up {
				atomic.AddInt64(&st.ChunksUploaded, 1)
				atomic.AddInt64(&st.BytesUploaded, n)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, st, err
	}
	return next, st, nil
}

// ensureChunk uploads a chunk unless a recent copy already exists. A copy
// older than half the GC grace period is re-uploaded so the GC cannot delete
// it between our check and the manifest write that references it.
func (e *Engine) ensureChunk(ctx context.Context, h string, data []byte) (bool, error) {
	key := manifest.ChunkKey(h)
	info, err := e.S.Head(ctx, key)
	switch {
	case err == nil:
		if e.GCGrace <= 0 || e.now().Sub(info.LastModified) < e.GCGrace/2 {
			return false, nil
		}
	case errors.Is(err, store.ErrNotFound):
	default:
		return false, fmt.Errorf("head %s: %w", key, err)
	}
	if _, err := e.S.Put(ctx, key, data, store.PutOpts{}); err != nil {
		return false, fmt.Errorf("put %s: %w", key, err)
	}
	return true, nil
}

// Restore writes every non-zero chunk of m to dev. dev must be zero-filled
// (a freshly created thin LV is), so zero chunks are skipped.
func (e *Engine) Restore(ctx context.Context, dev io.WriterAt, m *manifest.Manifest, progress func(done, total int64)) error {
	var total int64
	for _, h := range m.Chunks {
		if h != "" {
			total++
		}
	}
	var done int64
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(e.parallel())
	for i, h := range m.Chunks {
		if h == "" {
			continue
		}
		i, h := int64(i), h
		g.Go(func() error {
			b, _, err := e.S.Get(gctx, manifest.ChunkKey(h))
			if err != nil {
				return fmt.Errorf("get chunk %d (%s): %w", i, h, err)
			}
			if int64(len(b)) != m.ChunkLen(i) {
				return fmt.Errorf("chunk %d (%s): size %d, want %d", i, h, len(b), m.ChunkLen(i))
			}
			sum := sha256.Sum256(b)
			if hex.EncodeToString(sum[:]) != h {
				return fmt.Errorf("chunk %d: checksum mismatch", i)
			}
			if _, err := dev.WriteAt(b, i*m.ChunkSize); err != nil {
				return fmt.Errorf("write chunk %d: %w", i, err)
			}
			d := atomic.AddInt64(&done, 1)
			if progress != nil {
				progress(d, total)
			}
			return nil
		})
	}
	return g.Wait()
}

// GC deletes chunks that no manifest references and that are older than grace.
func GC(ctx context.Context, s store.ObjectStore, grace time.Duration, now time.Time) (deleted int, err error) {
	referenced := map[string]struct{}{}
	collect := func(prefix string) error {
		return s.List(ctx, prefix, func(o store.ObjectInfo) error {
			if !manifest.IsManifestKey(o.Key) {
				return nil
			}
			m, _, err := manifest.Load(ctx, s, o.Key)
			if err != nil {
				if errors.Is(err, store.ErrNotFound) {
					return nil
				}
				return err
			}
			for _, h := range m.Chunks {
				if h != "" {
					referenced[h] = struct{}{}
				}
			}
			return nil
		})
	}
	if err := collect(manifest.VolumePrefix); err != nil {
		return 0, fmt.Errorf("list volumes: %w", err)
	}
	if err := collect(manifest.SnapshotPrefix); err != nil {
		return 0, fmt.Errorf("list snapshots: %w", err)
	}
	var victims []string
	err = s.List(ctx, manifest.ChunkPrefix, func(o store.ObjectInfo) error {
		h := o.Key[len(manifest.ChunkPrefix):]
		if _, ok := referenced[h]; ok {
			return nil
		}
		if now.Sub(o.LastModified) < grace {
			return nil
		}
		victims = append(victims, o.Key)
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("list chunks: %w", err)
	}
	for _, k := range victims {
		if err := s.Delete(ctx, k); err != nil {
			return deleted, err
		}
		deleted++
	}
	return deleted, nil
}
