// Package manifest defines the S3 object layout:
//
//	chunks/<sha256>              content-addressed chunk data (shared by all volumes)
//	volumes/<id>/latest.json.gz  the newest manifest of a volume (overwritten each backup)
//	volumes/<id>/owner.json      which node currently owns (is attached to) the volume
//	snapshots/<id>.json.gz       immutable manifest captured at VolumeSnapshot time
package manifest

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/tsuzu/hermit-csi/pkg/store"
)

// Manifest maps every chunk index of a volume to the hash of its content.
// An empty string means the chunk is all zeroes and is not stored.
type Manifest struct {
	VolumeID   string    `json:"volumeId"`
	SizeBytes  int64     `json:"sizeBytes"`
	ChunkSize  int64     `json:"chunkSize"`
	Generation int64     `json:"generation"` // +1 on every successful backup
	Epoch      int64     `json:"epoch"`      // ownership epoch, bumped by the controller to fence old writers
	CreatedAt  time.Time `json:"createdAt"`
	// Set only on snapshot manifests.
	SnapshotID     string `json:"snapshotId,omitempty"`
	SourceVolumeID string `json:"sourceVolumeId,omitempty"`

	Chunks []string `json:"chunks"`
}

// Owner records the node that a volume is attached to.
type Owner struct {
	NodeID string `json:"nodeId,omitempty"` // empty = detached
	Epoch  int64  `json:"epoch"`
	// Address of the node agent (host:port) used for in-use snapshots.
	Address string `json:"address,omitempty"`
}

func LatestKey(vol string) string    { return "volumes/" + vol + "/latest.json.gz" }
func OwnerKey(vol string) string     { return "volumes/" + vol + "/owner.json" }
func SnapshotKey(snap string) string { return "snapshots/" + snap + ".json.gz" }
func ChunkKey(hash string) string    { return "chunks/" + hash }

const (
	ChunkPrefix    = "chunks/"
	VolumePrefix   = "volumes/"
	SnapshotPrefix = "snapshots/"
)

// IsManifestKey reports whether a listed key is a manifest (latest or snapshot).
func IsManifestKey(k string) bool {
	return (strings.HasPrefix(k, VolumePrefix) && strings.HasSuffix(k, "/latest.json.gz")) ||
		(strings.HasPrefix(k, SnapshotPrefix) && strings.HasSuffix(k, ".json.gz"))
}

// New returns an all-zero manifest for a volume of the given size.
func New(vol string, size, chunkSize int64) *Manifest {
	n := (size + chunkSize - 1) / chunkSize
	return &Manifest{
		VolumeID:  vol,
		SizeBytes: size,
		ChunkSize: chunkSize,
		CreatedAt: time.Now().UTC(),
		Chunks:    make([]string, n),
	}
}

// NumChunks returns the number of chunks.
func (m *Manifest) NumChunks() int64 { return int64(len(m.Chunks)) }

// ChunkLen returns the length in bytes of chunk i (the last one may be short).
func (m *Manifest) ChunkLen(i int64) int64 {
	off := i * m.ChunkSize
	if off+m.ChunkSize > m.SizeBytes {
		return m.SizeBytes - off
	}
	return m.ChunkSize
}

// Clone returns a deep copy.
func (m *Manifest) Clone() *Manifest {
	c := *m
	c.Chunks = append([]string(nil), m.Chunks...)
	return &c
}

// Resize grows the manifest; new chunks are zero.
func (m *Manifest) Resize(size int64) error {
	if size < m.SizeBytes {
		return fmt.Errorf("cannot shrink volume from %d to %d", m.SizeBytes, size)
	}
	n := (size + m.ChunkSize - 1) / m.ChunkSize
	for int64(len(m.Chunks)) < n {
		m.Chunks = append(m.Chunks, "")
	}
	m.SizeBytes = size
	return nil
}

// UsedBytes is the logical size of non-zero chunks.
func (m *Manifest) UsedBytes() int64 {
	var u int64
	for i, h := range m.Chunks {
		if h != "" {
			u += m.ChunkLen(int64(i))
		}
	}
	return u
}

// Encode serializes as gzipped JSON.
func (m *Manifest) Encode() ([]byte, error) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if err := json.NewEncoder(zw).Encode(m); err != nil {
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode parses gzipped JSON.
func Decode(b []byte) (*Manifest, error) {
	zr, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	raw, err := io.ReadAll(zr)
	if err != nil {
		return nil, err
	}
	var m Manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	if m.ChunkSize <= 0 {
		return nil, fmt.Errorf("invalid manifest: chunkSize=%d", m.ChunkSize)
	}
	if want := (m.SizeBytes + m.ChunkSize - 1) / m.ChunkSize; int64(len(m.Chunks)) != want {
		return nil, fmt.Errorf("invalid manifest: %d chunks, want %d", len(m.Chunks), want)
	}
	return &m, nil
}

// Load reads a manifest and its ETag.
func Load(ctx context.Context, s store.ObjectStore, key string) (*Manifest, string, error) {
	b, etag, err := s.Get(ctx, key)
	if err != nil {
		return nil, "", err
	}
	m, err := Decode(b)
	if err != nil {
		return nil, "", fmt.Errorf("decode %s: %w", key, err)
	}
	return m, etag, nil
}

// Save writes a manifest conditionally and returns the new ETag.
func Save(ctx context.Context, s store.ObjectStore, key string, m *Manifest, o store.PutOpts) (string, error) {
	b, err := m.Encode()
	if err != nil {
		return "", err
	}
	return s.Put(ctx, key, b, o)
}

// LoadOwner reads the owner record.
func LoadOwner(ctx context.Context, s store.ObjectStore, vol string) (*Owner, string, error) {
	b, etag, err := s.Get(ctx, OwnerKey(vol))
	if err != nil {
		return nil, "", err
	}
	var o Owner
	if err := json.Unmarshal(b, &o); err != nil {
		return nil, "", err
	}
	return &o, etag, nil
}

// SaveOwner writes the owner record conditionally.
func SaveOwner(ctx context.Context, s store.ObjectStore, vol string, o *Owner, opt store.PutOpts) (string, error) {
	b, err := json.Marshal(o)
	if err != nil {
		return "", err
	}
	return s.Put(ctx, OwnerKey(vol), b, opt)
}
