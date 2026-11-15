package store

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// ObjectStore is the subset of S3 the driver relies on. *Store implements it;
// Mem is an in-memory implementation for tests.
type ObjectStore interface {
	Get(ctx context.Context, key string) ([]byte, string, error)
	Put(ctx context.Context, key string, data []byte, o PutOpts) (string, error)
	Head(ctx context.Context, key string) (ObjectInfo, error)
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix string, fn func(ObjectInfo) error) error
}

var _ ObjectStore = (*Store)(nil)
var _ ObjectStore = (*Mem)(nil)

type memObj struct {
	data []byte
	etag string
	mod  time.Time
}

// Mem is an in-memory ObjectStore with S3-like conditional write semantics.
type Mem struct {
	mu   sync.Mutex
	objs map[string]memObj
	Now  func() time.Time
}

// NewMem returns an empty in-memory store.
func NewMem() *Mem { return &Mem{objs: map[string]memObj{}, Now: time.Now} }

func (m *Mem) Get(_ context.Context, k string) ([]byte, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.objs[k]
	if !ok {
		return nil, "", fmt.Errorf("%w: %s", ErrNotFound, k)
	}
	return append([]byte(nil), o.data...), o.etag, nil
}

func (m *Mem) Put(_ context.Context, k string, data []byte, opt PutOpts) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, exists := m.objs[k]
	if opt.IfNoneMatch && exists {
		return "", fmt.Errorf("%w: %s exists", ErrPrecondition, k)
	}
	if opt.IfMatch != "" && (!exists || cur.etag != opt.IfMatch) {
		return "", fmt.Errorf("%w: %s etag mismatch", ErrPrecondition, k)
	}
	sum := md5.Sum(append([]byte(m.Now().String()), data...))
	et := `"` + hex.EncodeToString(sum[:]) + `"`
	m.objs[k] = memObj{data: append([]byte(nil), data...), etag: et, mod: m.Now()}
	return et, nil
}

func (m *Mem) Head(_ context.Context, k string) (ObjectInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.objs[k]
	if !ok {
		return ObjectInfo{}, fmt.Errorf("%w: %s", ErrNotFound, k)
	}
	return ObjectInfo{Key: k, LastModified: o.mod, Size: int64(len(o.data))}, nil
}

func (m *Mem) Delete(_ context.Context, k string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objs, k)
	return nil
}

func (m *Mem) List(_ context.Context, prefix string, fn func(ObjectInfo) error) error {
	m.mu.Lock()
	var out []ObjectInfo
	for k, o := range m.objs {
		if strings.HasPrefix(k, prefix) {
			out = append(out, ObjectInfo{Key: k, LastModified: o.mod, Size: int64(len(o.data))})
		}
	}
	m.mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	for _, o := range out {
		if err := fn(o); err != nil {
			return err
		}
	}
	return nil
}

// SetModTime is a test helper to age an object.
func (m *Mem) SetModTime(k string, t time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if o, ok := m.objs[k]; ok {
		o.mod = t
		m.objs[k] = o
	}
}
