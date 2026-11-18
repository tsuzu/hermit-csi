package driver

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/tsuzu/hermit-csi/pkg/backup"
	"github.com/tsuzu/hermit-csi/pkg/lvm"
)

// fakeVG emulates an LVM thin pool with sparse files. Snapshots are full
// copies; ChangedRanges compares content at the pool block size, which is
// what thin_delta reports for real thin devices.
type fakeVG struct {
	mu     sync.Mutex
	dir    string
	block  int64
	nextID int
	lvs    map[string]*lvm.LV
	calls  map[string]int
}

func newFakeVG(dir string) *fakeVG {
	return &fakeVG{dir: dir, block: 64 << 10, nextID: 1, lvs: map[string]*lvm.LV{}, calls: map[string]int{}}
}

func (f *fakeVG) count(op string) { f.calls[op]++ }

func (f *fakeVG) DevicePath(name string) string { return filepath.Join(f.dir, name) }

func (f *fakeVG) CreateThin(_ context.Context, name string, size int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.count("create")
	if _, ok := f.lvs[name]; ok {
		return fmt.Errorf("lv %s exists", name)
	}
	fh, err := os.Create(f.DevicePath(name))
	if err != nil {
		return err
	}
	defer fh.Close()
	if err := fh.Truncate(size); err != nil {
		return err
	}
	f.lvs[name] = &lvm.LV{Name: name, Size: size, ThinID: f.nextID, Tags: []string{lvm.TagVolume}}
	f.nextID++
	return nil
}

func (f *fakeVG) Snapshot(_ context.Context, origin, name string, tags ...string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.count("snapshot")
	o, ok := f.lvs[origin]
	if !ok {
		return fmt.Errorf("no origin %s", origin)
	}
	if _, ok := f.lvs[name]; ok {
		return fmt.Errorf("lv %s exists", name)
	}
	src, err := os.Open(f.DevicePath(origin))
	if err != nil {
		return err
	}
	defer src.Close()
	dst, err := os.Create(f.DevicePath(name))
	if err != nil {
		return err
	}
	defer dst.Close()
	if _, err := io.Copy(dst, src); err != nil {
		return err
	}
	f.lvs[name] = &lvm.LV{Name: name, Size: o.Size, ThinID: f.nextID, Tags: append([]string{}, tags...)}
	f.nextID++
	return nil
}

func (f *fakeVG) Activate(context.Context, string) error   { return nil }
func (f *fakeVG) Deactivate(context.Context, string) error { return nil }

func (f *fakeVG) Remove(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.lvs[name]; !ok {
		return nil
	}
	f.count("remove")
	delete(f.lvs, name)
	return os.Remove(f.DevicePath(name))
}

func (f *fakeVG) AddTag(_ context.Context, name, tag string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if lv, ok := f.lvs[name]; ok {
		lv.Tags = append(lv.Tags, tag)
	}
	return nil
}

func (f *fakeVG) Get(_ context.Context, name string) (*lvm.LV, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.count("get")
	if lv, ok := f.lvs[name]; ok {
		c := *lv
		return &c, nil
	}
	return nil, nil
}

func (f *fakeVG) List(context.Context) ([]lvm.LV, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.count("list")
	var out []lvm.LV
	for _, lv := range f.lvs {
		out = append(out, *lv)
	}
	return out, nil
}

func (f *fakeVG) byID(id int) (*lvm.LV, error) {
	for _, lv := range f.lvs {
		if lv.ThinID == id {
			return lv, nil
		}
	}
	return nil, fmt.Errorf("no thin id %d", id)
}

func (f *fakeVG) read(id int) ([]byte, error) {
	lv, err := f.byID(id)
	if err != nil {
		return nil, err
	}
	return os.ReadFile(f.DevicePath(lv.Name))
}

func (f *fakeVG) ChangedRanges(_ context.Context, oldID, newID int) ([]backup.Range, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.count("delta")
	a, err := f.read(oldID)
	if err != nil {
		return nil, err
	}
	b, err := f.read(newID)
	if err != nil {
		return nil, err
	}
	var out []backup.Range
	for off := int64(0); off < int64(len(b)); off += f.block {
		end := min(off+f.block, int64(len(b)))
		if !bytes.Equal(a[off:end], b[off:end]) {
			out = append(out, backup.Range{Off: off, Len: end - off})
		}
	}
	return out, nil
}

func (f *fakeVG) MappedRanges(_ context.Context, id int) ([]backup.Range, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.count("dump")
	b, err := f.read(id)
	if err != nil {
		return nil, err
	}
	var out []backup.Range
	zero := make([]byte, f.block)
	for off := int64(0); off < int64(len(b)); off += f.block {
		end := min(off+f.block, int64(len(b)))
		if !bytes.Equal(b[off:end], zero[:end-off]) {
			out = append(out, backup.Range{Off: off, Len: end - off})
		}
	}
	return out, nil
}

// fakePoolSize is the data size the fake thin pool reports.
const fakePoolSize = 1 << 30

// PoolUsage reports the blocks the sparse files really use, like the data
// usage of a thin pool (snapshots are full copies here, so it overcounts).
func (f *fakeVG) PoolUsage(context.Context) (lvm.PoolUsage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var used int64
	for name := range f.lvs {
		fi, err := os.Stat(f.DevicePath(name))
		if err != nil {
			return lvm.PoolUsage{}, err
		}
		used += fi.Sys().(*syscall.Stat_t).Blocks * 512
	}
	return lvm.PoolUsage{VG: "fake", Pool: "pool", Size: fakePoolSize,
		DataRatio: float64(used) / fakePoolSize, MetadataRatio: float64(len(f.lvs)) / 1000}, nil
}
