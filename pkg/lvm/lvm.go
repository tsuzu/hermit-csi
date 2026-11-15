// Package lvm wraps the LVM2 and thin-provisioning-tools CLIs.
package lvm

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"sync"

	"github.com/example/s3lvm-csi/pkg/backup"
)

const (
	// TagVolume marks LVs that hold a volume's live data.
	TagVolume = "s3lvm"
	// TagBase marks the thin snapshot that equals the last uploaded state.
	TagBase = "s3lvm-base"
)

// Runner executes a command; replaceable in tests.
type Runner func(ctx context.Context, name string, args ...string) ([]byte, error)

// DefaultRunner runs the command and returns stdout, folding stderr into errors.
func DefaultRunner(ctx context.Context, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		return out.Bytes(), fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(errb.String()))
	}
	return out.Bytes(), nil
}

// VG is a volume group with a thin pool.
type VG struct {
	Name string
	Pool string
	Run  Runner

	// Only one metadata snapshot can be reserved per pool at a time.
	metaMu sync.Mutex
}

// LV describes a logical volume.
type LV struct {
	Name   string
	Tags   []string
	ThinID int
	Size   int64
}

func (v *VG) run(ctx context.Context, name string, args ...string) ([]byte, error) {
	r := v.Run
	if r == nil {
		r = DefaultRunner
	}
	return r(ctx, name, args...)
}

func (v *VG) path(lv string) string { return v.Name + "/" + lv }

// DevicePath returns the block device path of an (active) LV.
func (v *VG) DevicePath(lv string) string { return "/dev/" + v.Name + "/" + lv }

// dmName escapes a name the way device-mapper does ("-" becomes "--").
func dmName(parts ...string) string {
	esc := make([]string, len(parts))
	for i, p := range parts {
		esc[i] = strings.ReplaceAll(p, "-", "--")
	}
	return strings.Join(esc, "-")
}

func (v *VG) tpoolDM() string { return dmName(v.Name, v.Pool) + "-tpool" }
func (v *VG) tmetaDev() string {
	return "/dev/mapper/" + dmName(v.Name, v.Pool+"_tmeta")
}

// CreateThin creates a thin LV of the given size (bytes, must be a multiple of 512).
func (v *VG) CreateThin(ctx context.Context, name string, size int64) error {
	_, err := v.run(ctx, "lvcreate", "-y", "--thin", "-V", strconv.FormatInt(size, 10)+"b",
		"-n", name, "--addtag", TagVolume, v.path(v.Pool))
	return err
}

// Snapshot creates a thin snapshot of origin and activates it (ignoring the
// activation-skip flag) so it can be read.
func (v *VG) Snapshot(ctx context.Context, origin, name string, tags ...string) error {
	args := []string{"-y", "-s", "-n", name}
	for _, t := range tags {
		args = append(args, "--addtag", t)
	}
	args = append(args, v.path(origin))
	if _, err := v.run(ctx, "lvcreate", args...); err != nil {
		return err
	}
	return v.Activate(ctx, name)
}

// Activate activates an LV, including thin snapshots with activation skip.
func (v *VG) Activate(ctx context.Context, name string) error {
	_, err := v.run(ctx, "lvchange", "-ay", "-K", v.path(name))
	return err
}

// Deactivate deactivates an LV.
func (v *VG) Deactivate(ctx context.Context, name string) error {
	_, err := v.run(ctx, "lvchange", "-an", v.path(name))
	return err
}

// Remove deletes an LV. Missing LVs are not an error.
func (v *VG) Remove(ctx context.Context, name string) error {
	ok, err := v.Exists(ctx, name)
	if err != nil || !ok {
		return err
	}
	_, err = v.run(ctx, "lvremove", "-f", v.path(name))
	return err
}

// AddTag / DelTag change LV tags.
func (v *VG) AddTag(ctx context.Context, name, tag string) error {
	_, err := v.run(ctx, "lvchange", "--addtag", tag, v.path(name))
	return err
}
func (v *VG) DelTag(ctx context.Context, name, tag string) error {
	_, err := v.run(ctx, "lvchange", "--deltag", tag, v.path(name))
	return err
}

// List returns all LVs in the VG (excluding the pool and hidden LVs).
func (v *VG) List(ctx context.Context) ([]LV, error) {
	out, err := v.run(ctx, "lvs", "--noheadings", "--nosuffix", "--units", "b", "--separator", "|",
		"-o", "lv_name,lv_tags,thin_id,lv_size", v.Name)
	if err != nil {
		return nil, err
	}
	return parseLVs(out)
}

func parseLVs(out []byte) ([]LV, error) {
	var lvs []LV
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		f := strings.Split(line, "|")
		if len(f) != 4 {
			return nil, fmt.Errorf("unexpected lvs line %q", line)
		}
		lv := LV{Name: strings.TrimSpace(f[0])}
		if strings.HasPrefix(lv.Name, "[") { // hidden (e.g. [pool_tmeta])
			continue
		}
		if t := strings.TrimSpace(f[1]); t != "" {
			lv.Tags = strings.Split(t, ",")
		}
		if s := strings.TrimSpace(f[2]); s != "" {
			id, err := strconv.Atoi(s)
			if err != nil {
				return nil, fmt.Errorf("thin_id %q: %w", s, err)
			}
			lv.ThinID = id
		}
		if s := strings.TrimSpace(f[3]); s != "" {
			sz, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("lv_size %q: %w", s, err)
			}
			lv.Size = sz
		}
		lvs = append(lvs, lv)
	}
	return lvs, nil
}

// Get returns one LV, or (nil, nil) if it does not exist.
func (v *VG) Get(ctx context.Context, name string) (*LV, error) {
	lvs, err := v.List(ctx)
	if err != nil {
		return nil, err
	}
	for i := range lvs {
		if lvs[i].Name == name {
			return &lvs[i], nil
		}
	}
	return nil, nil
}

// Exists reports whether an LV exists.
func (v *VG) Exists(ctx context.Context, name string) (bool, error) {
	lv, err := v.Get(ctx, name)
	return lv != nil, err
}

// withMetadataSnap reserves a pool metadata snapshot for consistent reads of
// thin metadata while the pool is live.
func (v *VG) withMetadataSnap(ctx context.Context, fn func() error) error {
	v.metaMu.Lock()
	defer v.metaMu.Unlock()
	reserve := func() error {
		_, err := v.run(ctx, "dmsetup", "message", v.tpoolDM(), "0", "reserve_metadata_snap")
		return err
	}
	if err := reserve(); err != nil {
		// A previous process may have died while holding it: release and retry once.
		if _, rerr := v.run(ctx, "dmsetup", "message", v.tpoolDM(), "0", "release_metadata_snap"); rerr != nil {
			return err
		}
		if err := reserve(); err != nil {
			return err
		}
	}
	defer func() {
		// Use a fresh context so release still happens if ctx was cancelled.
		_, _ = v.run(context.Background(), "dmsetup", "message", v.tpoolDM(), "0", "release_metadata_snap")
	}()
	return fn()
}

// ChangedRanges returns the byte ranges that differ between two thin devices
// (by thin_id), using thin_delta on the pool's metadata snapshot.
func (v *VG) ChangedRanges(ctx context.Context, oldID, newID int) ([]backup.Range, error) {
	var out []byte
	err := v.withMetadataSnap(ctx, func() error {
		var err error
		out, err = v.run(ctx, "thin_delta", "-m",
			"--snap1", strconv.Itoa(oldID), "--snap2", strconv.Itoa(newID), v.tmetaDev())
		return err
	})
	if err != nil {
		return nil, err
	}
	return ParseThinDelta(out)
}

// MappedRanges returns the byte ranges that are allocated in a thin device.
// Used for the very first backup, when there is no base snapshot.
func (v *VG) MappedRanges(ctx context.Context, id int) ([]backup.Range, error) {
	var out []byte
	err := v.withMetadataSnap(ctx, func() error {
		var err error
		out, err = v.run(ctx, "thin_dump", "-m", "--dev-id", strconv.Itoa(id), v.tmetaDev())
		return err
	})
	if err != nil {
		return nil, err
	}
	return ParseThinDump(out, id)
}
