package lvm

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/tsuzu/hermit-csi/pkg/backup"
)

// TestRealThinTools runs the real thin_delta / thin_dump binaries against
// metadata built with thin_restore, to make sure flags and output parsing
// match the installed thin-provisioning-tools. Skipped if they are missing.
func TestRealThinTools(t *testing.T) {
	for _, b := range []string{"thin_restore", "thin_delta", "thin_dump"} {
		if _, err := exec.LookPath(b); err != nil {
			t.Skipf("%s not installed", b)
		}
	}
	dir := t.TempDir()
	xmlPath, meta := filepath.Join(dir, "md.xml"), filepath.Join(dir, "meta.bin")
	// dev 2 is a snapshot of dev 1: block 1 rewritten, block 9 newly written, block 2 discarded.
	md := `<superblock uuid="" time="1" transaction="2" data_block_size="128" nr_data_blocks="100">
  <device dev_id="1" mapped_blocks="3" transaction="0" creation_time="0" snap_time="1">
    <range_mapping origin_begin="0" data_begin="0" length="3" time="0"/>
  </device>
  <device dev_id="2" mapped_blocks="3" transaction="1" creation_time="1" snap_time="1">
    <single_mapping origin_block="0" data_block="0" time="0"/>
    <single_mapping origin_block="1" data_block="10" time="1"/>
    <single_mapping origin_block="9" data_block="11" time="1"/>
  </device>
</superblock>`
	if err := os.WriteFile(xmlPath, []byte(md), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(meta, make([]byte, 8<<20), 0o600); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("thin_restore", "-i", xmlPath, "-o", meta).CombinedOutput(); err != nil {
		t.Fatalf("thin_restore: %v: %s", err, out)
	}

	// Run the real tools, but without a live pool: skip dmsetup, drop -m
	// (a metadata file has no metadata snapshot) and point at the file.
	v := &VG{Name: "vg", Pool: "pool", Run: func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if name == "dmsetup" {
			return nil, nil
		}
		var a []string
		for _, x := range args {
			switch x {
			case "-m":
			case "/dev/mapper/vg-pool_tmeta":
				a = append(a, meta)
			default:
				a = append(a, x)
			}
		}
		return DefaultRunner(ctx, name, a...)
	}}
	ctx := context.Background()
	got, err := v.ChangedRanges(ctx, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	want := []backup.Range{{Off: 1 * blk, Len: blk}, {Off: 2 * blk, Len: blk}, {Off: 9 * blk, Len: blk}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ChangedRanges = %+v, want %+v", got, want)
	}
	mapped, err := v.MappedRanges(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	// thin_dump may coalesce adjacent mappings into a range; compare chunk sets.
	gotChunks := backup.ChunksForRanges(mapped, blk, 100)
	if !reflect.DeepEqual(gotChunks, []int64{0, 1, 9}) {
		t.Fatalf("MappedRanges chunks = %v", gotChunks)
	}
}
