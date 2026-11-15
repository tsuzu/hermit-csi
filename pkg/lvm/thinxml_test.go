package lvm

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/tsuzu/s3lvm-csi/pkg/backup"
)

// data_block_size is in 512-byte sectors: 128 sectors = 64 KiB.
const blk = 128 * 512

func TestParseThinDelta(t *testing.T) {
	out := `<superblock uuid="" time="3" transaction="5" data_block_size="128" nr_data_blocks="1600">
  <diff left="1" right="2">
    <same begin="0" length="10"/>
    <different begin="10" length="2"/>
    <same begin="12" length="4"/>
    <right_only begin="16" length="1"/>
    <left_only begin="40" length="3"/>
  </diff>
</superblock>`
	got, err := ParseThinDelta([]byte(out))
	if err != nil {
		t.Fatal(err)
	}
	want := []backup.Range{
		{Off: 10 * blk, Len: 2 * blk},
		{Off: 16 * blk, Len: 1 * blk},
		{Off: 40 * blk, Len: 3 * blk},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestParseThinDeltaNoChanges(t *testing.T) {
	out := `<superblock uuid="" time="1" transaction="1" data_block_size="128" nr_data_blocks="16"><diff left="1" right="2"><same begin="0" length="16"/></diff></superblock>`
	got, err := ParseThinDelta([]byte(out))
	if err != nil || len(got) != 0 {
		t.Fatalf("got %v, %v", got, err)
	}
}

func TestParseThinDeltaErrors(t *testing.T) {
	for _, in := range []string{
		``,
		`<diff><different begin="1" length="1"/></diff>`,
		`<superblock data_block_size="0"></superblock>`,
		`<superblock data_block_size="128"><different begin="x" length="1"/></superblock>`,
	} {
		if _, err := ParseThinDelta([]byte(in)); err == nil {
			t.Errorf("expected error for %q", in)
		}
	}
}

func TestParseThinDump(t *testing.T) {
	out := `<superblock uuid="" time="1" transaction="2" flags="0" version="2" data_block_size="128" nr_data_blocks="0">
  <device dev_id="1" mapped_blocks="3" transaction="0" creation_time="0" snap_time="1">
    <range_mapping origin_begin="0" data_begin="0" length="2" time="0"/>
  </device>
  <device dev_id="2" mapped_blocks="4" transaction="1" creation_time="1" snap_time="1">
    <range_mapping origin_begin="0" data_begin="0" length="2" time="0"/>
    <single_mapping origin_block="9" data_block="7" time="1"/>
    <range_mapping origin_begin="20" data_begin="8" length="1" time="1"/>
  </device>
</superblock>`
	got, err := ParseThinDump([]byte(out), 2)
	if err != nil {
		t.Fatal(err)
	}
	want := []backup.Range{{Off: 0, Len: 2 * blk}, {Off: 9 * blk, Len: blk}, {Off: 20 * blk, Len: blk}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	if _, err := ParseThinDump([]byte(out), 3); err == nil {
		t.Fatal("expected error for missing device")
	}
}

func TestParseLVs(t *testing.T) {
	out := "  pool|||1073741824\n  pvc-1|s3lvm|1|16777216\n  pvc-1-b3|s3lvm,s3lvm-base|2|16777216\n  [pool_tmeta]|||4194304\n"
	got, err := parseLVs([]byte(out))
	if err != nil {
		t.Fatal(err)
	}
	want := []LV{
		{Name: "pool", Size: 1073741824},
		{Name: "pvc-1", Tags: []string{"s3lvm"}, ThinID: 1, Size: 16777216},
		{Name: "pvc-1-b3", Tags: []string{"s3lvm", "s3lvm-base"}, ThinID: 2, Size: 16777216},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestDMName(t *testing.T) {
	v := &VG{Name: "my-vg", Pool: "thin-pool"}
	if got := v.tpoolDM(); got != "my--vg-thin--pool-tpool" {
		t.Fatalf("tpool = %s", got)
	}
	if got := v.tmetaDev(); got != "/dev/mapper/my--vg-thin--pool_tmeta" {
		t.Fatalf("tmeta = %s", got)
	}
}

func TestChangedRangesReleasesMetadataSnap(t *testing.T) {
	var calls []string
	v := &VG{Name: "vg", Pool: "pool", Run: func(_ context.Context, name string, args ...string) ([]byte, error) {
		calls = append(calls, name+" "+strings.Join(args, " "))
		if name == "thin_delta" {
			return []byte(`<superblock data_block_size="128"><diff left="1" right="2"><different begin="1" length="1"/></diff></superblock>`), nil
		}
		return nil, nil
	}}
	r, err := v.ChangedRanges(context.Background(), 1, 2)
	if err != nil || len(r) != 1 {
		t.Fatalf("got %v %v", r, err)
	}
	want := []string{
		"dmsetup message vg-pool-tpool 0 reserve_metadata_snap",
		"thin_delta -m --snap1 1 --snap2 2 /dev/mapper/vg-pool_tmeta",
		"dmsetup message vg-pool-tpool 0 release_metadata_snap",
	}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("calls:\n%s", strings.Join(calls, "\n"))
	}
}
