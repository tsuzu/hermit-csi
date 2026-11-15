package lvm

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"strconv"

	"github.com/tsuzu/hermit-csi/pkg/backup"
)

const sectorSize = 512

func attr(se xml.StartElement, name string) (string, bool) {
	for _, a := range se.Attr {
		if a.Name.Local == name {
			return a.Value, true
		}
	}
	return "", false
}

func attrInt(se xml.StartElement, name string) (int64, error) {
	s, ok := attr(se, name)
	if !ok {
		return 0, fmt.Errorf("<%s> missing attribute %q", se.Name.Local, name)
	}
	return strconv.ParseInt(s, 10, 64)
}

// blockBytes reads data_block_size (in 512-byte sectors) from <superblock>.
func blockBytes(se xml.StartElement) (int64, error) {
	n, err := attrInt(se, "data_block_size")
	if err != nil {
		return 0, err
	}
	if n <= 0 {
		return 0, fmt.Errorf("invalid data_block_size %d", n)
	}
	return n * sectorSize, nil
}

// ParseThinDelta parses `thin_delta` XML. Every <different>, <left_only> and
// <right_only> range counts as changed (left_only = discarded => now zero).
func ParseThinDelta(out []byte) ([]backup.Range, error) {
	dec := xml.NewDecoder(bytes.NewReader(out))
	var bs int64
	var ranges []backup.Range
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("parse thin_delta: %w", err)
		}
		se, ok := tok.(xml.StartElement)
		if !ok {
			continue
		}
		switch se.Name.Local {
		case "superblock":
			if bs, err = blockBytes(se); err != nil {
				return nil, err
			}
		case "different", "left_only", "right_only":
			if bs == 0 {
				return nil, fmt.Errorf("thin_delta: range before <superblock>")
			}
			b, err := attrInt(se, "begin")
			if err != nil {
				return nil, err
			}
			l, err := attrInt(se, "length")
			if err != nil {
				return nil, err
			}
			ranges = append(ranges, backup.Range{Off: b * bs, Len: l * bs})
		}
	}
	if bs == 0 {
		return nil, fmt.Errorf("thin_delta: no <superblock> in output")
	}
	return ranges, nil
}

// ParseThinDump parses `thin_dump` XML and returns the mapped (allocated)
// virtual ranges of device devID.
func ParseThinDump(out []byte, devID int) ([]backup.Range, error) {
	dec := xml.NewDecoder(bytes.NewReader(out))
	var bs int64
	inDev, found := false, false
	var ranges []backup.Range
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("parse thin_dump: %w", err)
		}
		switch t := tok.(type) {
		case xml.StartElement:
			switch t.Name.Local {
			case "superblock":
				if bs, err = blockBytes(t); err != nil {
					return nil, err
				}
			case "device":
				id, err := attrInt(t, "dev_id")
				if err != nil {
					return nil, err
				}
				inDev = int(id) == devID
				found = found || inDev
			case "range_mapping":
				if !inDev {
					continue
				}
				b, err := attrInt(t, "origin_begin")
				if err != nil {
					return nil, err
				}
				l, err := attrInt(t, "length")
				if err != nil {
					return nil, err
				}
				ranges = append(ranges, backup.Range{Off: b * bs, Len: l * bs})
			case "single_mapping":
				if !inDev {
					continue
				}
				b, err := attrInt(t, "origin_block")
				if err != nil {
					return nil, err
				}
				ranges = append(ranges, backup.Range{Off: b * bs, Len: bs})
			}
		case xml.EndElement:
			if t.Name.Local == "device" {
				inDev = false
			}
		}
	}
	if bs == 0 {
		return nil, fmt.Errorf("thin_dump: no <superblock> in output")
	}
	if !found {
		return nil, fmt.Errorf("thin_dump: device %d not found", devID)
	}
	return ranges, nil
}
