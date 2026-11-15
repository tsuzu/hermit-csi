package manifest

import "testing"

func TestRoundTripAndValidation(t *testing.T) {
	m := New("v", 10<<20+1, 4<<20)
	if m.NumChunks() != 3 || m.ChunkLen(2) != 2<<20+1 {
		t.Fatalf("chunks=%d last=%d", m.NumChunks(), m.ChunkLen(2))
	}
	m.Chunks[1] = "abc"
	b, err := m.Encode()
	if err != nil {
		t.Fatal(err)
	}
	got, err := Decode(b)
	if err != nil || got.Chunks[1] != "abc" || got.UsedBytes() != 4<<20 {
		t.Fatalf("decode: %+v %v", got, err)
	}
	if err := got.Resize(1 << 20); err == nil {
		t.Fatal("shrink must fail")
	}
	if err := got.Resize(20 << 20); err != nil || got.NumChunks() != 5 {
		t.Fatalf("resize: %v %d", err, got.NumChunks())
	}
	got.Chunks = got.Chunks[:2]
	b, _ = got.Encode()
	if _, err := Decode(b); err == nil {
		t.Fatal("inconsistent chunk count must be rejected")
	}
}

func TestIsManifestKey(t *testing.T) {
	for k, want := range map[string]bool{
		LatestKey("v"):   true,
		SnapshotKey("s"): true,
		OwnerKey("v"):    false,
		ChunkKey("abc"):  false,
	} {
		if IsManifestKey(k) != want {
			t.Errorf("IsManifestKey(%q) != %v", k, want)
		}
	}
}
