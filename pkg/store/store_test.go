package store

import "testing"

func TestKeyPrefix(t *testing.T) {
	s := &Store{prefix: "cluster-a"}
	for in, want := range map[string]string{
		"chunks/":              "cluster-a/chunks/",
		"chunks/abc":           "cluster-a/chunks/abc",
		"volumes/v/owner.json": "cluster-a/volumes/v/owner.json",
	} {
		if got := s.key(in); got != want {
			t.Errorf("key(%q) = %q, want %q", in, got, want)
		}
		if got := s.rel(s.key(in)); got != in && in != "chunks/" {
			t.Errorf("rel(key(%q)) = %q", in, got)
		}
	}
	if got := (&Store{}).key("chunks/"); got != "chunks/" {
		t.Errorf("no prefix: %q", got)
	}
}
