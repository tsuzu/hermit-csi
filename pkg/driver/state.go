package driver

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// volState is the node-local record of a volume's LV, persisted as JSON so
// the node can resume backups after a restart.
type volState struct {
	VolumeID   string `json:"volumeId"`
	Epoch      int64  `json:"epoch"`      // ownership epoch we were granted
	ETag       string `json:"etag"`       // ETag of latest.json we last wrote/read
	Generation int64  `json:"generation"` // generation the base snapshot corresponds to
	BaseSnap   string `json:"baseSnap"`   // thin snapshot == state uploaded as Generation
	Staged     bool   `json:"staged"`
	Fenced     bool   `json:"fenced"` // another owner took over; never write again
	Block      bool   `json:"block"`
	Staging    string `json:"staging,omitempty"`
	// BackupInterval overrides the node default (from the volume context).
	BackupInterval string `json:"backupInterval,omitempty"`
	// LastBackup is when the newest state known to be in S3 was taken: the
	// snapshot time of the last successful backup, or the stage time (the
	// local LV then equals latest.json). Exported as a metric.
	LastBackup time.Time `json:"lastBackup,omitzero"`
	// BackupFailures counts failed backups since the last successful one.
	BackupFailures int `json:"backupFailures,omitempty"`
}

type stateStore struct{ dir string }

func (s stateStore) path(vol string) string { return filepath.Join(s.dir, vol+".json") }

func (s stateStore) load(vol string) (*volState, error) {
	b, err := os.ReadFile(s.path(vol))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var st volState
	if err := json.Unmarshal(b, &st); err != nil {
		return nil, err
	}
	return &st, nil
}

func (s stateStore) save(st *volState) error {
	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path(st.VolumeID) + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, s.path(st.VolumeID))
}

func (s stateStore) remove(vol string) error {
	err := os.Remove(s.path(vol))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (s stateStore) list() ([]string, error) {
	ents, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	var vols []string
	for _, e := range ents {
		if n := e.Name(); strings.HasSuffix(n, ".json") && !e.IsDir() {
			vols = append(vols, strings.TrimSuffix(n, ".json"))
		}
	}
	return vols, nil
}
