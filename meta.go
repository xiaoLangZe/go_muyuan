package download

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const metaVersion = 1

// fileMeta is the on-disk sidecar (<dst>.dltmp.json) used for resuming.
type fileMeta struct {
	Version      int       `json:"version"`
	URL          string    `json:"url"`
	ETag         string    `json:"etag,omitempty"`
	LastModified string    `json:"last_modified,omitempty"`
	Total        int64     `json:"total"`
	ChunkSize    int64     `json:"chunk_size"`
	Done         []bool    `json:"done"`
	SavedAt      time.Time `json:"saved_at"`
}

// saveMetaFile writes the sidecar atomically: a partial write must never be
// mistaken for a valid resume point.
func saveMetaFile(path string, m *fileMeta) error {
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".dltmp-*.json")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

// loadMetaFile returns nil when the sidecar is missing, unreadable or invalid.
// Corrupt metadata is treated as "no resume point" rather than an error.
func loadMetaFile(path string) *fileMeta {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var m fileMeta
	if err := json.Unmarshal(data, &m); err != nil {
		return nil
	}
	if m.Version != metaVersion || m.Total < 0 || m.ChunkSize <= 0 || len(m.Done) == 0 {
		return nil
	}
	if needed := int((m.Total + m.ChunkSize - 1) / m.ChunkSize); needed != len(m.Done) {
		return nil
	}
	return &m
}

func removeMetaFile(path string) {
	_ = os.Remove(path)
}

// validateMeta checks that a sidecar describes the same file the server is
// serving now. A mismatch means the resume point is unusable.
func validateMeta(m *fileMeta, url string, total, chunkSize int64) error {
	if m == nil {
		return fmt.Errorf("download: no metadata")
	}
	if m.URL != url {
		return fmt.Errorf("download: metadata url changed")
	}
	if m.Total != total {
		return fmt.Errorf("download: metadata size changed")
	}
	if m.ChunkSize != chunkSize {
		return fmt.Errorf("download: metadata chunk size changed")
	}
	return nil
}
