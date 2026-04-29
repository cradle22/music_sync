package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

type dbFileInfo struct {
	Hash      string `json:"hash"`
	Target    string `json:"target"`     // encoded via pathKey for JSON safety
	RelTarget string `json:"rel_target"` // encoded via pathKey for JSON safety
	RunID     int64  `json:"runid"`
	RunTime   string `json:"runtime"`
}

type dbFileInfoJSON struct {
	Hash      string `json:"hash"`
	Target    string `json:"target"`
	RelTarget string `json:"rel_target"`
	RunID     int64  `json:"runid"`
	RunTime   string `json:"runtime"`
}

func (f dbFileInfo) MarshalJSON() ([]byte, error) {
	tmp := dbFileInfoJSON{
		Hash:      f.Hash,
		Target:    encodePathField(f.Target),
		RelTarget: encodePathField(f.RelTarget),
		RunID:     f.RunID,
		RunTime:   f.RunTime,
	}
	return json.Marshal(tmp)
}

func (f *dbFileInfo) UnmarshalJSON(data []byte) error {
	var tmp dbFileInfoJSON
	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}
	f.Hash = tmp.Hash
	f.Target = decodePathField(tmp.Target)
	f.RelTarget = decodePathField(tmp.RelTarget)
	f.RunID = tmp.RunID
	f.RunTime = tmp.RunTime
	return nil
}

type dbData struct {
	// IMPORTANT: keys are pathKey(relSource)
	Files          map[string]dbFileInfo `json:"files"`
	GlobalMetadata struct {
		LastRunID   int64  `json:"last_runid"`
		LastRunTime string `json:"last_runtime"`
	} `json:"global_metadata"`
}

type db struct {
	Path           string
	Data           dbData
	CurrentRunID   int64
	CurrentRunTime string
	mu             sync.RWMutex
}

func dbEmpty() dbData {
	var d dbData
	d.Files = map[string]dbFileInfo{}
	d.GlobalMetadata.LastRunID = 0
	return d
}

func dbLoad(path string) dbData {
	raw, err := os.ReadFile(path)
	if err != nil {
		return dbEmpty()
	}
	var d dbData
	if err := json.Unmarshal(raw, &d); err != nil || d.Files == nil {
		fmt.Fprintln(os.Stderr, "Warning: Corrupted database file, starting fresh")
		return dbEmpty()
	}
	if d.Files == nil {
		d.Files = map[string]dbFileInfo{}
	}

	// Normalize keys for compatibility across DB formats:
	// legacy plain UTF-8 keys, older u: keys, and x: keys for non-UTF8.
	normalized := make(map[string]dbFileInfo, len(d.Files))
	for k, v := range d.Files {
		canonical := pathKey(pathFromKey(k))
		v.Hash = normalizeLegacyHash(v.Hash)
		if cur, exists := normalized[canonical]; !exists || v.RunID > cur.RunID {
			normalized[canonical] = v
		}
	}
	d.Files = normalized

	return d
}

func normalizeLegacyHash(h string) string {
	if h == "" {
		return h
	}
	parts := strings.SplitN(h, ":", 2)
	if len(parts) != 2 {
		return h
	}
	if strings.HasSuffix(parts[0], ".0") {
		parts[0] = strings.TrimSuffix(parts[0], ".0")
		return parts[0] + ":" + parts[1]
	}
	return h
}

func dbNew(path string) *db {
	d := &db{Path: path, Data: dbLoad(path)}
	d.CurrentRunTime = time.Now().Format("2006-01-02T15:04:05")
	d.CurrentRunID = d.Data.GlobalMetadata.LastRunID + 1
	d.Data.GlobalMetadata.LastRunID = d.CurrentRunID
	d.Data.GlobalMetadata.LastRunTime = d.CurrentRunTime
	fmt.Printf("--- Starting Sync Run #%d at %s ---\n", d.CurrentRunID, d.CurrentRunTime)
	return d
}

func (d *db) Save() error {
	d.mu.RLock()
	defer d.mu.RUnlock()

	b, err := json.MarshalIndent(d.Data, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(d.Path, b, 0o644); err != nil {
		return err
	}
	fmt.Printf("Database saved to %s\n", d.Path)
	return nil
}

func fileHashMtimeSize(path string) (string, error) {
	st, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	mtime := st.ModTime().Unix()
	size := st.Size()
	return fmt.Sprintf("%d:%d", mtime, size), nil
}

func (d *db) IsFileChanged(sourceAbs string, relKey string) bool {
	cur, err := fileHashMtimeSize(sourceAbs)
	if err != nil {
		return true
	}
	d.mu.RLock()
	stored := d.Data.Files[relKey].Hash
	d.mu.RUnlock()
	return cur != stored
}

func (d *db) Update(relKey, sourceAbs, targetAbs, relTarget string) {
	cur, err := fileHashMtimeSize(sourceAbs)
	if err != nil {
		cur = ""
	}
	d.mu.Lock()
	d.Data.Files[relKey] = dbFileInfo{
		Hash:      cur,
		Target:    targetAbs,
		RelTarget: relTarget,
		RunID:     d.CurrentRunID,
		RunTime:   d.CurrentRunTime,
	}
	d.mu.Unlock()
}

func (d *db) Remove(relKey string) {
	d.mu.Lock()
	delete(d.Data.Files, relKey)
	d.mu.Unlock()
}

func (d *db) AllSyncedRelKeys() []string {
	d.mu.RLock()
	defer d.mu.RUnlock()

	out := make([]string, 0, len(d.Data.Files))
	for k := range d.Data.Files {
		out = append(out, k)
	}
	return out
}

func (d *db) PrintRuns() {
	d.mu.RLock()
	defer d.mu.RUnlock()

	runs := map[int64]string{}
	for _, v := range d.Data.Files {
		if v.RunID != 0 {
			runs[v.RunID] = v.RunTime
		}
	}
	ids := make([]int64, 0, len(runs))
	for id := range runs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		fmt.Printf("Run ID: %d | Timestamp: %s\n", id, runs[id])
	}
}

func (d *db) FilesByRun(runID int64) [][2]string {
	d.mu.RLock()
	defer d.mu.RUnlock()

	var out [][2]string
	for _, v := range d.Data.Files {
		if v.RunID == runID {
			out = append(out, [2]string{v.Target, v.RelTarget})
		}
	}
	return out
}
