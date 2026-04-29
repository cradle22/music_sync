package main

import (
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"unicode/utf8"
)

// pathKey returns a stable representation for a filesystem path string, even if it
// contains odd bytes / invalid UTF-8. This helps keep DB keys consistent.
func pathKey(p string) string {
	// Keep valid UTF-8 unchanged for backward compatibility with existing DB files.
	if utf8.ValidString(p) {
		return p
	}
	// Otherwise store hex of raw bytes.
	return "x:" + hex.EncodeToString([]byte(p))
}

// pathKeyDecode is best-effort decoding for display/logging only.
// If key is x:..., returns a printable placeholder. Plain UTF-8 keys are returned as-is.
func pathKeyDecode(key string) string {
	if strings.HasPrefix(key, "x:") {
		b, err := hex.DecodeString(strings.TrimPrefix(key, "x:"))
		if err != nil {
			return "<invalid-path>"
		}
		// Display as escaped bytes; do not attempt to force UTF-8.
		return fmt.Sprintf("<bytes:%x>", b)
	}
	return key
}

// pathFromKey decodes pathKey() output back to the original Go string bytes.
// For plain values (legacy/current UTF-8), it returns the input unchanged.
func pathFromKey(key string) string {
	if strings.HasPrefix(key, "x:") {
		b, err := hex.DecodeString(strings.TrimPrefix(key, "x:"))
		if err != nil {
			return key
		}
		return string(b)
	}
	return key
}

func encodePathField(p string) string {
	if p == "" {
		return ""
	}
	return pathKey(p)
}

func decodePathField(v string) string {
	if v == "" {
		return ""
	}
	return pathFromKey(v)
}

// cleanJoinKeyed: normalize abs path and convert to key for map comparisons.
func absKey(p string) string {
	abs, err := filepath.Abs(p)
	if err != nil {
		abs = p
	}
	abs = filepath.Clean(abs)
	return pathKey(abs)
}

func relPathUnder(baseAbs, pathAbs string) string {
	base := filepath.Clean(baseAbs) + string(os.PathSeparator)
	p := filepath.Clean(pathAbs)
	if strings.HasPrefix(p, base) {
		return strings.TrimPrefix(p, base)
	}
	return pathAbs
}

func withSuffix(path, ext string) string {
	return strings.TrimSuffix(path, filepath.Ext(path)) + ext
}

func mkdirForFile(path string) error {
	dir := filepath.Dir(path)
	return os.MkdirAll(dir, 0o755)
}

func hasTool(name string) bool {
	_, err := exec.LookPath(name)
	return err == nil
}
