package main

import (
	"fmt"
	"sync/atomic"
)

var (
	audioExtensions = map[string]bool{".flac": true, ".mp3": true, ".ogg": true, ".mpc": true, ".aac": true, ".m4a": true, ".m3u": true, ".m3u8": true}
	copyExtensions  = map[string]bool{".mp3": true, ".ogg": true, ".aac": true, ".m4a": true}
	transExtensions = map[string]bool{".flac": true, ".mpc": true}
	plExtensions    = map[string]bool{".m3u": true, ".m3u8": true}
	dbFilename      = ".music_sync_db.json"
)

type stats struct {
	copied     atomic.Int64
	transcoded atomic.Int64
	skipped    atomic.Int64
	deleted    atomic.Int64
}

func (s *stats) String() string {
	return fmt.Sprintf("  Copied:              %d\n  Transcoded:          %d\n  Skipped (unchanged): %d\n  Deleted:             %d\n",
		s.copied.Load(), s.transcoded.Load(), s.skipped.Load(), s.deleted.Load())
}
