package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type syncPlaylistEntry struct {
	SourceFile string
	TargetFile string
}

type syncPlaylist struct {
	SourceAbs string
	TargetAbs string
	Entries   []syncPlaylistEntry
}
type syncer struct {
	sourceAbsolutePath string
	targetAbsolutePath string
	db                 *db
	dryRun             bool
	copyOnly           bool
	threads            int
	updateHash         bool
	plRewrite          bool
	st                 stats

	mu        sync.Mutex
	pids      map[int]struct{}
	playlists map[string]syncPlaylist
}

func newSyncer(source, target string, myDb *db, dry, copyOnly bool, threads int, updateHash, plRewrite bool) (*syncer, error) {
	sourceAbsolute, err := filepath.Abs(source)
	if err != nil {
		return nil, err
	}
	ta, err := filepath.Abs(target)
	if err != nil {
		return nil, err
	}
	if threads <= 0 {
		threads = runtime.NumCPU()
	}
	return &syncer{
		sourceAbsolutePath: sourceAbsolute,
		targetAbsolutePath: ta,
		db:                 myDb,
		dryRun:             dry,
		copyOnly:           copyOnly,
		threads:            threads,
		updateHash:         updateHash,
		plRewrite:          plRewrite,
		pids:               map[int]struct{}{},
		playlists:          map[string]syncPlaylist{},
	}, nil
}

func (s *syncer) processParallel(ctx context.Context, files []string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	fmt.Println("Compiling list of files to process")
	toProcess := make([]string, 0, len(files))
	for _, p := range files {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if s.updateHash {
			toProcess = append(toProcess, p)
			continue
		}
		rel := relPathUnder(s.sourceAbsolutePath, p)
		relKey := pathKey(rel)

		ext := strings.ToLower(filepath.Ext(p))

		var targetAbs string
		if plExtensions[ext] {
			targetAbs = s.targetPathFor(p, nil)
		} else if s.copyOnly || copyExtensions[ext] {
			targetAbs = s.targetPathFor(p, nil)
		} else if transExtensions[ext] {
			mp3 := ".mp3"
			targetAbs = s.targetPathFor(p, &mp3)
		}

		if targetAbs == "" {
			continue
		}
		if _, err := os.Stat(targetAbs); err != nil || s.db.IsFileChanged(p, relKey) {
			toProcess = append(toProcess, p)
		} else {
			s.st.skipped.Add(1)
		}
	}

	if len(toProcess) == 0 {
		fmt.Println("No files need processing")
		return nil
	}

	n := s.threads
	slog.Info(fmt.Sprintf("Processing %d files using %d threads...", len(toProcess), n))
	sort.Strings(toProcess)

	workCh := make(chan string)
	updateCh := make(chan *updateInfo, 1024)
	errCh := make(chan error, 1)

	// Enqueue
	go func() {
		defer close(workCh)
		for _, p := range toProcess {
			select {
			case <-ctx.Done():
				return
			case workCh <- p:
			}
		}
	}()

	var wg sync.WaitGroup
	worker := func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case p, ok := <-workCh:
				if !ok {
					return
				}
				upd, err := s.syncOne(ctx, p)
				if err != nil {
					cancel()
					// Cancel everyone and stop ffmpeg
					s.terminateActiveFFmpeg()
					select {
					case errCh <- err:
					default:
					}
					return
				}
				if upd != nil {
					updateCh <- upd
				}
			}
		}
	}

	wg.Add(n)
	for i := 0; i < n; i++ {
		go worker()
	}

	// Close updateCh after workers finish
	go func() {
		wg.Wait()
		close(updateCh)
	}()

	// Apply updates serially
	for upd := range updateCh {
		s.db.Update(upd.RelKey, upd.SourceAbs, upd.TargetAbs, upd.RelTarget)
	}

	select {
	case err := <-errCh:
		return err
	default:
		return ctx.Err()
	}
}

func (s *syncer) syncOne(ctx context.Context, sourceAbs string) (*updateInfo, error) {
	rel := relPathUnder(s.sourceAbsolutePath, sourceAbs)
	relKey := pathKey(rel)

	ext := strings.ToLower(filepath.Ext(sourceAbs))

	var targetAbs string
	if plExtensions[ext] {
		targetAbs = s.targetPathFor(sourceAbs, nil)
	} else if s.copyOnly || copyExtensions[ext] {
		targetAbs = s.targetPathFor(sourceAbs, nil)
	} else if transExtensions[ext] {
		mp3 := ".mp3"
		targetAbs = s.targetPathFor(sourceAbs, &mp3)
	} else {
		return nil, nil
	}
	relTarget := relPathUnder(s.targetAbsolutePath, targetAbs)

	if s.updateHash {
		if _, err := os.Stat(targetAbs); err == nil {
			slog.Info("Updating hash for", "file", targetAbs)
			return &updateInfo{RelKey: relKey, SourceAbs: sourceAbs, TargetAbs: targetAbs, RelTarget: relTarget}, nil
		}
	}

	if _, err := os.Stat(targetAbs); err != nil {
		slog.Info("Target missing, reprocessing", "file", targetAbs)
	} else if !s.db.IsFileChanged(sourceAbs, relKey) {
		s.st.skipped.Add(1)
		return nil, nil
	}

	if plExtensions[ext] {
		if err := s.handlePlaylist(sourceAbs, targetAbs); err != nil {
			return nil, err
		}
		return &updateInfo{RelKey: relKey, SourceAbs: sourceAbs, TargetAbs: targetAbs, RelTarget: relTarget}, nil
	}
	if s.copyOnly || copyExtensions[ext] {
		if err := s.copyFile(sourceAbs, targetAbs); err != nil {
			return nil, err
		}
		return &updateInfo{RelKey: relKey, SourceAbs: sourceAbs, TargetAbs: targetAbs, RelTarget: relTarget}, nil
	}
	if transExtensions[ext] {
		if err := s.transcodeToMP3(ctx, sourceAbs, targetAbs); err != nil {
			return nil, err
		}
		return &updateInfo{RelKey: relKey, SourceAbs: sourceAbs, TargetAbs: targetAbs, RelTarget: relTarget}, nil
	}
	return nil, nil
}

func (s *syncer) rewritePlaylists(ctx context.Context) error {
	fmt.Println("Rewriting playlists if needed...")
	targetMetaData := make(map[string]songTagData)
	sourceMetaData := make(map[string]songTagData)
	allTargetFiles := []string{}
	var err error
	pls := s.playlistsSnapshot()

	for name := range pls {
		playlist := pls[name]
		var changed = false
		slog.Info("Playlist Name", "name", name)
		slog.Info("Source Path", "path", playlist.SourceAbs)

		// You can also nest a loop to iterate through the Entries slice
		for i := range playlist.Entries {
			lookFile := s.targetPathFor(playlist.Entries[i].TargetFile, nil)
			if _, statErr := os.Stat(lookFile); statErr == nil {
				continue
			} else if errors.Is(statErr, os.ErrNotExist) {

				// ok, we need to try to find it anywhere inside the target folders
				slog.Info("File does NOT exist", "file", playlist.Entries[i].TargetFile)

				// Build metadata for all target files if not already done (lazy)
				if len(allTargetFiles) == 0 {
					slog.Info("Collecting all target files...")
					allTargetFiles, err = collectFiles(ctx, s.sourceAbsolutePath)
					if err != nil {
						return err
					}
				}

				// get source metadata
				if _, ok := sourceMetaData[playlist.Entries[i].SourceFile]; !ok {
					sourceTagData, err := getSongMetadata(playlist.Entries[i].SourceFile)
					if err != nil {
						return err
					}
					sourceMetaData[playlist.Entries[i].SourceFile] = sourceTagData
				}
				if sourceTagData, ok := sourceMetaData[playlist.Entries[i].SourceFile]; ok {

					// try to find a match in targetMetaData, first using artist+title, then fallback to title only
					var match *songTagData = findSongMatch(sourceTagData, targetMetaData)
					if match == nil {
						// maybe not all files have been added to the targetMetaData map yet
						targetMetaData, err = getSongMetadataForFiles(ctx, allTargetFiles, targetMetaData, sourceTagData)
						if err != nil {
							return err
						}
						match = findSongMatch(sourceTagData, targetMetaData)
					}

					if match != nil {
						slog.Info("Found match", "artist", sourceTagData.Artist, "title", sourceTagData.Title, "file", match.SourceFile)
						playlist.Entries[i].TargetFile = relPathUnder(s.targetAbsolutePath, match.SourceFile)
						changed = true
					} else {
						slog.Info("No match found", "artist", sourceTagData.Artist, "title", sourceTagData.Title)
					}
				}
			}
		}
		if changed {
			// write the changed playlist back to disk
			slog.Info("Rewriting playlist", "file", playlist.TargetAbs)
			out, err := os.Create(playlist.TargetAbs)
			if err != nil {
				return err
			}
			defer func() { _ = out.Close() }()
			for _, entry := range playlist.Entries {
				if _, err := fmt.Fprintln(out, entry.TargetFile); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (s *syncer) playlistsSnapshot() map[string]syncPlaylist {
	s.mu.Lock()
	defer s.mu.Unlock()

	snapshot := make(map[string]syncPlaylist, len(s.playlists))
	for key, playlist := range s.playlists {
		snapshot[key] = playlist
	}
	return snapshot
}

func (s *syncer) targetPathFor(sourceAbs string, newExt *string) string {
	rel := relPathUnder(s.sourceAbsolutePath, sourceAbs)
	tgt := filepath.Join(s.targetAbsolutePath, rel)
	if newExt != nil {
		tgt = withSuffix(tgt, *newExt)
	}
	return tgt
}

func (s *syncer) copyFile(src, dst string) error {
	if s.dryRun {
		slog.Info("Dry run: would copy file", "src", src, "dst", dst)
		return nil
	}
	if err := mkdirForFile(dst); err != nil {
		return err
	}

	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() { _ = out.Close() }()

	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}

	slog.Info("Copied file", "file", filepath.Base(src))
	s.st.copied.Add(1)
	return nil
}

func (s *syncer) handlePlaylist(src, dst string) error {
	if s.dryRun {
		slog.Info("Dry run: would handle playlist", "src", src, "dst", dst)
		return nil
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	if err := mkdirForFile(dst); err != nil {
		return err
	}
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() { _ = out.Close() }()

	srcRoot := filepath.Clean(s.sourceAbsolutePath)
	dstRoot := filepath.Clean(s.targetAbsolutePath)
	dstDir := filepath.Dir(dst)

	sc := bufio.NewScanner(in)
	lineNo := 0
	p := syncPlaylist{
		SourceAbs: src,
		TargetAbs: dst,
	}
	for sc.Scan() {
		lineNo++
		line := sc.Text()

		// Strip UTF-8 BOM on first line if present.
		if lineNo == 1 {
			line = strings.TrimPrefix(line, "\uFEFF")
			line = strings.TrimPrefix(line, "\xEF\xBB\xBF")
		}

		line = strings.TrimRight(line, "\r\n")
		if strings.HasPrefix(line, "#") || line == "" {
			continue
		}
		pl := syncPlaylistEntry{
			SourceFile: line,
		}

		// Normalize Windows separators.
		line = strings.ReplaceAll(line, "\\", "/")

		ext := strings.ToLower(filepath.Ext(line))

		// Rebase absolute playlist entries under source root to target root.
		if strings.HasPrefix(line, "/") {
			abs := filepath.Clean(line)
			if strings.HasPrefix(abs, srcRoot+string(os.PathSeparator)) || abs == srcRoot {
				rel := strings.TrimPrefix(abs, srcRoot)
				rel = strings.TrimPrefix(rel, string(os.PathSeparator))
				rebasedAbs := filepath.Join(dstRoot, rel)
				relToPlaylist, _ := filepath.Rel(dstDir, rebasedAbs)
				relToPlaylist = filepath.ToSlash(relToPlaylist)
				line = relToPlaylist
			}
		}

		// Handle possible transcoding in playlist refs
		if transExtensions[ext] && !s.copyOnly {
			line = withSuffix(line, ".mp3")
		}
		pl.TargetFile = line
		p.Entries = append(p.Entries, pl)
		if _, err := fmt.Fprintln(out, line); err != nil {
			return err
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	s.playlists[src] = p
	s.mu.Unlock()

	return nil
}

func (s *syncer) replayGainDB(ctx context.Context, sourceAbs string) (*float64, error) {
	cmd := exec.CommandContext(ctx, "ffprobe",
		"-v", "quiet",
		"-show_entries", "format_tags=replaygain_track_gain",
		"-of", "default=noprint_wrappers=1:nokey=1",
		sourceAbs,
	)
	out, err := cmd.Output()
	if err != nil {
		return nil, nil // treat as absent
	}
	txt := strings.TrimSpace(string(out))
	if txt == "" {
		return nil, nil
	}
	fields := strings.Fields(txt)
	if len(fields) == 0 {
		return nil, nil
	}
	v, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return nil, nil
	}
	return &v, nil
}

func (s *syncer) registerPID(pid int) {
	s.mu.Lock()
	s.pids[pid] = struct{}{}
	s.mu.Unlock()
}

func (s *syncer) unregisterPID(pid int) {
	s.mu.Lock()
	delete(s.pids, pid)
	s.mu.Unlock()
}

func (s *syncer) terminateActiveFFmpeg() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for pid := range s.pids {
		_ = syscall.Kill(pid, syscall.SIGINT)
		_ = syscall.Kill(pid, syscall.SIGTERM)
	}
}

func (s *syncer) transcodeToMP3(ctx context.Context, sourceAbs, targetAbs string) error {
	if s.dryRun {
		slog.Info("Dry run: would transcode", "src", sourceAbs, "dst", targetAbs)
		return nil
	}
	if err := mkdirForFile(targetAbs); err != nil {
		return err
	}

	args := []string{
		"-i", sourceAbs,
		"-c:a", "libmp3lame",
		"-q:a", "0",
		"-map", "0:a",
		"-map", "0:v?",
		"-id3v2_version", "3",
		"-write_id3v1", "1",
		"-y",
	}

	// Keep ffprobe short-ish so ctrl-c doesn't hang
	rgCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	rg, _ := s.replayGainDB(rgCtx, sourceAbs)
	cancel()

	if rg != nil {
		//fmt.Printf("Applying ReplayGain: %gdB to %s\n", *rg, filepath.Base(sourceAbs))
		args = append(args, "-af", fmt.Sprintf("volume=%gdB", *rg))
	}
	args = append(args, targetAbs)

	cmd := exec.CommandContext(ctx, "ffmpeg", args...)

	var stderr bytes.Buffer
	cmd.Stdout = io.Discard
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("cannot run ffmpeg: %w", err)
	}
	if cmd.Process != nil {
		s.registerPID(cmd.Process.Pid)
	}
	err := cmd.Wait()
	if cmd.Process != nil {
		s.unregisterPID(cmd.Process.Pid)
	}

	// If context canceled (Ctrl-C), prefer that error
	if ctx.Err() != nil {
		return ctx.Err()
	}

	if err != nil {
		return fmt.Errorf("error transcoding %s:\n%s", sourceAbs, stderr.String())
	}
	slog.Info("Transcoded file", "src", filepath.Base(sourceAbs), "dst", filepath.Base(targetAbs))
	s.st.transcoded.Add(1)
	return nil
}

type updateInfo struct {
	RelKey    string
	SourceAbs string
	TargetAbs string
	RelTarget string
}

func (s *syncer) removeDeletedAndOrphans(ctx context.Context, currentRelKeys map[string]bool) error {
	slog.Info("Looking for deleted files")
	for _, relKey := range s.db.AllSyncedRelKeys() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if currentRelKeys[relKey] {
			continue
		}
		info := s.db.Data.Files[relKey]
		deleted := false

		// Prefer rel_target with current root
		if info.RelTarget != "" {
			currentTarget := filepath.Join(s.targetAbsolutePath, info.RelTarget)
			if st, err := os.Stat(currentTarget); err == nil && !st.IsDir() {
				deleted = true
				if s.dryRun {
					slog.Info("Dry run: would delete file", "file", currentTarget)
				} else {
					_ = os.Remove(currentTarget)
					slog.Info("Deleted file", "file", currentTarget)
					s.st.deleted.Add(1)
				}
			}
		}

		// Fallback: absolute DB path only if under current target root
		if !deleted && info.Target != "" {
			tgt := filepath.Clean(info.Target)
			root := filepath.Clean(s.targetAbsolutePath) + string(os.PathSeparator)
			if strings.HasPrefix(tgt+string(os.PathSeparator), root) || strings.HasPrefix(tgt, root) {
				if st, err := os.Stat(tgt); err == nil && !st.IsDir() {
					if s.dryRun {
						slog.Info("Dry run: would delete file", "file", tgt)
					} else {
						_ = os.Remove(tgt)
						slog.Info("Deleted file", "file", tgt)
						s.st.deleted.Add(1)
					}
				}
			}
		}

		// Always remove DB entry
		s.db.Remove(relKey)
	}

	// Second pass: remove any target file not tracked by DB
	fmt.Println("Looking for orphans")

	known := map[string]bool{}
	// Keep DB file itself
	known[absKey(s.db.Path)] = true

	for _, info := range s.db.Data.Files {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if info.Target != "" {
			known[absKey(info.Target)] = true
		}
		if info.RelTarget != "" {
			known[absKey(filepath.Join(s.targetAbsolutePath, info.RelTarget))] = true
		}
	}

	var orphans []string
	filepath.WalkDir(s.targetAbsolutePath, func(path string, d fs.DirEntry, err error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			return nil
		}
		if d.IsDir() {
			return nil
		}
		if known[absKey(path)] {
			return nil
		}
		orphans = append(orphans, path)
		return nil
	})

	for _, p := range orphans {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if s.dryRun {
			slog.Info("Dry run: would delete orphan target file", "file", p)
			continue
		}
		_ = os.Remove(p)
		if _, err := os.Stat(p); os.IsNotExist(err) {
			slog.Info("Deleted orphan target file", "file", p)
			s.st.deleted.Add(1)
		}
	}

	// prune empty dirs bottom-up (keep target root)
	if s.dryRun {
		return nil
	}
	var dirs []string
	filepath.WalkDir(s.targetAbsolutePath, func(path string, d fs.DirEntry, err error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err == nil && d.IsDir() {
			dirs = append(dirs, path)
		}
		return nil
	})
	sort.Slice(dirs, func(i, j int) bool { return len(dirs[i]) > len(dirs[j]) })
	for _, dir := range dirs {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if filepath.Clean(dir) == filepath.Clean(s.targetAbsolutePath) {
			continue
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		if len(entries) == 0 {
			if err := os.Remove(dir); err == nil {
				slog.Info("Removed empty directory", "dir", dir)
			}
		}
	}
	return nil
}

func (s *syncer) dumpRun(ctx context.Context, runID int64, dumpTarget string) error {
	entries := s.db.FilesByRun(runID)
	if len(entries) == 0 {
		slog.Info("No files found for Run ID", "runID", runID)
		return nil
	}
	slog.Info("Dumping files from Run", "count", len(entries), "runID", runID, "dumpTarget", dumpTarget)
	for _, e := range entries {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		absSrc := e[0]
		rel := e[1]
		if absSrc == "" || rel == "" {
			continue
		}
		if st, err := os.Stat(absSrc); err != nil || st.IsDir() {
			slog.Warn("Warning: Source file missing, skipping", "file", absSrc)
			continue
		}
		dst := filepath.Join(dumpTarget, rel)
		if s.dryRun {
			slog.Info("Dry run: would copy file", "src", absSrc, "dst", dst)
			continue
		}
		if err := mkdirForFile(dst); err != nil {
			return err
		}
		if err := copyFileSimple(absSrc, dst); err != nil {
			slog.Error("Copy failed", "src", absSrc, "dst", dst, "error", err)
			continue
		}
		slog.Info("Dumped file", "file", rel)
	}
	return nil
}
