package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
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

type syncer struct {
	sourceAbs  string
	targetAbs  string
	db         *db
	dryRun     bool
	copyOnly   bool
	threads    int
	updateHash bool

	st stats

	mu   sync.Mutex
	pids map[int]struct{}
}

func newSyncer(source, target string, myDb *db, dry, copyOnly bool, threads int, updateHash bool) (*syncer, error) {
	sa, err := filepath.Abs(source)
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
		sourceAbs:  sa,
		targetAbs:  ta,
		db:         myDb,
		dryRun:     dry,
		copyOnly:   copyOnly,
		threads:    threads,
		updateHash: updateHash,
		pids:       map[int]struct{}{},
	}, nil
}

func (s *syncer) checkDeps() {
	if !hasTool("ffmpeg") || !hasTool("ffprobe") {
		fmt.Println("Error: Missing required tools: ffmpeg/ffprobe")
		fmt.Println("Please install: sudo dnf install ffmpeg")
		os.Exit(1)
	}
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
		rel := relPathUnder(s.sourceAbs, p)
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
	fmt.Printf("Processing %d files using %d threads...\n", len(toProcess), n)
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
	rel := relPathUnder(s.sourceAbs, sourceAbs)
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
	relTarget := relPathUnder(s.targetAbs, targetAbs)

	if s.updateHash {
		if _, err := os.Stat(targetAbs); err == nil {
			fmt.Printf("Updating hash for: %s\n", targetAbs)
			return &updateInfo{RelKey: relKey, SourceAbs: sourceAbs, TargetAbs: targetAbs, RelTarget: relTarget}, nil
		}
	}

	if _, err := os.Stat(targetAbs); err != nil {
		fmt.Printf("Target missing, reprocessing: %s\n", targetAbs)
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

func (s *syncer) targetPathFor(sourceAbs string, newExt *string) string {
	rel := relPathUnder(s.sourceAbs, sourceAbs)
	tgt := filepath.Join(s.targetAbs, rel)
	if newExt != nil {
		tgt = withSuffix(tgt, *newExt)
	}
	return tgt
}

func (s *syncer) copyFile(src, dst string) error {
	if s.dryRun {
		fmt.Printf("[DRY RUN] Would copy: %s -> %s\n", src, dst)
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

	fmt.Printf("Copied: %s\n", filepath.Base(src))
	s.st.copied.Add(1)
	return nil
}

func (s *syncer) handlePlaylist(src, dst string) error {
	if s.dryRun {
		fmt.Printf("[DRY RUN] Would handle playlist: %s -> %s\n", src, dst)
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

	srcRoot := filepath.Clean(s.sourceAbs)
	dstRoot := filepath.Clean(s.targetAbs)
	dstDir := filepath.Dir(dst)

	sc := bufio.NewScanner(in)
	lineNo := 0
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

		if _, err := fmt.Fprintln(out, line); err != nil {
			return err
		}
	}
	return sc.Err()
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
		fmt.Printf("[DRY RUN] Would transcode: %s -> %s\n", sourceAbs, targetAbs)
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
	fmt.Printf("Transcoded: %s -> %s\n", filepath.Base(sourceAbs), filepath.Base(targetAbs))
	s.st.transcoded.Add(1)
	return nil
}

type updateInfo struct {
	RelKey    string
	SourceAbs string
	TargetAbs string
	RelTarget string
}

func (s *syncer) collectFiles(ctx context.Context) ([]string, error) {
	var out []string
	err := filepath.WalkDir(s.sourceAbs, func(path string, d fs.DirEntry, err error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			// keep walking, but report error up
			return err
		}
		if d.IsDir() {
			return nil
		}
		ext := strings.ToLower(filepath.Ext(path))
		if audioExtensions[ext] {
			out = append(out, path)
		}
		return nil
	})
	return out, err
}

func (s *syncer) removeDeletedAndOrphans(ctx context.Context, currentRelKeys map[string]bool) error {
	fmt.Println("Looking for deleted files")
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
			currentTarget := filepath.Join(s.targetAbs, info.RelTarget)
			if st, err := os.Stat(currentTarget); err == nil && !st.IsDir() {
				deleted = true
				if s.dryRun {
					fmt.Printf("[DRY RUN] Would delete: %s\n", currentTarget)
				} else {
					_ = os.Remove(currentTarget)
					fmt.Printf("Deleted: %s\n", currentTarget)
					s.st.deleted.Add(1)
				}
			}
		}

		// Fallback: absolute DB path only if under current target root
		if !deleted && info.Target != "" {
			tgt := filepath.Clean(info.Target)
			root := filepath.Clean(s.targetAbs) + string(os.PathSeparator)
			if strings.HasPrefix(tgt+string(os.PathSeparator), root) || strings.HasPrefix(tgt, root) {
				if st, err := os.Stat(tgt); err == nil && !st.IsDir() {
					if s.dryRun {
						fmt.Printf("[DRY RUN] Would delete: %s\n", tgt)
					} else {
						_ = os.Remove(tgt)
						fmt.Printf("Deleted: %s\n", tgt)
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
			known[absKey(filepath.Join(s.targetAbs, info.RelTarget))] = true
		}
	}

	var orphans []string
	filepath.WalkDir(s.targetAbs, func(path string, d fs.DirEntry, err error) error {
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
			fmt.Printf("[DRY RUN] Would delete orphan target file: %s\n", p)
			continue
		}
		_ = os.Remove(p)
		if _, err := os.Stat(p); os.IsNotExist(err) {
			fmt.Printf("Deleted orphan target file: %s\n", p)
			s.st.deleted.Add(1)
		}
	}

	// prune empty dirs bottom-up (keep target root)
	if s.dryRun {
		return nil
	}
	var dirs []string
	filepath.WalkDir(s.targetAbs, func(path string, d fs.DirEntry, err error) error {
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
		if filepath.Clean(dir) == filepath.Clean(s.targetAbs) {
			continue
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		if len(entries) == 0 {
			if err := os.Remove(dir); err == nil {
				fmt.Printf("Removed empty directory: %s\n", dir)
			}
		}
	}
	return nil
}

func (s *syncer) dumpRun(ctx context.Context, runID int64, dumpTarget string) error {
	entries := s.db.FilesByRun(runID)
	if len(entries) == 0 {
		fmt.Printf("No files found for Run ID: %d\n", runID)
		return nil
	}
	fmt.Printf("Dumping %d files from Run %d to %s...\n", len(entries), runID, dumpTarget)
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
			fmt.Printf("Warning: Source file %s missing, skipping.\n", absSrc)
			continue
		}
		dst := filepath.Join(dumpTarget, rel)
		if s.dryRun {
			fmt.Printf("[DRY RUN] Would copy %s -> %s\n", absSrc, dst)
			continue
		}
		if err := mkdirForFile(dst); err != nil {
			return err
		}
		if err := copyFileSimple(absSrc, dst); err != nil {
			fmt.Fprintf(os.Stderr, "Copy failed: %v\n", err)
			continue
		}
		fmt.Printf("Dumped: %s\n", rel)
	}
	return nil
}

func copyFileSimple(src, dst string) error {
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
	_, err = io.Copy(out, in)
	return err
}
