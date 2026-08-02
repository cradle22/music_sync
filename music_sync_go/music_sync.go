package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
)

func usage() {
	fmt.Println(`Usage: music_sync [options] <source> <target>

  --print               Print all distinct run IDs and runtimes
  --dump RUNID          Copy all files from a specific run ID to --dump-target
  --dump-target PATH    Target folder for --dump
  --dry-run             Show what would be done without making changes
  --copy-only           Copy all files without transcoding
  --threads|-j N        Number of parallel workers (default: CPU count)
  --db PATH             Database file location (default: target/.music_sync_db.json)
  --update-hash         Updates the hash if the target file exists
  --plrewrite           Tries to rewrite playlist files, finding songs in the library`)
	os.Exit(1)
}

func reorderArgs(args []string, boolFlags map[string]struct{}) ([]string, []string, error) {
	flagArgs := make([]string, 0, len(args))
	positionals := make([]string, 0, 2)

	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			positionals = append(positionals, args[i+1:]...)
			break
		}
		if !strings.HasPrefix(arg, "-") || arg == "-" {
			positionals = append(positionals, arg)
			continue
		}

		flagArgs = append(flagArgs, arg)
		flagName := arg
		if eq := strings.IndexByte(flagName, '='); eq >= 0 {
			flagName = flagName[:eq]
		}
		flagName = strings.TrimLeft(flagName, "-")

		if _, ok := boolFlags[flagName]; ok {
			continue
		}
		if strings.Contains(arg, "=") {
			continue
		}
		if i+1 >= len(args) {
			return nil, nil, fmt.Errorf("flag %s requires a value", arg)
		}
		i++
		flagArgs = append(flagArgs, args[i])
	}

	return flagArgs, positionals, nil
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)
	var (
		optPrint      bool
		optDump       int64
		optDumpTarget string
		optDryRun     bool
		optCopyOnly   bool
		optThreads    int
		optDB         string
		optUpdateHash bool
		optPLRewrite  bool
	)

	fs := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.Usage = usage

	fs.BoolVar(&optPrint, "print", false, "")
	fs.Int64Var(&optDump, "dump", -1, "")
	fs.StringVar(&optDumpTarget, "dump-target", "", "")
	fs.BoolVar(&optDryRun, "dry-run", false, "")
	fs.BoolVar(&optCopyOnly, "copy-only", false, "")
	fs.IntVar(&optThreads, "threads", 0, "")
	fs.IntVar(&optThreads, "j", 0, "")
	fs.StringVar(&optDB, "db", "", "")
	fs.BoolVar(&optUpdateHash, "update-hash", false, "")
	fs.BoolVar(&optPLRewrite, "plrewrite", false, "")

	flagArgs, args, err := reorderArgs(os.Args[1:], map[string]struct{}{
		"print":       {},
		"dry-run":     {},
		"copy-only":   {},
		"update-hash": {},
		"plrewrite":   {},
	})
	if err != nil {
		slog.Error("Failed to parse arguments", "error", err)
		usage()
	}
	if err := fs.Parse(flagArgs); err != nil {
		usage()
	}

	if len(args) < 2 {
		usage()
	}
	source := args[0]
	target := args[1]

	st, err := os.Stat(source)
	if err != nil {
		slog.Error("Failed to read file", "file", source, "error", err)
		os.Exit(1)
	}
	if !st.IsDir() {
		slog.Error("Source is not a directory", "file", source)
		os.Exit(1)
	}
	_ = os.MkdirAll(target, 0o755)

	dbPath := optDB
	if dbPath == "" {
		dbPath = filepath.Join(target, dbFilename)
	}

	myDb := dbNew(dbPath)

	if optPrint {
		myDb.PrintRuns()
		return
	}

	mySyncer, err := newSyncer(source, target, myDb, optDryRun, optCopyOnly, optThreads, optUpdateHash, optPLRewrite)
	if err != nil {
		slog.Error("Failed to create syncer", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		mySyncer.terminateActiveFFmpeg()
	}()

	if optDump >= 0 {
		if optDumpTarget == "" {
			slog.Error("Error: --dump requires --dump-target")
			os.Exit(1)
		}
		if err := mySyncer.dumpRun(ctx, optDump, optDumpTarget); err != nil {
			if err == context.Canceled {
				slog.Error("Canceled (Ctrl-C).")
				os.Exit(130)
			}
			slog.Error("Failed to dump run", "error", err)
			os.Exit(1)
		}
		return
	}

	if !optCopyOnly {
		checkDeps()
	}

	slog.Info("Starting sync", "source", mySyncer.sourceAbsolutePath, "target", mySyncer.targetAbsolutePath)
	slog.Info(fmt.Sprintf("Using %d parallel workers", mySyncer.threads))
	slog.Info("Scanning source directory...")
	files, err := collectFiles(ctx, mySyncer.sourceAbsolutePath)
	if err != nil {
		if err == context.Canceled {
			fmt.Println("\n[!] Canceled (Ctrl-C).")
			os.Exit(130)
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	slog.Info(fmt.Sprintf("Found %d audio files", len(files)))

	if err := mySyncer.processParallel(ctx, files); err != nil {
		if err == context.Canceled {
			slog.Error("Canceled (Ctrl-C).")
			if !mySyncer.dryRun {
				if err := mySyncer.db.Save(); err != nil {
					slog.Error("Failed to save DB", "error", err)
				}
			}
			os.Exit(130)
		}
		slog.Error("Sync interrupted!", "error", err)
		os.Exit(1)
	}

	if mySyncer.plRewrite {
		if err := mySyncer.rewritePlaylists(ctx); err != nil {
			slog.Error("Failed to rewrite playlists", "error", err)
			os.Exit(1)
		}
	}

	current := make(map[string]bool, len(files))
	for _, p := range files {
		rel := relPathUnder(mySyncer.sourceAbsolutePath, p)
		current[pathKey(rel)] = true
	}

	slog.Info("Checking for deleted files...")
	if err := mySyncer.removeDeletedAndOrphans(ctx, current); err != nil {
		if err == context.Canceled {
			slog.Error("Canceled (Ctrl-C).")
			if !mySyncer.dryRun {
				if err := mySyncer.db.Save(); err != nil {
					slog.Error("Failed to save DB", "error", err)
				}
			}
			os.Exit(130)
		}
		slog.Error("Failed to remove deleted and/or orphaned files", "error", err)
		os.Exit(1)
	}

	if !mySyncer.dryRun {
		if err := mySyncer.db.Save(); err != nil {
			slog.Error("Failed to save DB", "error", err)
		}
	}

	slog.Info("==================================================")
	slog.Info("Sync complete!")
	slog.Info(mySyncer.st.String())
	slog.Info("==================================================")

	for k := range mySyncer.db.Data.Files {
		if strings.HasPrefix(k, "x:") {
			slog.Info(fmt.Sprintf("Note: DB contains non-UTF8 paths (stored as hex keys), e.g. %s", pathKeyDecode(k)))
			break
		}
	}
}
