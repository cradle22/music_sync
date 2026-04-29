package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
)

func usage() {
	fmt.Println(`Usage: music_sync <source> <target> [options]

  --print               Print all distinct run IDs and runtimes
  --dump RUNID          Copy all files from a specific run ID to --dump-target
  --dump-target PATH    Target folder for --dump
  --dry-run             Show what would be done without making changes
  --copy-only           Copy all files without transcoding
  --threads|-j N        Number of parallel workers (default: CPU count)
  --db PATH             Database file location (default: target/.music_sync_db.json)
  --update-hash         Updates the hash if the target file exists`)
	os.Exit(1)
}

func main() {
	var (
		optPrint      bool
		optDump       int64
		optDumpTarget string
		optDryRun     bool
		optCopyOnly   bool
		optThreads    int
		optDB         string
		optUpdateHash bool
	)

	flag.BoolVar(&optPrint, "print", false, "")
	flag.Int64Var(&optDump, "dump", -1, "")
	flag.StringVar(&optDumpTarget, "dump-target", "", "")
	flag.BoolVar(&optDryRun, "dry-run", false, "")
	flag.BoolVar(&optCopyOnly, "copy-only", false, "")
	flag.IntVar(&optThreads, "threads", 0, "")
	flag.IntVar(&optThreads, "j", 0, "")
	flag.StringVar(&optDB, "db", "", "")
	flag.BoolVar(&optUpdateHash, "update-hash", false, "")
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if len(args) < 2 {
		usage()
	}
	source := args[0]
	target := args[1]

	st, err := os.Stat(source)
	if err != nil {
		fmt.Printf("Error: Source folder does not exist: %s\n", source)
		os.Exit(1)
	}
	if !st.IsDir() {
		fmt.Printf("Error: Source is not a directory: %s\n", source)
		os.Exit(1)
	}
	_ = os.MkdirAll(target, 0o755)

	dbPath := optDB
	if dbPath == "" {
		dbPath = filepath.Join(target, dbFilename)
	}

	d := dbNew(dbPath)

	if optPrint {
		d.PrintRuns()
		return
	}

	s, err := newSyncer(source, target, d, optDryRun, optCopyOnly, optThreads, optUpdateHash)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		s.terminateActiveFFmpeg()
	}()

	if optDump >= 0 {
		if optDumpTarget == "" {
			fmt.Println("Error: --dump requires --dump-target")
			os.Exit(1)
		}
		if err := s.dumpRun(ctx, optDump, optDumpTarget); err != nil {
			if err == context.Canceled {
				fmt.Println("\n[!] Canceled (Ctrl-C).")
				os.Exit(130)
			}
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}

	if !optCopyOnly {
		s.checkDeps()
	}

	fmt.Printf("Syncing: %s -> %s\n", s.sourceAbs, s.targetAbs)
	fmt.Printf("Using %d parallel workers\n", s.threads)

	fmt.Println("Scanning source directory...")
	files, err := s.collectFiles(ctx)
	if err != nil {
		if err == context.Canceled {
			fmt.Println("\n[!] Canceled (Ctrl-C).")
			os.Exit(130)
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("Found %d audio files\n", len(files))

	if err := s.processParallel(ctx, files); err != nil {
		if err == context.Canceled {
			fmt.Println("\n[!] Canceled (Ctrl-C).")
			if !s.dryRun {
				if err := s.db.Save(); err != nil {
					fmt.Fprintln(os.Stderr, err)
				}
			}
			os.Exit(130)
		}
		fmt.Println("\n==================================================")
		fmt.Println("Sync interrupted!")
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	current := make(map[string]bool, len(files))
	for _, p := range files {
		rel := relPathUnder(s.sourceAbs, p)
		current[pathKey(rel)] = true
	}

	fmt.Println("\nChecking for deleted files...")
	if err := s.removeDeletedAndOrphans(ctx, current); err != nil {
		if err == context.Canceled {
			fmt.Println("\n[!] Canceled (Ctrl-C).")
			if !s.dryRun {
				if err := s.db.Save(); err != nil {
					fmt.Fprintln(os.Stderr, err)
				}
			}
			os.Exit(130)
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if !s.dryRun {
		if err := s.db.Save(); err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
	}

	fmt.Println("\n==================================================")
	fmt.Println("Sync complete!")
	fmt.Print(s.st.String())
	fmt.Println("==================================================")

	for k := range s.db.Data.Files {
		if strings.HasPrefix(k, "x:") {
			fmt.Printf("Note: DB contains non-UTF8 paths (stored as hex keys), e.g. %s\n", pathKeyDecode(k))
			break
		}
	}
}
