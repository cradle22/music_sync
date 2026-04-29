#!/usr/bin/env python3
"""
Music Folder Synchronization Tool
Syncs audio files from source to target, transcoding FLAC/MPC to MP3 while preserving metadata
"""

import os
import sys
import json
import hashlib
import shutil
import argparse
import subprocess
from pathlib import Path
from typing import Dict, Set, Optional, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from datetime import datetime

# Configuration
AUDIO_EXTENSIONS = {'.flac', '.mp3', '.ogg', '.mpc', '.aac', '.m4a'}
COPY_EXTENSIONS = {'.mp3', '.ogg', '.aac', '.m4a'}
TRANSCODE_EXTENSIONS = {'.flac', '.mpc'}
PLAYLIST_EXTENSIONS = {'.m3u', '.m3u8'}
DB_FILENAME = '.music_sync_db.json'

class MusicSyncDB:
    """Manages the sync state database"""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.data = self._load()
        # Capture run info immediately at start
        self._initialize_run()

    def _load(self) -> Dict:
        """Load database from file"""
        if self.db_path.exists():
            try:
                with open(self.db_path, 'r') as f:
                    return json.load(f)
            except json.JSONDecodeError:
                print(f"Warning: Corrupted database file, starting fresh")
        return {'files': {}, 'global_metadata': {'last_runid': 0}}

    def _initialize_run(self):
        """Captures the run timestamp and increments the run ID at script start"""
        if 'global_metadata' not in self.data:
            self.data['global_metadata'] = {'last_runid': 0}

        self.current_runtime = datetime.now().isoformat()
        self.current_runid = self.data['global_metadata'].get('last_runid', 0) + 1

        # Update global metadata
        self.data['global_metadata']['last_runid'] = self.current_runid
        self.data['global_metadata']['last_runtime'] = self.current_runtime
        print(f"--- Starting Sync Run #{self.current_runid} at {self.current_runtime} ---")

    def save(self):
        """Save database to file"""
        with open(self.db_path, 'w') as f:
            json.dump(self.data, f, indent=2)

    def get_file_hash(self, filepath: Path) -> str:
        """Calculate file hash and modification time"""
        stat = filepath.stat()
        # Use mtime and size as a quick hash
        return f"{stat.st_mtime}:{stat.st_size}"

    def is_file_changed(self, source_path: Path, rel_path: str) -> bool:
        """Check if source file has changed since last sync"""
        current_hash = self.get_file_hash(source_path)
        stored_hash = self.data['files'].get(rel_path, {}).get('hash')
        return current_hash != stored_hash

    def update_file(self, rel_path: str, source_path: Path, target_path: Path, rel_target_path: str):
        """Update database entry for a file with the specific run ID and time"""
        self.data['files'][rel_path] = {
            'hash': self.get_file_hash(source_path),
            'target': str(target_path),
            'rel_target': rel_target_path,
            'runid': self.current_runid,    # Specific ID for this file
            'runtime': self.current_runtime # Specific time for this file
        }

    def get_all_synced_files(self) -> Set[str]:
        """Get all files that were previously synced"""
        return set(self.data['files'].keys())

    def remove_file(self, rel_path: str):
        """Remove file entry from database"""
        if rel_path in self.data['files']:
            del self.data['files'][rel_path]

    def print_runs(self):
        """Outputs distinct runid/runtime pairs found in the files list"""
        runs = {}
        for info in self.data.get('files', {}).values():
            rid = info.get('runid')
            time = info.get('runtime')
            if rid is not None:
                runs[rid] = time

        for rid in sorted(runs.keys()):
            print(f"Run ID: {rid} | Timestamp: {runs[rid]}")

    def get_files_by_run(self, runid: int) -> List[Tuple[Path, str]]:
        """Returns list of (absolute_target_path, relative_path) for a specific runid"""
        results = []
        for info in self.data.get('files', {}).values():
            if info.get('runid') == runid:
                # Use rel_target if available, otherwise reconstruct from target path
                rel = info.get('rel_target')
                abs_path = Path(info.get('target'))
                results.append((abs_path, rel))
        return results

class MusicSyncer:
    """Main synchronization class"""

    def __init__(self, source: Path, target: Path, db: MusicSyncDB, dry_run: bool = False, copy_only: bool = False, threads: int = None):
        self.source = source.resolve()
        self.target = target.resolve()
        self.db = db
        self.dry_run = dry_run
        self.copy_only = copy_only
        self.threads = threads if threads else os.cpu_count()
        self.stats = {'copied': 0, 'transcoded': 0, 'skipped': 0, 'deleted': 0}
        self.stats_lock = Lock()  # Thread-safe stats updates

    def dump_run(self, runid: int, dump_path: Path):
        """Copies all files from a specific runid to a new target folder"""
        files_to_dump = self.db.get_files_by_run(runid)
        if not files_to_dump:
            print(f"No files found for Run ID: {runid}")
            return

        print(f"Dumping {len(files_to_dump)} files from Run {runid} to {dump_path}...")
        for abs_src, rel_path in files_to_dump:
            if not abs_src.exists():
                print(f"Warning: Source file {abs_src} missing, skipping.")
                continue

            dest = dump_path / rel_path
            if self.dry_run:
                print(f"[DRY RUN] Would copy {abs_src} -> {dest}")
            else:
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(abs_src, dest)
                print(f"Dumped: {rel_path}")
    def check_dependencies(self):
        """Check if required tools are installed"""
        required = ['ffmpeg']
        missing = []

        for tool in required:
            if shutil.which(tool) is None:
                missing.append(tool)

        if missing:
            print(f"Error: Missing required tools: {', '.join(missing)}")
            print("Please install: sudo apt-get install ffmpeg")
            sys.exit(1)

    def get_target_path(self, source_path: Path, ext: str = None) -> Path:
        """Calculate target path for a source file"""
        rel_path = source_path.relative_to(self.source)
        target_path = self.target / rel_path

        # Change extension if transcoding
        if ext:
            target_path = target_path.with_suffix(ext)

        return target_path

    def copy_file(self, source_path: Path, target_path: Path):
        """Copy file preserving metadata"""
        if self.dry_run:
            print(f"[DRY RUN] Would copy: {source_path} -> {target_path}")
            return

        target_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, target_path)
        print(f"Copied: {source_path.name}")
        with self.stats_lock:
            self.stats['copied'] += 1

    def handle_playlist(self, source_file: Path, target_file: Path):
        """Recreate a playlist with rewritten relative paths"""
        if self.dry_run:
            print(f"[DRY RUN] Would rewrite playlist from: {source_path} -> {target_path}")
            return


    def transcode_to_mp3(self, source_path: Path, target_path: Path):
        """Transcode FLAC/MPC to MP3 with metadata preservation"""
        if self.dry_run:
            print(f"[DRY RUN] Would transcode: {source_path} -> {target_path}")
            return

        target_path.parent.mkdir(parents=True, exist_ok=True)

        # Build ffmpeg command
        cmd = [
            'ffmpeg',
            '-i', str(source_path),
            '-c:a', 'libmp3lame',
            '-q:a', '0',  # Highest quality VBR (V0)
            '-map', '0:a',  # Map audio
            '-map', '0:v?',  # Map cover art if present
            '-id3v2_version', '3',
            '-write_id3v1', '1',
            '-y',  # Overwrite
        ]

        # Check for ReplayGain tags
        rg_info = self._get_replaygain_info(source_path)
        if rg_info:
            cmd.extend(['-af', f'volume={rg_info}dB'])
            print(f"Applying ReplayGain: {rg_info}dB to {source_path.name}")

        cmd.append(str(target_path))

        try:
            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True
            )
            print(f"Transcoded: {source_path.name} -> {target_path.name}")
            with self.stats_lock:
                self.stats['transcoded'] += 1
        except subprocess.CalledProcessError as e:
            print(f"Error transcoding {source_path}: {e.stderr.decode()}")
            raise

    def _get_replaygain_info(self, source_path: Path) -> Optional[float]:
        """Extract ReplayGain information from file"""
        try:
            cmd = [
                'ffprobe',
                '-v', 'quiet',
                '-show_entries', 'format_tags=replaygain_track_gain',
                '-of', 'default=noprint_wrappers=1:nokey=1',
                str(source_path)
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)

            if result.stdout.strip():
                # Parse value like "-7.89 dB"
                gain_str = result.stdout.strip().split()[0]
                return float(gain_str)
        except (subprocess.CalledProcessError, ValueError, IndexError):
            pass

        return None

    def sync_file(self, source_path: Path):
        """Sync a single file"""
        rel_path = str(source_path.relative_to(self.source))
        ext = source_path.suffix.lower()
        skip = False

        # Check if file has changed
        if not self.db.is_file_changed(source_path, rel_path):
            skip = True
            with self.stats_lock:
                self.stats['skipped'] += 1
            #return

        if self.copy_only or ext in COPY_EXTENSIONS:
            target_path = self.get_target_path(source_path)
        elif ext in TRANSCODE_EXTENSIONS:
            target_path = self.get_target_path(source_path, '.mp3')
        elif ext in PLAYLIST_EXTENSIONS:
            target_path = self.get_target_path(source_path)
        rel_target_path = str(target_path.relative_to(self.target))



        # In copy-only mode, copy everything
        if not skip:
            if self.copy_only or ext in COPY_EXTENSIONS:
                # Simple copy
                self.copy_file(source_path, target_path)
                self.db.update_file(rel_path, source_path, target_path, rel_target_path)

            elif ext in TRANSCODE_EXTENSIONS:
                # Transcode to MP3
                self.transcode_to_mp3(source_path, target_path)
                self.db.update_file(rel_path, source_path, target_path, rel_target_path)
            elif ext in PLAYLIST_EXTENSIONS:
                # recreate PLAYLIST_EXTENSIONS
                self.handle_playlist(source_path, target_path)

    def process_files_parallel(self, source_files: List[str]):
        """Process files in parallel using thread pool"""
        file_paths = [self.source / rel_path for rel_path in source_files]

        # Separate files that need processing from those that are unchanged
        print("Compiling list of files to process")
        files_to_process = []
        for source_path in file_paths:
            rel_path = str(source_path.relative_to(self.source))
            if self.db.is_file_changed(source_path, rel_path):
                files_to_process.append(source_path)
            else:
                with self.stats_lock:
                    self.stats['skipped'] += 1


        if not files_to_process:
            print("No files need processing")
            return

        print(f"Processing {len(files_to_process)} files using {self.threads} threads...")

        # Process files in parallel
        with ThreadPoolExecutor(max_workers=self.threads) as executor:
            future_to_path = {executor.submit(self.sync_file, path): path for path in files_to_process}
            try:
                # as_completed yields futures as they finish
                for future in as_completed(future_to_path):
                    path = future_to_path[future]
                    try:
                        # Catch exceptions for individual files (e.g., Network I/O errors)
                        future.result()
                    except Exception as e:
                        print(f"Error processing {path.name}: {e}")

            except KeyboardInterrupt:
                # Catch the Ctrl-C for the entire batch
                print("\n[!] Ctrl-C detected. Canceling all pending tasks...")
                # wait=False stops the executor from waiting for current threads to finish
                # cancel_futures=True removes all items from the queue immediately
                executor.shutdown(wait=False, cancel_futures=True)
                # Re-raise so the sync() method's 'finally' block can save the database state
                raise

    def collect_source_files(self) -> Set[str]:
        """Collect all audio files from source"""
        source_files = set()

        for root, dirs, files in os.walk(self.source):
            root_path = Path(root)
            for filename in files:
                filepath = root_path / filename
                if filepath.suffix.lower() in AUDIO_EXTENSIONS:
                    rel_path = str(filepath.relative_to(self.source))
                    source_files.add(rel_path)

        return source_files

    def remove_deleted_files(self, current_files: Set[str]):
        """Remove files from target that no longer exist in source"""
        synced_files = self.db.get_all_synced_files()
        deleted_files = synced_files - current_files

        for rel_path in deleted_files:
            file_info = self.db.data['files'].get(rel_path, {})
            target_path = file_info.get('target')
            rel_target_path = file_info.get('rel_target')
            deleted = False

            if target_path:
                target_path = Path(target_path)
                if target_path.exists():
                    deleted = True
                    if self.dry_run:
                        print(f"[DRY RUN] Would delete: {target_path}")
                    else:
                        target_path.unlink()
                        print(f"Deleted: {target_path}")
                        with self.stats_lock:
                            self.stats['deleted'] += 1
            if not deleted and rel_target_path:
                target_path = Path(self.target) / rel_target_path
                if target_path.exists():
                    deleted = True
                    if self.dry_run:
                        print(f"[DRY RUN] Would delete: {target_path}")
                    else:
                        target_path.unlink()
                        print(f"Deleted: {target_path}")
                        with self.stats_lock:
                            self.stats['deleted'] += 1

            if deleted:
                self.db.remove_file(rel_path)

    def sync(self):
        """Main sync operation"""
        print(f"Syncing: {self.source} -> {self.target}")
        print(f"Using {self.threads} parallel threads")

        # Collect all source files
        try:
            print("Scanning source directory...")
            source_files = self.collect_source_files()
            print(f"Found {len(source_files)} audio files")

            # Sync files in parallel
            self.process_files_parallel(sorted(source_files))

            # Remove deleted files
            print("\nChecking for deleted files...")
            self.remove_deleted_files(source_files)
        except KeyboardInterrupt:
            print("\n" + "="*50)
            print("Sync interrupted!")
        finally:
            # Save database
            if not self.dry_run:
                self.db.save()
            # Print statistics
            print("\n" + "="*50)
            print("Sync complete!")
            print(f"  Copied: {self.stats['copied']}")
            print(f"  Transcoded: {self.stats['transcoded']}")
            print(f"  Skipped (unchanged): {self.stats['skipped']}")
            print(f"  Deleted: {self.stats['deleted']}")
            print("="*50)

def main():
    parser = argparse.ArgumentParser(
        description='Synchronize music folders with transcoding support'
    )
    parser.add_argument('source', type=Path, help='Source music folder')
    parser.add_argument('target', type=Path, help='Target music folder')
    parser.add_argument('--print', action='store_true', help='Print all distinct run IDs and runtimes')
    parser.add_argument('--dump', type=int, metavar='RUNID', help='Copy all files from a specific run ID to dump-target')
    parser.add_argument('--dump-target', type=Path, help='Target folder for the --dump operation')
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be done without making changes')
    parser.add_argument('--copy-only', action='store_true',
                       help='Copy all files without transcoding (keeps original formats)')
    parser.add_argument('--threads', '-j', type=int, default=None,
                       help=f'Number of parallel threads (default: CPU count = {os.cpu_count()})')
    parser.add_argument('--db', type=Path,
                       help='Database file location (default: target/.music_sync_db.json)')

    args = parser.parse_args()

    # Validate paths
    if not args.source.exists():
        print(f"Error: Source folder does not exist: {args.source}")
        sys.exit(1)

    if not args.source.is_dir():
        print(f"Error: Source is not a directory: {args.source}")
        sys.exit(1)

    # Create target if it doesn't exist
    args.target.mkdir(parents=True, exist_ok=True)

    # Database location
    db_path = args.db if args.db else (args.target / DB_FILENAME)

    # Initialize and run sync
    db = MusicSyncDB(db_path)

    if args.print:
        db.print_runs()
        sys.exit(0)

    syncer = MusicSyncer(args.source, args.target, db, args.dry_run, args.copy_only, args.threads)

    if args.dump is not None:
        if not args.dump_target:
            print("Error: --dump requires --dump-target")
            sys.exit(1)
        syncer.dump_run(args.dump, args.dump_target)
        sys.exit(0)

    if not args.copy_only:
        syncer.check_dependencies()
    syncer.sync()

if __name__ == '__main__':
    main()
