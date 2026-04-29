#!/usr/bin/env perl
# Music Folder Synchronization Tool
# Syncs audio files from source to target, transcoding FLAC/MPC to MP3
# while preserving metadata.
#
# Translated from Python to Perl 5.

use strict;
use warnings;
use 5.010;

use File::Find      qw(find);
use File::Path      qw(make_path);
use File::Copy      qw(copy);
use File::Basename  qw(basename dirname);
use File::Spec;
use Cwd             qw(abs_path);
use JSON::PP;
use POSIX           qw(strftime);
use Getopt::Long    qw(:config no_auto_abbrev);
use Encode          qw(encode encode_utf8 decode FB_CROAK);
use threads;
use threads::shared;
use Thread::Queue;

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
my %AUDIO_EXTENSIONS     = map { $_ => 1 } qw(.flac .mp3 .ogg .mpc .aac .m4a .m3u .m3u8);
my %COPY_EXTENSIONS      = map { $_ => 1 } qw(.mp3 .ogg .aac .m4a);
my %TRANSCODE_EXTENSIONS = map { $_ => 1 } qw(.flac .mpc);
my %PLAYLIST_EXTENSIONS  = map { $_ => 1 } qw(.m3u .m3u8);
my $DB_FILENAME          = '.music_sync_db.json';

# ---------------------------------------------------------------------------
# Thread-safe statistics
# ---------------------------------------------------------------------------
my %stats : shared = (copied => 0, transcoded => 0, skipped => 0, deleted => 0);
my %active_ffmpeg_pids : shared;

sub inc_stat {
    my ($key) = @_;
    lock(%stats);
    $stats{$key}++;
}

sub register_ffmpeg_pid {
    my ($pid) = @_;
    return unless defined $pid && $pid > 0;
    lock(%active_ffmpeg_pids);
    $active_ffmpeg_pids{$pid} = 1;
}

sub unregister_ffmpeg_pid {
    my ($pid) = @_;
    return unless defined $pid && $pid > 0;
    lock(%active_ffmpeg_pids);
    delete $active_ffmpeg_pids{$pid};
}

sub terminate_active_ffmpeg {
    my @pids;
    {
        lock(%active_ffmpeg_pids);
        @pids = keys %active_ffmpeg_pids;
    }
    return unless @pids;

    # Ask ffmpeg processes to stop gracefully first, then force if needed.
    kill 'INT', @pids;
    kill 'TERM', @pids;
}

# ---------------------------------------------------------------------------
# Helper: relative path of $path under $base (both absolute strings)
# ---------------------------------------------------------------------------
sub rel_path {
    my ($base, $path) = @_;
    $base =~ s{/?$}{/};
    (my $rel = $path) =~ s{^\Q$base\E}{};
    return $rel;
}

# ---------------------------------------------------------------------------
# Helper: replace/add extension
# ---------------------------------------------------------------------------
sub with_suffix {
    my ($path, $new_ext) = @_;
    $path =~ s/\.[^.\/]+$//;
    return $path . $new_ext;
}

# ---------------------------------------------------------------------------
# Helper: make parent directories for a file path
# ---------------------------------------------------------------------------
sub mkdir_p_for {
    my ($path) = @_;
    my $dir = -d $path ? $path : dirname($path);
    make_path($dir) unless -d $dir;
}

# ---------------------------------------------------------------------------
# Helper: check for an executable in PATH
# ---------------------------------------------------------------------------
sub has_tool {
    my ($cmd) = @_;
    for my $dir (split /:/, $ENV{PATH} // '') {
        return 1 if -x File::Spec->catfile($dir, $cmd);
    }
    return 0;
}

# ---------------------------------------------------------------------------
# Helper: number of CPUs
# ---------------------------------------------------------------------------
sub cpu_count {
    my $n = `nproc 2>/dev/null`;
    chomp $n;
    return ($n && $n =~ /^\d+$/) ? int($n) : 4;
}

# ---------------------------------------------------------------------------
# Helper: decode UTF-8 filesystem bytes to Perl text when possible
# ---------------------------------------------------------------------------
sub fs_path_to_text {
    my ($path) = @_;
    return '' if !defined $path;

    if (utf8::is_utf8($path)) {
        # The UTF-8 flag may have been set by Perl's thread machinery on raw
        # bytes without actually decoding them (a known Perl threads quirk).
        # Detect this by: re-encoding as Latin-1 (gets back the raw bytes if
        # codepoints are all <= 0xFF) then decoding those as UTF-8.  If that
        # round-trip succeeds it means the string was wrongly-upgraded bytes.
        # For a correctly decoded Unicode char (e.g., ö as U+00F6), its Latin-1
        # byte is 0xF6, which is NOT a valid single UTF-8 byte, so the decode
        # fails and we return the original string unchanged.
        if ($path =~ /[^\x00-\x7F]/) {
            my $bytes = eval { encode('iso-8859-1', $path, FB_CROAK) };
            if (!$@ && defined $bytes) {
                my $fixed = eval { decode('UTF-8', $bytes, FB_CROAK) };
                return $fixed if !$@ && defined $fixed;
            }
        }
        return $path;
    }

    # Plain byte string: decode as UTF-8 (lenient — replace bad sequences).
    return decode('UTF-8', $path, 0);
}

# Build multiple comparable representations for one path so text-vs-bytes
# differences do not cause false orphan deletions.
sub fs_path_variants {
    my ($path) = @_;
    return () if !defined $path || $path eq '';

    my %variants;
    $variants{$path} = 1;

    my $text = fs_path_to_text($path);
    $variants{$text} = 1 if defined $text;

    my $utf8_bytes = eval { encode_utf8($text) };
    $variants{$utf8_bytes} = 1 if defined $utf8_bytes;

    return keys %variants;
}

# ===========================================================================
# DB functions  (operate on a plain hashref  $db)
# ===========================================================================

sub db_load {
    my ($db_path) = @_;
    if (-f $db_path) {
        open my $fh, '<:raw', $db_path
            or do { warn "Warning: Cannot open database file: $!\n";
                    return db_empty(); };
        local $/;
        my $raw = <$fh>;
        close $fh;
        # ->utf8 mode: reads raw UTF-8 bytes and returns Perl Unicode strings
        # (UTF-8 flag set for non-ASCII), so DB keys are proper Unicode.
        my $data = eval { JSON::PP->new->utf8->decode($raw) };
        if ($@) {
            warn "Warning: Corrupted database file, starting fresh\n";
            return db_empty();
        }
        return $data;
    }
    return db_empty();
}

sub db_empty {
    return { files => {}, global_metadata => { last_runid => 0 } };
}

sub db_new {
    my ($db_path) = @_;
    my $db = {
        db_path => $db_path,
        data    => db_load($db_path),
    };
    db_initialize_run($db);
    return $db;
}

sub db_initialize_run {
    my ($db) = @_;
    $db->{data}{global_metadata} //= { last_runid => 0 };

    $db->{current_runtime} = strftime('%Y-%m-%dT%H:%M:%S', localtime);
    $db->{current_runid}   = ($db->{data}{global_metadata}{last_runid} // 0) + 1;

    $db->{data}{global_metadata}{last_runid}   = $db->{current_runid};
    $db->{data}{global_metadata}{last_runtime} = $db->{current_runtime};

    print "--- Starting Sync Run #$db->{current_runid} at $db->{current_runtime} ---\n";
}

sub db_save {
    my ($db) = @_;
    open my $fh, '>:raw', $db->{db_path}
        or die "Cannot write database: $!\n";
    # ->utf8 mode: handles UTF-8 encoding at the JSON level; Unicode strings
    # (UTF-8 flag set) are written as proper UTF-8 bytes to the raw filehandle.
    print $fh JSON::PP->new->utf8->pretty->canonical->encode($db->{data});
    close $fh;
    print "Database saved to $db->{db_path}\n";
}

sub db_file_hash {
    my ($filepath) = @_;
    my @st = stat($filepath) or return '';
    return sprintf("%d:%d", $st[9], $st[7]);
}

sub db_is_file_changed {
    my ($db, $source_path, $rel_path) = @_;
    $rel_path = fs_path_to_text($rel_path);
    my $current = db_file_hash($source_path);
    my $stored  = ($db->{data}{files}{$rel_path} // {})->{hash} // '';
    $stored =~ s/^(\d+)\.0(:)/$1$2/;
    return $current ne $stored;
}

sub db_update_file {
    my ($db, $rel_path, $source_path, $target_path, $rel_target_path) = @_;
    $rel_path        = fs_path_to_text($rel_path);
    $source_path     = fs_path_to_text($source_path);
    $target_path     = fs_path_to_text($target_path);
    $rel_target_path = fs_path_to_text($rel_target_path);

    my $currentHash = db_file_hash($source_path);
    $currentHash =~ s/^(\d+)\.0(:)/$1$2/;
    $db->{data}{files}{$rel_path} = {
        hash       => $currentHash,
        target     => $target_path,
        rel_target => $rel_target_path,
        runid      => $db->{current_runid},
        runtime    => $db->{current_runtime},
    };
}

sub db_get_all_synced_files {
    my ($db) = @_;
    return keys %{ $db->{data}{files} };
}

sub db_remove_file {
    my ($db, $rel_path) = @_;
    $rel_path = fs_path_to_text($rel_path);
    delete $db->{data}{files}{$rel_path};
}

sub db_print_runs {
    my ($db) = @_;
    my %runs;
    for my $info (values %{ $db->{data}{files} // {} }) {
        my $rid  = $info->{runid};
        my $time = $info->{runtime};
        $runs{$rid} = $time if defined $rid;
    }
    for my $rid (sort { $a <=> $b } keys %runs) {
        print "Run ID: $rid | Timestamp: $runs{$rid}\n";
    }
}

sub db_get_files_by_run {
    my ($db, $runid) = @_;
    my @results;
    for my $info (values %{ $db->{data}{files} // {} }) {
        if (defined $info->{runid} && $info->{runid} == $runid) {
            push @results, [ $info->{target}, $info->{rel_target} ];
        }
    }
    return @results;
}

# ===========================================================================
# Syncer functions  (operate on a plain hashref  $s)
# ===========================================================================

sub syncer_new {
    my (%args) = @_;
    return {
        source    => abs_path($args{source}),
        target    => abs_path($args{target}),
        db        => $args{db},
        dry_run   => $args{dry_run}   // 0,
        copy_only => $args{copy_only} // 0,
        threads   => $args{threads}   // cpu_count(),
        update_hash => $args{update_hash} // 0,
    };
}

sub syncer_get_target_path {
    my ($s, $source_path, $new_ext) = @_;
    my $rel = rel_path($s->{source}, $source_path);
    my $tgt = File::Spec->catfile($s->{target}, $rel);
    $tgt = with_suffix($tgt, $new_ext) if defined $new_ext;
    return $tgt;
}

sub syncer_check_dependencies {
    my ($s) = @_;
    my @missing = grep { !has_tool($_) } qw(ffmpeg);
    if (@missing) {
        print "Error: Missing required tools: " . join(', ', @missing) . "\n";
        print "Please install: sudo dnf install ffmpeg\n";
        exit 1;
    }
}

sub syncer_copy_file {
    my ($s, $source_path, $target_path) = @_;
    if ($s->{dry_run}) {
        print "[DRY RUN] Would copy: $source_path -> $target_path\n";
        return;
    }
    mkdir_p_for($target_path);
    copy($source_path, $target_path)
        or die "Copy failed $source_path -> $target_path: $!\n";
    print "Copied: " . basename($source_path) . "\n";
    inc_stat('copied');
}

sub syncer_handle_playlist {
  my ($s, $source_file, $target_file) = @_;

  if ($s->{dry_run}) {
    print "[DRY RUN] Would handle playlist: $source_file -> $target_file\n";
    return;
  }

    # Rebase absolute entries from the sync source root to the sync target root.
    my $source_root = $s->{source};
    my $target_root = $s->{target};
    my $target_dir  = dirname($target_file);

    # Read/write as raw bytes so legacy playlist encodings (cp1252/latin1, etc.)
    # do not crash decoding. We only transform ASCII-safe path characters.
    open(my $in, "<:raw", $source_file) or die "Cannot open source: $!";
    mkdir_p_for($target_file);
    open(my $out, ">:raw", $target_file) or die "Cannot open target: $!";

    my $line_no = 0;
  while (my $line = <$in>) {
        $line_no++;

        # Strip UTF-8 BOM if present on the first line.
        $line =~ s/^\xEF\xBB\xBF// if $line_no == 1;

        # Normalize line endings for processing.
        $line =~ s/\r?\n\z//;

    # 1. Discard lines starting with #
    next if $line =~ /^#/;

    next if $line eq '';

    # 2. Replace Windows separators with Linux ones
    $line =~ s/\\/\//g;

    my ($ext) = ($line =~ /(\.[^.\/]+)$/);
    $ext = lc($ext // '');

        # 3. Handle path rewriting
    if (File::Spec->file_name_is_absolute($line)) {
            my $relative_part = rel_path($source_root, $line);
            if ($relative_part ne $line) {
                my $rebased_abs = File::Spec->catfile($target_root, $relative_part);
                $line = File::Spec->abs2rel($rebased_abs, $target_dir);
                $line =~ s{\\}{/}g;
      }
    }
    # If relative, we leave it as is (already handled by the \ to / swap)

    # handle possible transcoding
    if($TRANSCODE_EXTENSIONS{$ext} && ! $s->{copy_only}) {
            $line = with_suffix($line, ".mp3");
    }
    print $out "$line\n";
  }
  close($in);
  close($out);
}

sub syncer_get_replaygain {
    my ($source_path) = @_;
    my $quoted = $source_path;
    $quoted =~ s/'/'\\''/g;
    my $out = `ffprobe -v quiet -show_entries format_tags=replaygain_track_gain -of default=noprint_wrappers=1:nokey=1 '$quoted' 2>/dev/null`;
    chomp $out;
    return undef unless $out =~ /\S/;
    my ($gain) = split /\s+/, $out;
    return undef unless defined $gain && $gain =~ /^-?\d+(\.\d+)?$/;
    return $gain + 0;
}

sub syncer_transcode_to_mp3 {
    my ($s, $source_path, $target_path) = @_;
    if ($s->{dry_run}) {
        print "[DRY RUN] Would transcode: $source_path -> $target_path\n";
        return;
    }
    mkdir_p_for($target_path);

    my @cmd = (
        'ffmpeg',
        '-i',             $source_path,
        '-c:a',           'libmp3lame',
        '-q:a',           '0',
        '-map',           '0:a',
        '-map',           '0:v?',
        '-id3v2_version', '3',
        '-write_id3v1',   '1',
        '-y',
    );

    my $rg = syncer_get_replaygain($source_path);
    if (defined $rg) {
        push @cmd, ('-af', "volume=${rg}dB");
        print "Applying ReplayGain: ${rg}dB to " . basename($source_path) . "\n";
    }

    push @cmd, $target_path;

    my $stderr_output = '';
    my $pid = open my $fh, '-|', @cmd
        or die "Cannot run ffmpeg: $!\n";

    register_ffmpeg_pid($pid);
    while (<$fh>) { $stderr_output .= $_; }
    close $fh;
    unregister_ffmpeg_pid($pid);

    if ($? != 0) {
        die "Error transcoding $source_path:\n$stderr_output\n";
    }
    print "Transcoded: " . basename($source_path) . " -> " . basename($target_path) . "\n";
    inc_stat('transcoded');
}

sub syncer_sync_file {
    my ($s, $source_path) = @_;
    my $rel_path = rel_path($s->{source}, $source_path);

    my ($ext) = ($source_path =~ /(\.[^.\/]+)$/);
    my $target_path;
    $ext = lc($ext // '');

    if ($PLAYLIST_EXTENSIONS{$ext}) {
        $target_path = syncer_get_target_path($s, $source_path);
    } elsif ($s->{copy_only} || $COPY_EXTENSIONS{$ext}) {
        $target_path = syncer_get_target_path($s, $source_path);
    } elsif ($TRANSCODE_EXTENSIONS{$ext}) {
        $target_path = syncer_get_target_path($s, $source_path, '.mp3');
    } else {
        return;
    }
    my $rel_target = rel_path($s->{target}, $target_path);

    if($s->{update_hash} && -f $target_path) {
        print "Updating hash for: $target_path\n";
        return {
            rel_path   => $rel_path,
            source_path => $source_path,
            target_path => $target_path,
            rel_target  => $rel_target,
        };
    }

    if (!-f $target_path) {
        print "Target missing, reprocessing: $target_path\n";
    } elsif (!db_is_file_changed($s->{db}, $source_path, $rel_path)) {
        inc_stat('skipped');
        return undef;
    }

    if ($PLAYLIST_EXTENSIONS{$ext}) {
        syncer_handle_playlist($s, $source_path, $target_path);
        return {
            rel_path   => $rel_path,
            source_path => $source_path,
            target_path => $target_path,
            rel_target  => $rel_target,
        };
    } elsif ($s->{copy_only} || $COPY_EXTENSIONS{$ext}) {
        syncer_copy_file($s, $source_path, $target_path);
        return {
            rel_path   => $rel_path,
            source_path => $source_path,
            target_path => $target_path,
            rel_target  => $rel_target,
        };
    } elsif ($TRANSCODE_EXTENSIONS{$ext}) {
        syncer_transcode_to_mp3($s, $source_path, $target_path);
        return {
            rel_path   => $rel_path,
            source_path => $source_path,
            target_path => $target_path,
            rel_target  => $rel_target,
        };
    }

    return undef;
}

sub syncer_collect_files {
    my ($s) = @_;
    my @files;
    find(sub {
        return if -d $_;
        my ($ext) = ($_ =~ /(\.[^.\/]+)$/);
        $ext = lc($ext // '');
        push @files, $File::Find::name if $AUDIO_EXTENSIONS{$ext};
    }, $s->{source});
    return @files;
}

sub syncer_process_files_parallel {
    my ($s, @source_files) = @_;

    print "Compiling list of files to process\n";
    my @to_process;
    for my $path (@source_files) {
        if ($s->{update_hash}) {
            push @to_process, $path;
            next;
        }

        my ($ext) = ($path =~ /(\.[^.\/]+)$/);
        my $target_path;
        $ext = lc($ext // '');

        if ($PLAYLIST_EXTENSIONS{$ext}) {
            $target_path = syncer_get_target_path($s, $path);
        } elsif ($s->{copy_only} || $COPY_EXTENSIONS{$ext}) {
            $target_path = syncer_get_target_path($s, $path);
        } elsif ($TRANSCODE_EXTENSIONS{$ext}) {
            $target_path = syncer_get_target_path($s, $path, '.mp3');
        }

        my $rel = rel_path($s->{source}, $path);
        if (!defined $target_path || !-f $target_path || db_is_file_changed($s->{db}, $path, $rel)) {
            push @to_process, $path;
        } else {
            inc_stat('skipped');
        }
    }

    unless (@to_process) {
        print "No files need processing\n";
        return;
    }

    my $n = $s->{threads};
    print "Processing " . scalar(@to_process) . " files using $n threads...\n";

    my $queue = Thread::Queue->new();
    my $update_queue = Thread::Queue->new();
    $queue->enqueue($_) for @to_process;
    $queue->end();

    my $abort : shared = 0;
    my $interrupt_seen : shared = 0;

    local $SIG{INT} = sub {
        {
            lock($abort);
            $abort = 1;
        }
        terminate_active_ffmpeg();
        {
            lock($interrupt_seen);
            if (!$interrupt_seen) {
                print "\n[!] Ctrl-C detected. Canceling all pending tasks...\n";
                $interrupt_seen = 1;
            }
        }
    };

    my @workers;
    for (1 .. $n) {
        push @workers, threads->create(sub {
            local $SIG{INT} = sub {
                lock($abort);
                $abort = 1;
                terminate_active_ffmpeg();
                die "Interrupted\n";
            };

            while (defined(my $path = $queue->dequeue_nb())) {
                { lock($abort); last if $abort; }
                eval {
                    my $update = syncer_sync_file($s, $path);
                    $update_queue->enqueue($update) if $update;
                };
                if ($@) {
                    if ($@ =~ /^Interrupted/) {
                        lock($abort);
                        $abort = 1;
                        last;
                    }
                    print "Error processing " . basename($path) . ": $@\n";
                }
            }
        });
    }
    $_->join() for @workers;

    $update_queue->end();
    while (defined(my $update = $update_queue->dequeue_nb())) {
        db_update_file(
            $s->{db},
            $update->{rel_path},
            $update->{source_path},
            $update->{target_path},
            $update->{rel_target},
        );
    }

    {
        lock($abort);
        die "Interrupted\n" if $abort;
    }
}

sub syncer_remove_deleted_files {
    my ($s, %current_files) = @_;
    my @synced = db_get_all_synced_files($s->{db});
    print "Looking for deleted files\n";

    for my $rel_path (@synced) {
        next if exists $current_files{$rel_path};

        my $info            = $s->{db}{data}{files}{$rel_path} // {};
        my $target_abs      = $info->{target}     // '';
        my $rel_target_path = $info->{rel_target} // '';
        my $target_root     = $s->{target};
        $target_root =~ s{/?$}{/};
        my $deleted = 0;

        # Prefer deleting via rel_target mapped to the *current* target root.
        if ($rel_target_path) {
            my $current_target = File::Spec->catfile($s->{target}, $rel_target_path);
            if (-f $current_target) {
                $deleted = 1;
                if ($s->{dry_run}) {
                    print "[DRY RUN] Would delete: $current_target\n";
                } else {
                    unlink $current_target or warn "Cannot delete $current_target: $!\n";
                    print "Deleted: $current_target\n";
                    inc_stat('deleted');
                }
            }
        }

        # Legacy fallback: only use absolute DB path if it is still under
        # the currently selected target root.
        if (!$deleted && $target_abs && -f $target_abs && index($target_abs, $target_root) == 0) {
            $deleted = 1;
            if ($s->{dry_run}) {
                print "[DRY RUN] Would delete: $target_abs\n";
            } else {
                unlink $target_abs or warn "Cannot delete $target_abs: $!\n";
                print "Deleted: $target_abs\n";
                inc_stat('deleted');
            }
        }

        # Always remove the DB entry: source is gone, regardless of whether
        # the target was found and deleted or was already missing.
        db_remove_file($s->{db}, $rel_path);
    }

    # Second pass: remove any target file not tracked by the DB.
    # DB paths are Unicode strings (JSON loaded with :utf8); filesystem paths
    # from find are raw UTF-8 bytes.  Encode DB paths back to UTF-8 bytes so
    # the comparison works correctly for filenames with umlauts, etc.
    print "Looking for orphans\n";
    # Compare multiple representations (text/bytes) to avoid false mismatches
    # with non-ASCII filenames when Perl mixes internal string flags.
    my $db_abs = File::Spec->rel2abs($s->{db}{db_path});
    my %db_path_variants = map { $_ => 1 } fs_path_variants($db_abs);
    my %known_targets;
    for my $info (values %{ $s->{db}{data}{files} // {} }) {
        my $target_abs = $info->{target} // '';
        if ($target_abs ne '') {
            my $norm = File::Spec->rel2abs($target_abs);
            $known_targets{$_} = 1 for fs_path_variants($norm);
        }

        my $rel_target = $info->{rel_target} // '';
        if ($rel_target ne '') {
            my $candidate = File::Spec->catfile($s->{target}, $rel_target);
            my $norm = File::Spec->rel2abs($candidate);
            $known_targets{$_} = 1 for fs_path_variants($norm);
        }
    }

    my @orphan_files;
    find(
        {
            no_chdir => 1,
            wanted   => sub {
                return if -d $_;

                my $path = $File::Find::name;
                my $norm = File::Spec->rel2abs($path);
                my @variants = fs_path_variants($norm);

                for my $v (@variants) {
                    return if $db_path_variants{$v};
                    return if $known_targets{$v};
                }

                push @orphan_files, $path;
            },
        },
        $s->{target}
    );

    for my $path (@orphan_files) {
        if ($s->{dry_run}) {
            print "[DRY RUN] Would delete orphan target file: $path\n";
            next;
        }
        unlink $path or warn "Cannot delete orphan target file $path: $!\n";
        if (!-e $path) {
            print "Deleted orphan target file: $path\n";
            inc_stat('deleted');
        }
    }

    # Prune empty directories bottom-up, but keep the target root itself.
    return if $s->{dry_run};

    find(
        {
            no_chdir => 1,
            bydepth  => 1,
            wanted   => sub {
                return unless -d $_;

                my $dir = $File::Find::name;
                return if $dir eq $s->{target};

                opendir my $dh, $dir or return;
                my @entries = grep { $_ ne '.' && $_ ne '..' } readdir $dh;
                closedir $dh;

                return if @entries;

                if (rmdir $dir) {
                    print "Removed empty directory: $dir\n";
                }
            },
        },
        $s->{target}
    );
}

sub syncer_dump_run {
    my ($s, $runid, $dump_path) = @_;
    my @files = db_get_files_by_run($s->{db}, $runid);
    unless (@files) {
        print "No files found for Run ID: $runid\n";
        return;
    }
    print "Dumping " . scalar(@files) . " files from Run $runid to $dump_path...\n";
    for my $entry (@files) {
        my ($abs_src, $rel_path) = @$entry;
        unless (-f $abs_src) {
            print "Warning: Source file $abs_src missing, skipping.\n";
            next;
        }
        my $dest = File::Spec->catfile($dump_path, $rel_path);
        if ($s->{dry_run}) {
            print "[DRY RUN] Would copy $abs_src -> $dest\n";
        } else {
            mkdir_p_for($dest);
            copy($abs_src, $dest) or warn "Copy failed: $!\n";
            print "Dumped: $rel_path\n";
        }
    }
}

sub syncer_sync {
    my ($s) = @_;
    print "Syncing: $s->{source} -> $s->{target}\n";
    print "Using $s->{threads} parallel threads\n";

    my @source_files;
    eval {
        print "Scanning source directory...\n";
        @source_files = syncer_collect_files($s);
        print "Found " . scalar(@source_files) . " audio files\n";

        syncer_process_files_parallel($s, sort @source_files);

        print "\nChecking for deleted files...\n";
        # Normalize to Unicode text so keys match those stored in the DB.
        my %src_set = map { fs_path_to_text(rel_path($s->{source}, $_)) => 1 } @source_files;
        syncer_remove_deleted_files($s, %src_set);
    };
    if ($@ && $@ !~ /^Interrupted/) {
        print "\n" . ("=" x 50) . "\nSync interrupted!\n";
    }

    db_save($s->{db}) unless $s->{dry_run};

    print "\n" . ("=" x 50) . "\n";
    print "Sync complete!\n";
    print "  Copied:              $stats{copied}\n";
    print "  Transcoded:          $stats{transcoded}\n";
    print "  Skipped (unchanged): $stats{skipped}\n";
    print "  Deleted:             $stats{deleted}\n";
    print "=" x 50 . "\n";
}

# ===========================================================================
# main
# ===========================================================================

sub usage {
    print <<'END';
Usage: music_sync.pl <source> <target> [options]

  --print               Print all distinct run IDs and runtimes
  --dump RUNID          Copy all files from a specific run ID to --dump-target
  --dump-target PATH    Target folder for --dump
  --dry-run             Show what would be done without making changes
  --copy-only           Copy all files without transcoding
  --threads|-j N        Number of parallel threads (default: CPU count)
  --db PATH             Database file location (default: target/.music_sync_db.json)
  --update-hash         Updates the hash if the target file exists
END
    exit 1;
}

my ($opt_print, $opt_dump, $opt_dump_target);
my ($opt_dry_run, $opt_copy_only, $opt_update_hash);
my ($opt_threads, $opt_db);

GetOptions(
    'print'         => \$opt_print,
    'dump=i'        => \$opt_dump,
    'dump-target=s' => \$opt_dump_target,
    'dry-run'       => \$opt_dry_run,
    'copy-only'     => \$opt_copy_only,
    'threads|j=i'   => \$opt_threads,
    'db=s'          => \$opt_db,
    'update-hash'   => \$opt_update_hash,
) or usage();

my ($opt_source, $opt_target) = @ARGV;
usage() unless defined $opt_source && defined $opt_target;

unless (-e $opt_source) {
    print "Error: Source folder does not exist: $opt_source\n";
    exit 1;
}
unless (-d $opt_source) {
    print "Error: Source is not a directory: $opt_source\n";
    exit 1;
}

make_path($opt_target) unless -d $opt_target;

my $db_path = $opt_db // File::Spec->catfile($opt_target, $DB_FILENAME);
my $db      = db_new($db_path);

# maybe just print all prior runs
if ($opt_print) {
    db_print_runs($db);
    exit 0;
}

my $syncer = syncer_new(
    source    => $opt_source,
    target    => $opt_target,
    db        => $db,
    dry_run   => $opt_dry_run   // 0,
    copy_only => $opt_copy_only // 0,
    threads   => $opt_threads,
    update_hash => $opt_update_hash // 0,
);

# maybe dump all files from a prior run
if (defined $opt_dump) {
    unless ($opt_dump_target) {
        print "Error: --dump requires --dump-target\n";
        exit 1;
    }
    syncer_dump_run($syncer, $opt_dump, $opt_dump_target);
    exit 0;
}

# no, really sync
syncer_check_dependencies($syncer) unless $opt_copy_only;
syncer_sync($syncer);
