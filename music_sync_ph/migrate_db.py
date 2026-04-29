#!/usr/bin/env python3
import json
import os
import sys
from pathlib import Path
from datetime import datetime

def migrate_database(db_path_str):
    db_path = Path(db_path_str).expanduser().resolve()

    print(f"Checking for file: {db_path}")

    if not db_path.exists():
        print(f"Error: The file '{db_path}' does not exist.")
        return

    # Get the last modification time of the file
    mtime = os.path.getmtime(db_path)
    last_mod_date = datetime.fromtimestamp(mtime).isoformat()

    print(f"Loading database...")
    try:
        with open(db_path, 'r') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Error reading JSON: {e}")
        return

    # 1. Add global metadata if missing
    if 'global_metadata' not in data:
        data['global_metadata'] = {
            'last_runid': 1,
            'last_runtime': last_mod_date
        }
        print(f"Added global_metadata (RunID: 1, Time: {last_mod_date})")
    else:
        print("Global metadata already exists. Skipping.")

    # 2. Add runid and runtime to each individual file record
    files = data.get('files', {})
    updated_count = 0
    for rel_path, info in files.items():
        if 'runid' not in info:
            info['runid'] = 1
            info['runtime'] = last_mod_date
            updated_count += 1

    if updated_count > 0:
        print(f"Updating {updated_count} file entries...")
        with open(db_path, 'w') as f:
            json.dump(data, f, indent=2)
        print("Migration complete successfully.")
    else:
        print("No file records needed updating.")

if __name__ == '__main__':
    # If no arguments provided, show an error instead of staying silent
    if len(sys.argv) < 2:
        print("Usage: python3 migrate_db.py <path_to_db_json>")
        sys.exit(1)

    db_input_path = sys.argv[1]
    migrate_database(db_input_path)
