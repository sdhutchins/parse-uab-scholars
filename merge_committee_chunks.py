import os
import json
from collections import defaultdict

# Configuration
input_dir = "data/committees_by_id"
output_file = "data/uab_grad_committees_grouped.json"

# Initialize merged dictionary
merged = defaultdict(list)
merged_files = 0
skipped_files = 0
empty_files = 0

# Iterate over all faculty-level JSONs
for fname in sorted(os.listdir(input_dir)):
    if not fname.endswith(".json") or not fname.replace(".json", "").isdigit():
        continue  # Skip non-standard or malformed filenames

    discovery_id = fname.replace(".json", "")
    path = os.path.join(input_dir, fname)

    try:
        with open(path, "r", encoding="utf-8") as f:
            records = json.load(f)

        if not isinstance(records, list):
            raise ValueError("Expected a list of entries")

        if not records:
            empty_files += 1
            continue  # Skip empty but valid JSON files

        merged[discovery_id].extend(records)
        merged_files += 1

    except Exception as e:
        print(f"⚠️ Skipped {fname} due to error: {e}")
        skipped_files += 1

# Write final merged file
with open(output_file, "w", encoding="utf-8") as f:
    json.dump(merged, f, indent=2, ensure_ascii=False)

# Summary
print(f"✅ Merged {merged_files} faculty JSONs into {output_file}")
print(f"🧠 Total unique faculty: {len(merged)}")
if skipped_files > 0:
    print(f"⚠️ Skipped {skipped_files} corrupt or unreadable files")
if empty_files > 0:
    print(f"⚪ Skipped {empty_files} empty JSON files")
