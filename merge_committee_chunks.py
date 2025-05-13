import os
import json
from collections import defaultdict

# Configuration
input_dir = "data/chunked_committees"
output_file = "data/uab_grad_committees_grouped.json"

# Initialize merged dictionary
merged = defaultdict(list)
merged_files = 0
skipped_files = 0

# Iterate over all chunk files
for fname in sorted(os.listdir(input_dir)):
    if fname.startswith("grad_committees_chunk_") and fname.endswith(".json"):
        path = os.path.join(input_dir, fname)
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
                for discovery_id, entries in data.items():
                    merged[discovery_id].extend(entries)
            merged_files += 1
        except Exception as e:
            print(f"⚠️ Skipped {fname} due to error: {e}")
            skipped_files += 1

# Write final merged file
with open(output_file, "w", encoding="utf-8") as f:
    json.dump(merged, f, indent=2, ensure_ascii=False)

print(f"✅ Merged {merged_files} chunk files into {output_file}")
print(f"🧠 Total unique users: {len(merged)}")
if skipped_files > 0:
    print(f"⚠️ Skipped {skipped_files} corrupt or missing files")
