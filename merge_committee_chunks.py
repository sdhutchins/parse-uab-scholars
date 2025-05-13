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

# Iterate over all faculty-level JSONs
for fname in sorted(os.listdir(input_dir)):
    if fname.endswith(".json"):
        path = os.path.join(input_dir, fname)
        try:
            with open(path, "r", encoding="utf-8") as f:
                records = json.load(f)
                if not isinstance(records, list):
                    raise ValueError("Expected a list of entries")

                discovery_id = fname.replace(".json", "")
                merged[discovery_id].extend(records)
                merged_files += 1
        except Exception as e:
            print(f"⚠️ Skipped {fname} due to error: {e}")
            skipped_files += 1

# Write final merged file
with open(output_file, "w", encoding="utf-8") as f:
    json.dump(merged, f, indent=2, ensure_ascii=False)

print(f"✅ Merged {merged_files} faculty JSONs into {output_file}")
print(f"🧠 Total unique faculty: {len(merged)}")
if skipped_files > 0:
    print(f"⚠️ Skipped {skipped_files} corrupt or unreadable files")
