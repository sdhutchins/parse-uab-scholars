import os
import json
from collections import defaultdict

# Configuration
input_dir = "data/chunked_committees"
output_file = "data/uab_grad_committees_grouped.json"

# Initialize merged dictionary
merged = defaultdict(list)

# Iterate over all chunk files
for fname in os.listdir(input_dir):
    if fname.startswith("grad_committees_chunk_") and fname.endswith(".json"):
        path = os.path.join(input_dir, fname)
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            for discovery_id, entries in data.items():
                merged[discovery_id].extend(entries)

# Write final merged file
with open(output_file, "w", encoding="utf-8") as f:
    json.dump(merged, f, indent=2, ensure_ascii=False)

print(f"✅ Merged {len(os.listdir(input_dir))} chunks into {output_file}")
print(f"🧠 Total unique users: {len(merged)}")
