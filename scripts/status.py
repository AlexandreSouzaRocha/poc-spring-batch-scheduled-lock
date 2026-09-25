import json
import sys

for file in json.load(sys.stdin):
    partitioning = file.get("partitioning") or {}
    print(f"{file['id']}  {file['status']:<19} attempts={file.get('attempts')} "
          f"lines={partitioning.get('lineCount')} partitions={partitioning.get('count')}  {file['fileName']}")
