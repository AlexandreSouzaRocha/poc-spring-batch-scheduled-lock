import json
import sys

for file in json.load(sys.stdin):
    partitioning = file.get("partitioning") or {}
    execution = file.get("execution") or {}
    print(f"{file['id']}  {file['status']:<10} attempts={execution.get('attempts')} "
          f"lines={partitioning.get('lineCount')} partitions={partitioning.get('count')} "
          f"durationMs={execution.get('durationMs')}  {file['fileName']}")
