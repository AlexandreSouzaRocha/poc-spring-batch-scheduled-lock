import re
import subprocess
import sys
from collections import defaultdict

since = sys.argv[1]
containers = ["partitioner-1", "partitioner-2"]
pattern = re.compile(r"^(\S+)\s.*operation=(lock\.acquired|cycle\.end)\s+scheduler=(\S+)\s+owner=(\S+)")
intervals = defaultdict(list)
open_cycles = {}
cycles = defaultdict(int)

for container in containers:
    output = subprocess.run(["docker", "logs", "--since", since, "psl-" + container],
                            capture_output=True, text=True).stdout
    for line in output.splitlines():
        match = pattern.match(line)
        if not match:
            continue
        timestamp, operation, scheduler, owner = match.groups()
        key = (container, scheduler)
        if operation == "lock.acquired":
            open_cycles[key] = timestamp
            continue
        if key in open_cycles:
            intervals[scheduler].append((open_cycles.pop(key), timestamp, container))
            cycles[key] += 1

overlaps = 0
for scheduler, ranges in intervals.items():
    ordered = sorted(ranges)
    for previous, current in zip(ordered, ordered[1:]):
        if current[0] < previous[1] and current[2] != previous[2]:
            overlaps += 1
            print(f"  SOBREPOSIÇÃO {scheduler}: {previous} x {current}")

for (container, scheduler), count in sorted(cycles.items()):
    print(f"  {container:<14} scheduler={scheduler:<18} ciclos_executados={count}")
print(f"overlaps={overlaps}")
