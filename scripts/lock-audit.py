import re
import subprocess
import sys
from collections import defaultdict

since = sys.argv[1]
containers = ["partitioner-1", "partitioner-2"]
PATTERN = re.compile(
    r"^(\S+)\s.*operation=(lock\.acquired|lock\.released)\s+lock=(\S+)\s+owner=(\S+)")

holds = defaultdict(list)
open_locks = {}
acquisitions = defaultdict(int)


def collect(container):
    output = subprocess.run(["docker", "logs", "--since", since, "psl-" + container],
                            capture_output=True, text=True).stdout
    for line in output.splitlines():
        match = PATTERN.match(line)
        if match:
            record(container, *match.groups())


def record(container, timestamp, operation, lock, owner):
    key = (container, lock)
    if operation == "lock.acquired":
        open_locks[key] = timestamp
        acquisitions[key] += 1
        return
    if key in open_locks:
        holds[lock].append((open_locks.pop(key), timestamp, container))


def overlaps_of(lock, ranges):
    found = 0
    ordered = sorted(ranges)
    for previous, current in zip(ordered, ordered[1:]):
        if current[0] < previous[1] and current[2] != previous[2]:
            found += 1
            print(f"  SOBREPOSIÇÃO no lock {lock}: {previous} x {current}")
    return found


for name in containers:
    collect(name)

overlaps = sum(overlaps_of(lock, ranges) for lock, ranges in holds.items())

for (container, lock), count in sorted(acquisitions.items()):
    print(f"  {container:<14} lock={lock:<26} aquisicoes={count}")
print(f"locks_distintos={len(holds)}")
print(f"overlaps={overlaps}")
