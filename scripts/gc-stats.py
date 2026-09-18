import re
import sys
from pathlib import Path

PAUSE = re.compile(r"\[(\d+[.,]\d+)s\].*\bGC\((\d+)\)\s+(Pause [^0-9]+?)\s.*?(\d+[.,]\d+)ms\s*$")


def pauses(path):
    found = []
    for line in Path(path).read_text(errors="ignore").splitlines():
        match = PAUSE.search(line)
        if match:
            found.append((match.group(3).strip(), float(match.group(4).replace(",", "."))))
    return found


def report(path):
    found = pauses(path)
    if not found:
        print(f"{Path(path).name}: nenhuma pausa encontrada")
        return
    durations = [duration for _, duration in found]
    kinds = {}
    for kind, duration in found:
        total, count = kinds.get(kind, (0.0, 0))
        kinds[kind] = (total + duration, count + 1)
    print(f"== {Path(path).name}")
    print(f"   pausas={len(found)}  total={sum(durations):.0f}ms  "
          f"media={sum(durations) / len(durations):.1f}ms  max={max(durations):.1f}ms")
    for kind, (total, count) in sorted(kinds.items(), key=lambda item: -item[1][0]):
        print(f"   {kind:42} n={count:4}  total={total:8.0f}ms  media={total / count:6.1f}ms")


for argument in sys.argv[1:]:
    report(argument)
