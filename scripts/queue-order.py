import json
import re
import sys

ORDEM = re.compile(r'"ordem":(\[[^\]]*\])')
NAME = re.compile(r"MOV_([A-Z]+)_(\d{4})\.(\d{2})\.(\d{2})\.")


def entries(line):
    match = ORDEM.search(line)
    if not match:
        return []
    return json.loads(match.group(1))


def described(file_name):
    match = NAME.search(file_name)
    if not match:
        return file_name
    return "{} {}-{}-{}".format(match.group(1), match.group(2), match.group(3), match.group(4))


for name in entries(sys.stdin.read()):
    print(described(name))
