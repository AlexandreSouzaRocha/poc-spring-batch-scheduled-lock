import json
import sys

GIGABYTE = 1024 ** 3


def folder_totals(blobs):
    totals = {}
    for blob in blobs:
        folder = blob["name"].split("/")[0] + "/"
        size, count = totals.get(folder, (0, 0))
        totals[folder] = (size + (blob["bytes"] or 0), count + 1)
    return totals


def main():
    totals = folder_totals(json.load(sys.stdin))
    for folder, (size, count) in sorted(totals.items()):
        print("  {:20} {:8.2f} GB  {:5} arquivo(s)".format(folder, size / GIGABYTE, count))
    size = sum(value for value, _ in totals.values())
    count = sum(value for _, value in totals.values())
    print("  {:20} {:8.2f} GB  {:5} arquivo(s)".format("TOTAL", size / GIGABYTE, count))


main()
