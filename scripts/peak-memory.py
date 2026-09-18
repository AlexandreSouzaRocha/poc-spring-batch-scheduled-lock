import sys

UNITS = (("GiB", 1024), ("MiB", 1), ("KiB", 1 / 1024), ("B", 1 / 1024 ** 2))


def megabytes(value):
    for unit, factor in UNITS:
        if value.endswith(unit):
            return float(value[: -len(unit)]) * factor
    return 0.0


def main():
    samples = [megabytes(line.strip()) for line in sys.stdin if line.strip()]
    print(f"{max(samples):.0f}" if samples else "-")


main()
