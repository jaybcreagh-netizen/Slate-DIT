#!/usr/bin/env python3
"""
Times Slate's copy engine against a plain copy, on your own card and drives.

    python bench_offload.py /Volumes/CARD /Volumes/DriveA /Volumes/DriveB

It makes a new folder called "slate-bench-<time>" on each destination and
copies the card into it twice: once with a plain copy (no checking) and once
with Slate (copy, read back from the drive, compare checksums). It prints how
long each took. Nothing is deleted: remove the "slate-bench-..." folders
yourself when you are done. Each destination needs free space for two copies
of the card.

For a fair comparison, unplug and replug the card between runs of this
script, so the computer is not serving the card from memory.
"""
import os
import shutil
import sys
import time

import copy_engine


def card_files(card):
    for root, _, files in os.walk(card):
        for name in files:
            path = os.path.join(root, name)
            yield path, os.path.relpath(path, card)


def main():
    if len(sys.argv) < 3:
        sys.exit(__doc__)
    card, drives = sys.argv[1], sys.argv[2:]
    files = list(card_files(card))
    total = sum(os.path.getsize(p) for p, _ in files)
    stamp = time.strftime("%Y%m%d-%H%M%S")
    folders = [os.path.join(d, f"slate-bench-{stamp}") for d in drives]
    print(f"{len(files)} files, {total / 1e9:.2f} GB, to {len(drives)} destination(s)\n")

    start = time.perf_counter()
    for folder in folders:
        shutil.copytree(card, os.path.join(folder, "plain"))
    if hasattr(os, "sync"):
        os.sync()
    plain = time.perf_counter() - start
    print(f"Plain copy, nothing checked : {plain:7.1f} s  {total / 1e6 / plain:6.0f} MB/s")

    start = time.perf_counter()
    bad = 0
    for path, relative in files:
        result = copy_engine.copy_file(path, [os.path.join(f, "slate", relative) for f in folders], "xxh64", "full")
        bad += sum(not d['ok'] for d in result['destinations'])
    slate = time.perf_counter() - start
    print(f"Slate, copied and verified  : {slate:7.1f} s  {total / 1e6 / slate:6.0f} MB/s")
    print(f"\nCopies that failed their check: {bad}")
    print("Bench folders left in place:")
    for folder in folders:
        print("  " + folder)


if __name__ == "__main__":
    main()
