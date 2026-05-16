#!/usr/bin/env python3
"""
End-to-end driver that runs the two MapReduce stages **locally** (using
mrjob's ``-r local`` runner) and produces the final ``output.txt``.

Usage:  ``python run_local.py [INPUT] [OUTPUT]``

    INPUT  : path to the JSON-lines review file
             (default: ``../reviews_devset.json``)
    OUTPUT : where to write the consolidated result
             (default: ``../output.txt``)

The cluster equivalent ``run.sh`` follows exactly the same steps but uses
``-r hadoop`` and HDFS paths.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path


HERE = Path(__file__).resolve().parent


def _clean(p: Path) -> None:
    if p.exists():
        if p.is_dir():
            shutil.rmtree(p)
        else:
            p.unlink()


def _read_part_files(dirpath: Path):
    """Yield lines from every ``part-*`` file under ``dirpath``."""
    for part in sorted(dirpath.glob("part-*")):
        with part.open("r", encoding="utf-8") as fh:
            for line in fh:
                yield line


def main() -> int:
    input_path = Path(sys.argv[1]) if len(sys.argv) > 1 else HERE.parent / "reviews_devset.json"
    output_path = Path(sys.argv[2]) if len(sys.argv) > 2 else HERE.parent / "output.txt"

    work = HERE / ".work"
    count_out = work / "counts"
    stats_out = work / "stats"
    meta_tsv = work / "meta.tsv"

    work.mkdir(exist_ok=True)
    _clean(count_out)
    _clean(stats_out)
    _clean(meta_tsv)

    python = sys.executable

    # ------------------------------------------------------------------
    # Stage 1: counting
    # ------------------------------------------------------------------
    print(f"[run_local] Stage 1: counting on {input_path}")
    subprocess.check_call(
        [
            python, str(HERE / "chi_count.py"),
            "-r", "inline",
            "--file", str(HERE / "stopwords.txt"),
            "--output-dir", str(count_out),
            "--no-output",
            str(input_path),
        ]
    )

    # ------------------------------------------------------------------
    # Extract meta (cat sizes + N) from stage-1 output into meta.tsv
    # ------------------------------------------------------------------
    print("[run_local] Extracting meta.tsv")
    N = 0
    cat_sizes: dict[str, int] = {}
    for raw in _read_part_files(count_out):
        k_raw, _, v_raw = raw.rstrip("\n").partition("\t")
        if not v_raw:
            continue
        try:
            key = json.loads(k_raw)
            val = int(json.loads(v_raw))
        except (ValueError, TypeError):
            continue
        if not isinstance(key, list) or not key:
            continue
        if key[0] == "c" and len(key) >= 2:
            cat_sizes[key[1]] = cat_sizes.get(key[1], 0) + val
        elif key[0] == "n":
            N += val
    with meta_tsv.open("w", encoding="utf-8") as fh:
        fh.write(f"n\t{N}\n")
        for cat in sorted(cat_sizes):
            fh.write(f"c\t{cat}\t{cat_sizes[cat]}\n")
    print(f"[run_local] N = {N}, categories = {len(cat_sizes)}")

    # ------------------------------------------------------------------
    # Stage 2: chi-square + top-75 + merged dictionary
    # ------------------------------------------------------------------
    print("[run_local] Stage 2: chi-square, top-75, merge")
    part_inputs = [str(p) for p in sorted(count_out.glob("part-*"))]
    subprocess.check_call(
        [
            python, str(HERE / "chi_stats.py"),
            "-r", "inline",
            "--file", str(meta_tsv),
            "--meta", "meta.tsv",
            "--output-dir", str(stats_out),
            "--no-output",
            *part_inputs,
        ]
    )

    # ------------------------------------------------------------------
    # Merge the stage-2 output (already in correct order thanks to the
    # single reducer) into a single output file.
    # ------------------------------------------------------------------
    print(f"[run_local] Writing {output_path}")
    with output_path.open("w", encoding="utf-8") as out_fh:
        for raw in _read_part_files(stats_out):
            out_fh.write(raw)

    print("[run_local] Done.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
