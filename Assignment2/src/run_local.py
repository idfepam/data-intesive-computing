#!/usr/bin/env python3

"""
Local runner for Assignment 2 - Part 1 (Spark RDD version).

Usage:
    python run_local.py [INPUT] [OUTPUT]

Defaults:
    INPUT  = ../reviews_devset.json
    OUTPUT = output_rdd
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path


HERE = Path(__file__).resolve().parent


def clean_output(path: Path):
    """
    Remove old Spark output directory if it exists.
    """
    if path.exists():
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()


def main() -> int:
    # ---------------------------------------------------------
    # Input / output paths
    # ---------------------------------------------------------

    input_path = (
        Path(sys.argv[1])
        if len(sys.argv) > 1
        else HERE.parent / "reviews_devset.json"
    )

    output_path = (
        Path(sys.argv[2])
        if len(sys.argv) > 2
        else HERE / "output_rdd"
    )

    # ---------------------------------------------------------
    # Clean old Spark output
    # ---------------------------------------------------------

    clean_output(output_path)

    # ---------------------------------------------------------
    # Run Spark job locally
    # ---------------------------------------------------------

    print(f"[run_local] Input  : {input_path}")
    print(f"[run_local] Output : {output_path}")

    subprocess.check_call(
        [
            "spark-submit",

            # Run Spark locally using all CPU cores
            "--master", "local[*]",

            # Make stopwords visible to Spark workers
            "--files", str(HERE / "stopwords.txt"),

            str(HERE / "chi_rdd.py"),

            str(input_path),
            str(output_path),
        ]
    )

    print("[run_local] Done.")

    # ---------------------------------------------------------
    # Show final output location
    # ---------------------------------------------------------

    part_file = output_path / "part-00000"

    if part_file.exists():
        print(f"[run_local] Result file: {part_file}")

    return 0


if __name__ == "__main__":
    sys.exit(main())


    