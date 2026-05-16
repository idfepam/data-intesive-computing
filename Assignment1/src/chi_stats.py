#!/usr/bin/env python3
"""
Stage 2 of the chi-square pipeline.

This job takes the counts produced by chi_count.py plus the small
meta.tsv file (category sizes n_c and total document count N) and
turns them into the final output file:

  - one line per category with the 75 highest chi-square terms, and
  - one final line with all the selected terms in alphabetical order.

We do it in three MRSteps:

  1. Regroup counts by term. For each term we then see all its
     (category, A) pairs in a single reducer call, and from that we
     can compute chi-square for every (term, category) pair:

         A = A(t, c)                 # docs in c that contain t
         B = total_with_term - A     # docs NOT in c that contain t
         C = n_c - A                 # docs in c without t
         D = N - n_c - B             # docs NOT in c without t

         chi2 = N * (A*D - B*C)^2 / ((A+B)*(A+C)*(B+D)*(C+D))

  2. Per category, keep the top 75 terms using a bounded min-heap.

  3. A single reducer that just sorts the 22 category lines
     alphabetically, prints them, and appends the merged dictionary
     line at the end.

The last step uses RawValueProtocol so the output is plain text; the
first two steps use JSONProtocol (mrjob's default) for the
intermediate (k, v) records.
"""

from __future__ import annotations

import heapq
from typing import Iterable

from mrjob.job import MRJob
from mrjob.protocol import JSONProtocol, RawValueProtocol
from mrjob.step import MRStep


TOP_K = 75


class ChiStats(MRJob):
    """Chi-square, top-75 per category, and the final merged dictionary."""

    # Stage 1 of this job reads the JSON (k, v) records that chi_count.py
    # produced, so JSONProtocol is the right input format. We keep the
    # intermediate (k, v) records as JSON too, and switch to plain text
    # only for the final output.
    INPUT_PROTOCOL = JSONProtocol
    INTERNAL_PROTOCOL = JSONProtocol
    OUTPUT_PROTOCOL = RawValueProtocol

    # --- setup -----------------------------------------------------------
    def configure_args(self):
        super().configure_args()
        self.add_passthru_arg(
            "--meta",
            default="meta.tsv",
            help=("Name of the meta file built by the driver from "
                  "chi_count.py's output. The file itself is shipped "
                  "to the tasks separately with --file."),
        )

    def steps(self):
        return [
            # Step 1: re-key counts by term and compute chi-square.
            MRStep(
                mapper_init=self._load_meta,
                mapper=self.s1_mapper,
                reducer_init=self._load_meta,
                reducer=self.s1_reducer,
            ),
            # Step 2: for each category, keep only the top-75 terms.
            MRStep(reducer=self.s2_reducer),
            # Step 3: single reducer that produces the final output file.
            # We force a single reducer so the category lines come out
            # in alphabetical order and we can print the merged
            # dictionary on a single last line.
            MRStep(
                reducer_init=self.s3_reducer_init,
                reducer=self.s3_reducer,
                reducer_final=self.s3_reducer_final,
                jobconf={"mapreduce.job.reduces": "1"},
            ),
        ]

    # --- meta loader (used by both mapper_init and reducer_init of step 1)
    def _load_meta(self):
        # Reads meta.tsv and stores the category sizes and the global N.
        # Format of the file:
        #     n\t<N>
        #     c\t<category>\t<n_c>
        self.cat_sizes: dict[str, int] = {}
        self.N: int = 0
        with open(self.options.meta, "r", encoding="utf-8") as fh:
            for raw in fh:
                parts = raw.rstrip("\n").split("\t")
                if not parts:
                    continue
                if parts[0] == "c" and len(parts) >= 3:
                    self.cat_sizes[parts[1]] = int(parts[2])
                elif parts[0] == "n" and len(parts) >= 2:
                    self.N = int(parts[1])

    # --- step 1: regroup by term, compute chi-square ---------------------
    def s1_mapper(self, key, value):
        # key arrives as a JSON-decoded list, e.g. ["t", term, cat].
        # We only forward the "t" records - "c" / "n" records are
        # already captured in meta.tsv, so we drop them here.
        if not isinstance(key, list) or not key:
            return
        if key[0] != "t" or len(key) < 3:
            return
        term = key[1]
        cat = key[2]
        yield term, [cat, int(value)]

    def s1_reducer(self, term, values):
        # Collect (cat, A) for this term. We need the list twice:
        # once to get total_with_term and once to emit per-cat chi2.
        records = [(cat, int(a)) for cat, a in values]
        total_with_term = sum(a for _, a in records)
        N = self.N
        for cat, A in records:
            n_c = self.cat_sizes.get(cat, 0)
            B = total_with_term - A
            C = n_c - A
            D = N - n_c - B
            denom = (A + B) * (A + C) * (B + D) * (C + D)
            if denom <= 0:
                # Happens e.g. when a term appears only inside this one
                # category, which makes chi-square undefined. Skip it.
                continue
            diff = A * D - B * C
            chi2 = (N * diff * diff) / denom
            yield cat, [term, chi2]

    # --- step 2: top-K per category using a bounded min-heap -------------
    def s2_reducer(self, cat, values: Iterable):
        heap: list[tuple[float, str]] = []
        for term, chi2 in values:
            if len(heap) < TOP_K:
                heapq.heappush(heap, (chi2, term))
            elif chi2 > heap[0][0]:
                heapq.heapreplace(heap, (chi2, term))
        # Sort the heap once we're done: highest chi-square first,
        # ties broken alphabetically just to make the output stable.
        top = sorted(heap, key=lambda x: (-x[0], x[1]))
        line = cat + " " + " ".join(f"{t}:{c:.4f}" for c, t in top)
        terms_only = [t for _, t in top]
        # Route everything to the same step-3 reducer via a constant key.
        yield "ALL", ["CAT", cat, line, terms_only]

    # --- step 3: single reducer that writes the final file --------------
    def s3_reducer_init(self):
        self._cat_lines: dict[str, str] = {}
        self._all_terms: set[str] = set()

    def s3_reducer(self, _key, values):
        # Just collect. The actual output is written in reducer_final.
        for payload in values:
            _, cat, line, terms = payload
            self._cat_lines[cat] = line
            self._all_terms.update(terms)
        return
        yield  # pragma: no cover - just here so mrjob treats this as a generator

    def s3_reducer_final(self):
        # 22 category lines in alphabetical order ...
        for cat in sorted(self._cat_lines):
            yield None, self._cat_lines[cat]
        # ... and the merged dictionary on the last line.
        yield None, " ".join(sorted(self._all_terms))


if __name__ == "__main__":
    ChiStats.run()