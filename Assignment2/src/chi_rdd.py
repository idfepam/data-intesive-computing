#!/usr/bin/env python3
"""
Stage 1 of the chi-square pipeline - counting.

For every review we tokenise the reviewText field, lowercase it, drop
stopwords and single-character tokens, and turn the result into a SET
of unigrams (so a term contributes at most once per document - that's
what the chi-square test expects, document frequency and not raw
frequency).

The mapper then emits three kinds of (key, value) records:

    ("t", term, cat) -> 1     one more doc in `cat` contains `term`
    ("c", cat)       -> 1     one more doc belongs to `cat`
    ("n",)           -> 1     one more doc in total

We keep small per-task dictionaries and flush them in mapper_final
(this is the classic in-mapper combiner trick), and there's also a
regular combiner on top. Both are just sums. Between the two of them
the amount of data that has to be shuffled is much smaller than the
raw stream of 1s we would get otherwise.

The output of this job is consumed by chi_stats.py, together with a
tiny meta.tsv file that the driver builds from the "c" / "n" records.
"""

from __future__ import annotations

import json
import re
from collections import defaultdict

from mrjob.job import MRJob


# The exact delimiter class from the assignment description: whitespace,
# tabs, digits and a long list of punctuation characters, plus the euro
# and section symbols.
TOKEN_SPLIT_RE = re.compile(
    r"[\s\d()\[\]{}.!?,;:+=\-_\"'`~#@&*%€$§\\/]+",
    flags=re.UNICODE,
)


class ChiCount(MRJob):
    """Count A(t,c), n_c and N with one MapReduce job."""

    # How many distinct (term, category) keys we are willing to hold in the
    # in-mapper buffer before flushing it. This just keeps memory bounded
    # on very long input splits - the exact number isn't critical.
    _FLUSH_THRESHOLD = 200_000

    # --- setup -----------------------------------------------------------
    def configure_args(self):
        super().configure_args()
        self.add_passthru_arg(
            "--stopwords",
            default="stopwords.txt",
            help="Name of the stopword file made available via --file",
        )

    def mapper_init(self):
        # Load the stopword list. It's shipped to each task with --file.
        with open(self.options.stopwords, "r", encoding="utf-8") as fh:
            self._stopwords = {w.strip().lower() for w in fh if w.strip()}

        # Per-task accumulators for the in-mapper combiner.
        self._buf_tc: dict[tuple[str, str], int] = defaultdict(int)
        self._buf_c: dict[str, int] = defaultdict(int)
        self._buf_n: int = 0

    # --- mapper ----------------------------------------------------------
    def mapper(self, _key, line):
        # One review per line. Broken JSON is simply skipped.
        try:
            rec = json.loads(line)
        except (ValueError, TypeError):
            return

        category = rec.get("category")
        text = rec.get("reviewText") or ""
        if not category:
            return

        # Tokenise, lowercase, then drop stopwords and length-1 tokens.
        # `unique` is a set, so each term is counted at most once per doc.
        tokens = TOKEN_SPLIT_RE.split(text.lower())
        unique = {t for t in tokens
                  if len(t) > 1 and t not in self._stopwords}

        self._buf_n += 1
        self._buf_c[category] += 1
        for term in unique:
            self._buf_tc[(term, category)] += 1

        # Occasionally flush so the buffer cannot grow without bound.
        if len(self._buf_tc) > self._FLUSH_THRESHOLD:
            yield from self._flush()

    def mapper_final(self):
        # Final flush when the input split ends.
        yield from self._flush()

    def _flush(self):
        # Dump the current buffers as (key, count) pairs and reset them.
        for (term, cat), count in self._buf_tc.items():
            yield ("t", term, cat), count
        for cat, count in self._buf_c.items():
            yield ("c", cat), count
        if self._buf_n:
            yield ("n",), self._buf_n

        self._buf_tc.clear()
        self._buf_c.clear()
        self._buf_n = 0

    # --- combiner / reducer ---------------------------------------------
    # Both do the same thing - just add up partial counts for the same key.
    def combiner(self, key, values):
        yield key, sum(values)

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == "__main__":
    ChiCount.run()
