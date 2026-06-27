Put `reviews_devset.json` here, plus any additional corner-case reviews
you author for testing (per the assignment's requirement to include
these in the final submission archive). Keep devset results and your
own extra test reviews clearly separate -- the Results section of the
report must report numbers for `reviews_devset.json` ONLY.

The pipeline expects ONE review JSON object per S3 upload (assignment
requirement #2: "the function chain has to start when a new, single
review is inserted"), so `reviews_devset.json` -- a collection -- can't
be uploaded as-is. Use `scripts/run_devset.py` to split it and upload
each review individually, and `scripts/tally_results.py` (or the
combined `scripts/collect_results.sh`) to collect the numbers
afterward -- see the root README's "Results & report" section.

This directory will also pick up `results_summary.json` /
`results_smoke.json` once you've run those scripts -- safe to delete
and regenerate at any time, they're just script output.
