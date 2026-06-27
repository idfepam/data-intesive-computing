"""
run_devset.py
--------------
Splits data/reviews_devset.json into individual review objects and
uploads each one to the raw-reviews bucket, exactly as the assignment
requires (one review per S3 upload starts the chain).

Each upload is tagged with a deterministic "sourceId" field
(devset-000000, devset-000001, ...). preprocessing/handler.py uses
that as the canonical reviewId instead of generating a random UUID,
which is what lets tally_results.py later isolate "came from the
devset" rows from any ad-hoc/manual test uploads you make -- purely by
reviewId prefix, with no extra bookkeeping needed.

Usage:
    python scripts/run_devset.py
    python scripts/run_devset.py --limit 20        # quick smoke test
    python scripts/run_devset.py --devset path.json --endpoint http://localhost:4566
"""

import argparse
import json
import os
import time

import boto3

# MiniStack accepts dummy credentials, but boto3 still needs SOME region
# configured to resolve an endpoint at all -- setdefault so this doesn't
# clobber real values if you do have a profile/env configured (e.g. via
# run.sh's exports in the same shell), but works standalone otherwise.
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")


def load_reviews(path: str) -> list[dict]:
    """Handles both a JSON array and line-delimited JSON (one review
    object per line) -- Amazon-review-style datasets ship in either
    format depending on source, so we don't assume which one this is."""
    with open(path, "r", encoding="utf-8") as f:
        text = f.read().strip()

    try:
        data = json.loads(text)
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return [data]
    except json.JSONDecodeError:
        pass

    reviews = []
    for line in text.splitlines():
        line = line.strip()
        if line:
            reviews.append(json.loads(line))
    return reviews


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--devset", default="data/reviews_devset.json")
    parser.add_argument("--endpoint", default=os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566"))
    parser.add_argument("--limit", type=int, default=None, help="Only upload the first N reviews (smoke testing)")
    parser.add_argument("--sleep", type=float, default=0.0, help="Seconds to sleep between uploads, if you want to throttle")
    args = parser.parse_args()

    from botocore.config import Config
    session = boto3.session.Session()
    config = Config(max_pool_connections=10)
    s3 = session.client("s3", endpoint_url=args.endpoint, config=config)
    ssm = session.client("ssm", endpoint_url=args.endpoint, config=config)

    raw_bucket = ssm.get_parameter(Name="/dic-reviews-app/buckets/raw")["Parameter"]["Value"]

    reviews = load_reviews(args.devset)
    if args.limit:
        reviews = reviews[: args.limit]

    print(f"Uploading {len(reviews)} reviews from {args.devset} to s3://{raw_bucket}")

    for i, review in enumerate(reviews):
        source_id = f"devset-{i:06d}"
        payload = {**review, "sourceId": source_id}

        # Retry with exponential backoff -- MiniStack returns 500 when
        # it's overwhelmed, so we back off and try again rather than
        # crashing out entirely.
        for attempt in range(5):
            try:
                s3.put_object(
                    Bucket=raw_bucket,
                    Key=f"{source_id}.json",
                    Body=json.dumps(payload).encode("utf-8"),
                    ContentType="application/json",
                )
                break
            except Exception as e:
                if attempt == 4:
                    print(f"  ERROR: failed to upload {source_id} after 5 attempts: {e}")
                    raise
                wait = (2 ** attempt) * 0.5  # 0.5s, 1s, 2s, 4s
                print(f"  upload {source_id} failed (attempt {attempt+1}), retrying in {wait}s...")
                time.sleep(wait)

        if (i + 1) % 50 == 0 or (i + 1) == len(reviews):
            print(f"  uploaded {i + 1}/{len(reviews)}")
        if args.sleep:
            time.sleep(args.sleep)

    print("Done. The pipeline will keep processing asynchronously --")
    print("run scripts/tally_results.py --wait once you're ready to collect results.")


if __name__ == "__main__":
    main()
