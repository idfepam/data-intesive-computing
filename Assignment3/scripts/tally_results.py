"""
tally_results.py
------------------
Scans the reviews and users tables and produces the numbers the
assignment's Results section needs:

  - number of positive / neutral / negative reviews
  - number of reviews that didn't pass the profanity check
  - users resulting in a ban, if any

Scoped to devset-derived rows ONLY (reviewId starting with "devset-",
the prefix scripts/run_devset.py assigns) -- per the assignment, your
own corner-case test reviews must NOT be counted here. Banned users are
similarly scoped to userIds seen among devset rows, so a banned status
that only resulted from your own extra testing doesn't bleed into the
devset numbers either.

Usage:
    python scripts/tally_results.py --devset data/reviews_devset.json --wait
    python scripts/tally_results.py   # one-shot, no waiting -- reports
                                       # whatever has landed so far
"""

import argparse
import json
import os
import time
from collections import Counter
from decimal import Decimal

import boto3

# See run_devset.py for why this is needed standalone, not just via run.sh.
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

from run_devset import load_reviews  # reuse the same array/JSONL loader

SOURCE_PREFIX = "devset-"


def to_jsonable(value):
    """DynamoDB scans return Decimal for numeric fields -- convert for
    clean json.dumps() when we write the summary file."""
    if isinstance(value, Decimal):
        return int(value) if value % 1 == 0 else float(value)
    return value


def get_table_names(ssm):
    reviews = ssm.get_parameter(Name="/dic-reviews-app/tables/reviews")["Parameter"]["Value"]
    users = ssm.get_parameter(Name="/dic-reviews-app/tables/users")["Parameter"]["Value"]
    return reviews, users


def scan_devset_reviews(dynamodb, reviews_table_name):
    table = dynamodb.Table(reviews_table_name)
    items = []
    response = table.scan()
    items.extend(response.get("Items", []))
    while "LastEvaluatedKey" in response:
        response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        items.extend(response.get("Items", []))
    return [i for i in items if i.get("reviewId", "").startswith(SOURCE_PREFIX)]


def wait_for_completion(dynamodb, reviews_table_name, expected_count, timeout, poll_interval=5):
    """Polls until `expected_count` devset rows exist AND none of them
    are still sentiment='pending', or until timeout. Returns the final
    list of devset items either way (partial results on timeout)."""
    deadline = time.time() + timeout
    items = []
    while time.time() < deadline:
        items = scan_devset_reviews(dynamodb, reviews_table_name)
        pending = [i for i in items if i.get("sentiment", "pending") == "pending"]
        print(f"  {len(items)}/{expected_count} rows present, {len(pending)} still pending sentiment")
        if len(items) >= expected_count and not pending:
            return items
        time.sleep(poll_interval)
    print(f"  WARNING: timed out after {timeout}s -- reporting partial results")
    return items


def tally(items, dynamodb, users_table_name):
    sentiment_counts = Counter(i.get("sentiment", "pending") for i in items)
    profanity_fail_count = sum(1 for i in items if i.get("profanityCheck") == "impolite")

    # ratingSentimentAgreement is written by sentiment-analysis as
    # True/False/None (None when there was no "overall" rating to
    # compare against) -- only count rows where a comparison was
    # actually made.
    rating_compared = [i for i in items if i.get("ratingSentimentAgreement") is not None]
    rating_disagreements = sum(1 for i in rating_compared if i.get("ratingSentimentAgreement") is False)

    devset_user_ids = {i.get("userId") for i in items if i.get("userId")}
    users_table = dynamodb.Table(users_table_name)
    banned_users = []
    for user_id in devset_user_ids:
        user_item = users_table.get_item(Key={"userId": user_id}).get("Item")
        if user_item and user_item.get("banned"):
            banned_users.append({
                "userId": user_id,
                "impoliteCount": to_jsonable(user_item.get("impoliteCount", 0)),
            })

    return {
        "totalReviews": len(items),
        "sentimentCounts": dict(sentiment_counts),
        "profanityFailCount": profanity_fail_count,
        "ratingSentimentComparedCount": len(rating_compared),
        "ratingSentimentDisagreementCount": rating_disagreements,
        "bannedUsers": banned_users,
    }


def print_summary(summary):
    print("\n=== Results (devset only) ===")
    print(f"Total devset reviews processed: {summary['totalReviews']}")
    print("Sentiment distribution:")
    for label in ("positive", "neutral", "negative", "pending"):
        count = summary["sentimentCounts"].get(label, 0)
        if count or label != "pending":
            print(f"  {label}: {count}")
    print(f"Reviews failing profanity check: {summary['profanityFailCount']}")
    print(f"Rating/sentiment disagreements: {summary['ratingSentimentDisagreementCount']} "
          f"of {summary['ratingSentimentComparedCount']} compared")
    print(f"Banned users: {len(summary['bannedUsers'])}")
    for u in summary["bannedUsers"]:
        print(f"  - {u['userId']} (impoliteCount={u['impoliteCount']})")


def write_summary_file(summary, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    print(f"\nWrote {path}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--devset", default="data/reviews_devset.json",
                         help="Used only to know the expected review count for --wait")
    parser.add_argument("--endpoint", default=os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566"))
    parser.add_argument("--wait", action="store_true",
                         help="Poll until every devset review has a final sentiment, instead of reporting whatever exists right now")
    parser.add_argument("--limit", type=int, default=None,
                         help="Expect only the first N devset reviews (must match the --limit used with run_devset.py, e.g. for a smoke test) -- without this, --wait waits for the FULL devset count and will time out if you only uploaded a subset")
    parser.add_argument("--timeout", type=int, default=600)
    parser.add_argument("--out", default="data/results_summary.json")
    args = parser.parse_args()

    ssm = boto3.client("ssm", endpoint_url=args.endpoint)
    dynamodb = boto3.resource("dynamodb", endpoint_url=args.endpoint)
    reviews_table_name, users_table_name = get_table_names(ssm)

    if args.wait:
        total_in_devset = len(load_reviews(args.devset))
        expected_count = min(args.limit, total_in_devset) if args.limit else total_in_devset
        print(f"Waiting for {expected_count} devset reviews to finish processing...")
        items = wait_for_completion(dynamodb, reviews_table_name, expected_count, args.timeout)
    else:
        items = scan_devset_reviews(dynamodb, reviews_table_name)

    summary = tally(items, dynamodb, users_table_name)
    print_summary(summary)
    write_summary_file(summary, args.out)


if __name__ == "__main__":
    main()
