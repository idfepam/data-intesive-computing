"""Aggregate devset-only pipeline statistics from DynamoDB."""

import argparse
import json
import os
import time
from collections import Counter
from decimal import Decimal

import boto3

os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

from run_devset import read_review_file

DEVSET_PREFIX = "devset-"
SSM_REVIEWS = "/dic-reviews-app/tables/reviews"
SSM_USERS = "/dic-reviews-app/tables/users"


def _normalize_number(value):
    if isinstance(value, Decimal):
        return int(value) if value % 1 == 0 else float(value)
    return value


def _table_names(ssm):
    reviews = ssm.get_parameter(Name=SSM_REVIEWS)["Parameter"]["Value"]
    users = ssm.get_parameter(Name=SSM_USERS)["Parameter"]["Value"]
    return reviews, users


def _scan_all(table):
    items = []
    page = table.scan()
    items.extend(page.get("Items", []))
    while "LastEvaluatedKey" in page:
        page = table.scan(ExclusiveStartKey=page["LastEvaluatedKey"])
        items.extend(page.get("Items", []))
    return items


def _devset_rows(dynamodb, reviews_table_name: str) -> list[dict]:
    table = dynamodb.Table(reviews_table_name)
    return [
        row for row in _scan_all(table)
        if row.get("reviewId", "").startswith(DEVSET_PREFIX)
    ]


def _await_pipeline(dynamodb, reviews_table_name, expected, timeout, interval=5):
    deadline = time.time() + timeout
    rows = []
    while time.time() < deadline:
        rows = _devset_rows(dynamodb, reviews_table_name)
        waiting = sum(1 for r in rows if r.get("sentiment", "pending") == "pending")
        print(f"  {len(rows)}/{expected} rows, {waiting} pending sentiment")
        if len(rows) >= expected and waiting == 0:
            return rows
        time.sleep(interval)
    print(f"  timed out after {timeout}s; returning partial data")
    return rows


def _build_summary(rows, dynamodb, users_table_name: str) -> dict:
    sentiments = Counter(r.get("sentiment", "pending") for r in rows)
    profanity_fails = sum(1 for r in rows if r.get("profanityCheck") == "impolite")
    compared = [r for r in rows if r.get("ratingSentimentAgreement") is not None]
    disagreements = sum(1 for r in compared if r.get("ratingSentimentAgreement") is False)

    user_ids = {r.get("userId") for r in rows if r.get("userId")}
    users_table = dynamodb.Table(users_table_name)
    banned = []
    for uid in user_ids:
        profile = users_table.get_item(Key={"userId": uid}).get("Item")
        if profile and profile.get("banned"):
            banned.append({
                "userId": uid,
                "impoliteCount": _normalize_number(profile.get("impoliteCount", 0)),
            })

    return {
        "totalReviews": len(rows),
        "sentimentCounts": dict(sentiments),
        "profanityFailCount": profanity_fails,
        "ratingSentimentComparedCount": len(compared),
        "ratingSentimentDisagreementCount": disagreements,
        "bannedUsers": banned,
    }


def _print_report(summary: dict) -> None:
    print("\n=== Results (devset only) ===")
    print(f"Total devset reviews processed: {summary['totalReviews']}")
    print("Sentiment distribution:")
    for label in ("positive", "neutral", "negative", "pending"):
        count = summary["sentimentCounts"].get(label, 0)
        if count or label != "pending":
            print(f"  {label}: {count}")
    print(f"Reviews failing profanity check: {summary['profanityFailCount']}")
    print(
        f"Rating/sentiment disagreements: {summary['ratingSentimentDisagreementCount']} "
        f"of {summary['ratingSentimentComparedCount']} compared"
    )
    print(f"Banned users: {len(summary['bannedUsers'])}")
    for entry in summary["bannedUsers"]:
        print(f"  - {entry['userId']} (impoliteCount={entry['impoliteCount']})")


def _save_json(summary: dict, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2)
    print(f"\nWrote {path}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--devset", default="data/reviews_devset.json")
    parser.add_argument("--endpoint", default=os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566"))
    parser.add_argument("--wait", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--timeout", type=int, default=600)
    parser.add_argument("--out", default="data/results_summary.json")
    args = parser.parse_args()

    ssm = boto3.client("ssm", endpoint_url=args.endpoint)
    dynamodb = boto3.resource("dynamodb", endpoint_url=args.endpoint)
    reviews_name, users_name = _table_names(ssm)

    if args.wait:
        full_count = len(read_review_file(args.devset))
        target = min(args.limit, full_count) if args.limit else full_count
        print(f"Waiting for {target} devset reviews...")
        rows = _await_pipeline(dynamodb, reviews_name, target, args.timeout)
    else:
        rows = _devset_rows(dynamodb, reviews_name)

    summary = _build_summary(rows, dynamodb, users_name)
    _print_report(summary)
    _save_json(summary, args.out)


if __name__ == "__main__":
    main()
