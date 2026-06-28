"""Upload each devset review individually to the raw S3 bucket."""

import argparse
import json
import os
import time

import boto3
from botocore.config import Config

os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

SSM_RAW_BUCKET = "/dic-reviews-app/buckets/raw"
DEVSET_ID_FMT = "devset-{index:06d}"
MAX_RETRIES = 5


def read_review_file(path: str) -> list[dict]:
    raw = open(path, encoding="utf-8").read().strip()
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return parsed
        if isinstance(parsed, dict):
            return [parsed]
    except json.JSONDecodeError:
        pass
    return [json.loads(line) for line in raw.splitlines() if line.strip()]


def _clients(endpoint: str):
    cfg = Config(max_pool_connections=10)
    session = boto3.session.Session()
    return (
        session.client("s3", endpoint_url=endpoint, config=cfg),
        session.client("ssm", endpoint_url=endpoint, config=cfg),
    )


def _put_with_retry(s3, bucket: str, key: str, body: bytes) -> None:
    for attempt in range(MAX_RETRIES):
        try:
            s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=body,
                ContentType="application/json",
            )
            return
        except Exception as exc:
            if attempt == MAX_RETRIES - 1:
                raise RuntimeError(f"upload failed for {key}: {exc}") from exc
            delay = (2 ** attempt) * 0.5
            print(f"  retry {key} in {delay:.1f}s ({exc})")
            time.sleep(delay)


def upload_devset(devset_path: str, endpoint: str, limit: int | None, pause: float) -> None:
    s3, ssm = _clients(endpoint)
    bucket = ssm.get_parameter(Name=SSM_RAW_BUCKET)["Parameter"]["Value"]
    reviews = read_review_file(devset_path)
    if limit:
        reviews = reviews[:limit]

    total = len(reviews)
    print(f"Uploading {total} reviews from {devset_path} -> s3://{bucket}")

    for idx, review in enumerate(reviews):
        source_id = DEVSET_ID_FMT.format(index=idx)
        payload = {**review, "sourceId": source_id}
        _put_with_retry(
            s3,
            bucket,
            f"{source_id}.json",
            json.dumps(payload).encode("utf-8"),
        )
        if (idx + 1) % 50 == 0 or idx + 1 == total:
            print(f"  progress {idx + 1}/{total}")
        if pause:
            time.sleep(pause)

    print("Upload complete. Run tally_results.py --wait to collect statistics.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--devset", default="data/reviews_devset.json")
    parser.add_argument("--endpoint", default=os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566"))
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--sleep", type=float, default=0.0)
    args = parser.parse_args()
    upload_devset(args.devset, args.endpoint, args.limit, args.sleep)


if __name__ == "__main__":
    main()
