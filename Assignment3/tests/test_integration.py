import json
import os
import time
import typing
import uuid

import boto3
import pytest

if typing.TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_ssm import SSMClient
    from mypy_boto3_lambda import LambdaClient
    from mypy_boto3_dynamodb import DynamoDBServiceResource

os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
os.environ["AWS_ACCESS_KEY_ID"] = "test"
os.environ["AWS_SECRET_ACCESS_KEY"] = "test"

s3: "S3Client" = boto3.client("s3", endpoint_url="http://localhost:4566")
ssm: "SSMClient" = boto3.client("ssm", endpoint_url="http://localhost:4566")
awslambda: "LambdaClient" = boto3.client("lambda", endpoint_url="http://localhost:4566")
dynamodb: "DynamoDBServiceResource" = boto3.resource("dynamodb", endpoint_url="http://localhost:4566")

POLL_INTERVAL_SECONDS = 1
POLL_TIMEOUT_SECONDS = 30


@pytest.fixture(autouse=True)
def _wait_for_lambdas():
    # makes sure that the lambdas are available before running integration tests
    awslambda.get_waiter("function_active").wait(FunctionName="preprocessing")
    awslambda.get_waiter("function_active").wait(FunctionName="profanity-check")
    awslambda.get_waiter("function_active").wait(FunctionName="sentiment-analysis")


@pytest.fixture(scope="session", autouse=True)
def _warm_up_pipeline():
    """The first review sent through a freshly (re)deployed chain pays
    for NLTK data downloads on cold start (preprocessing AND
    sentiment-analysis both download packages on their first
    invocation), on top of normal processing time -- that can push the
    very first review past the per-test POLL_TIMEOUT_SECONDS budget
    even though the pipeline is working correctly, which is exactly
    what produces "test 1 times out, tests 2-4 pass quickly right
    after". Absorbing that one-time cost here, before any individually-
    timed assertion runs, makes the real tests' timeouts meaningful
    again instead of being a coin flip on redeploy."""
    awslambda.get_waiter("function_active").wait(FunctionName="preprocessing")
    awslambda.get_waiter("function_active").wait(FunctionName="profanity-check")
    awslambda.get_waiter("function_active").wait(FunctionName="sentiment-analysis")

    warm_user = f"warmup-{uuid.uuid4()}"
    key = upload_review({
        "reviewerID": warm_user,
        "asin": "WARMUP",
        "reviewText": "warming up the pipeline",
        "summary": "warmup",
        "overall": 3.0,
    })
    try:
        wait_for_review(
            lambda i: i.get("userId") == warm_user and i.get("sentiment", "pending") != "pending",
            timeout=90,
        )
    except AssertionError:
        print(
            "WARNING: warm-up review did not finish within 90s -- the "
            "pipeline may be unusually slow right now (e.g. slow NLTK "
            "download); the real tests below may time out too"
        )
    finally:
        s3.delete_object(Bucket=raw_bucket(), Key=key)


def raw_bucket() -> str:
    return ssm.get_parameter(Name="/dic-reviews-app/buckets/raw")["Parameter"]["Value"]


def reviews_table():
    name = ssm.get_parameter(Name="/dic-reviews-app/tables/reviews")["Parameter"]["Value"]
    return dynamodb.Table(name)


def users_table():
    name = ssm.get_parameter(Name="/dic-reviews-app/tables/users")["Parameter"]["Value"]
    return dynamodb.Table(name)


def upload_review(review: dict) -> str:
    """Uploads a single review JSON object to the raw bucket. Returns
    the S3 key used (the canonical reviewId is assigned downstream by
    the preprocessing lambda, not here)."""
    bucket = raw_bucket()
    key = f"test-{uuid.uuid4()}.json"
    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(review).encode("utf-8"))
    return key


def wait_for_review(predicate, timeout=POLL_TIMEOUT_SECONDS) -> dict:
    """Polls the reviews table (scan -- fine at test scale) until an
    item matching `predicate` shows up, or raises on timeout. DynamoDB
    doesn't ship a built-in waiter for "item satisfying a predicate
    exists", unlike s3's object_exists waiter, so we roll our own."""
    deadline = time.time() + timeout
    table = reviews_table()
    while time.time() < deadline:
        for item in table.scan().get("Items", []):
            if predicate(item):
                return item
        time.sleep(POLL_INTERVAL_SECONDS)
    raise AssertionError(f"timed out after {timeout}s waiting for matching review")


def wait_for_sentiment(test_user: str, timeout=POLL_TIMEOUT_SECONDS) -> dict:
    """Wait for sentiment specifically, not just for the row to exist --
    profanityCheck is written synchronously by Lambda 2's put_item, but
    sentiment (and ratingSentimentAgreement) is filled in afterward,
    asynchronously, once the DynamoDB Stream fires Lambda 3. Matching on
    userId alone risks grabbing the item mid-pipeline."""
    return wait_for_review(
        lambda i: i.get("userId") == test_user and i.get("sentiment", "pending") != "pending",
        timeout=timeout,
    )


def test_clean_review_full_pipeline():
    """A polite, clearly-positive 5-star review should flow through all
    three lambdas, end up with profanityCheck='clean', sentiment
    'positive', and a rating/sentiment agreement of True (5 stars
    matches positive text)."""
    test_user = f"test-user-{uuid.uuid4()}"
    key = upload_review({
        "reviewerID": test_user,
        "asin": "TESTASIN0001",
        "reviewText": "This product is absolutely wonderful, I loved it.",
        "summary": "Great purchase",
        "overall": 5.0,
    })

    item = wait_for_sentiment(test_user)

    assert item["profanityCheck"] == "clean"
    assert item["sentiment"] == "positive"
    assert item["ratingSentimentAgreement"] is True

    s3.delete_object(Bucket=raw_bucket(), Key=key)


def test_rating_sentiment_disagreement_is_flagged():
    """A 5-star review with clearly negative text should end up with
    sentiment 'negative' but a 5-star rating -- ratingSentimentAgreement
    should be False, catching exactly the kind of sarcastic/mismatched
    review the assignment's overall-field requirement is meant to
    surface."""
    test_user = f"test-user-{uuid.uuid4()}"
    key = upload_review({
        "reviewerID": test_user,
        "asin": "TESTASIN0003",
        "reviewText": "Terrible, broken, awful experience, complete waste.",
        "summary": "Worst purchase ever",
        "overall": 5.0,
    })

    item = wait_for_sentiment(test_user)

    assert item["sentiment"] == "negative"
    assert item["ratingSentimentAgreement"] is False

    s3.delete_object(Bucket=raw_bucket(), Key=key)


def test_impolite_review_is_flagged():
    """A review containing profanity should be flagged impolite.
    "damn" is a real (and fairly mild) entry in profanityfilter's
    bundled wordlist -- see profanityfilter/data/badwords.txt -- chosen
    so the test stays readable while still tripping the filter."""
    test_user = f"test-user-{uuid.uuid4()}"
    key = upload_review({
        "reviewerID": test_user,
        "asin": "TESTASIN0002",
        "reviewText": "This is a damn awful product, total waste of money.",
        "summary": "Bad",
        "overall": 1.0,
    })

    item = wait_for_review(lambda i: i.get("userId") == test_user)
    assert item["profanityCheck"] == "impolite"

    s3.delete_object(Bucket=raw_bucket(), Key=key)


def test_user_gets_banned_after_threshold():
    """Upload 4 impolite reviews from the same user (ban threshold is
    '> 3') and confirm the user ends up banned."""
    test_user = f"test-user-{uuid.uuid4()}"
    keys = []
    for i in range(4):
        keys.append(upload_review({
            "reviewerID": test_user,
            "asin": f"TESTASIN-BAN-{i}",
            "reviewText": "This is a damn awful product, total waste of money.",
            "summary": "Bad",
            "overall": 1.0,
        }))

    wait_for_review(
        lambda i: i.get("userId") == test_user and i.get("profanityCheck") == "impolite"
    )

    deadline = time.time() + POLL_TIMEOUT_SECONDS
    table = users_table()
    user_item = None
    while time.time() < deadline:
        user_item = table.get_item(Key={"userId": test_user}).get("Item")
        if user_item and user_item.get("impoliteCount", 0) >= 4:
            break
        time.sleep(POLL_INTERVAL_SECONDS)

    assert user_item is not None, "user row was never created"
    assert user_item["impoliteCount"] >= 4
    assert user_item["banned"] is True

    for key in keys:
        s3.delete_object(Bucket=raw_bucket(), Key=key)
