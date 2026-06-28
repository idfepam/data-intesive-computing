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

ENDPOINT = "http://localhost:4566"
POLL_EVERY = 1
DEFAULT_WAIT = 30

os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
os.environ["AWS_ACCESS_KEY_ID"] = "test"
os.environ["AWS_SECRET_ACCESS_KEY"] = "test"

s3: "S3Client" = boto3.client("s3", endpoint_url=ENDPOINT)
ssm: "SSMClient" = boto3.client("ssm", endpoint_url=ENDPOINT)
awslambda: "LambdaClient" = boto3.client("lambda", endpoint_url=ENDPOINT)
dynamodb: "DynamoDBServiceResource" = boto3.resource("dynamodb", endpoint_url=ENDPOINT)

LAMBDA_NAMES = ("preprocessing", "profanity-check", "sentiment-analysis")
SSM_RAW = "/dic-reviews-app/buckets/raw"
SSM_REVIEWS = "/dic-reviews-app/tables/reviews"
SSM_USERS = "/dic-reviews-app/tables/users"


class PipelineClient:
    @staticmethod
    def raw_bucket() -> str:
        return ssm.get_parameter(Name=SSM_RAW)["Parameter"]["Value"]

    @staticmethod
    def reviews():
        name = ssm.get_parameter(Name=SSM_REVIEWS)["Parameter"]["Value"]
        return dynamodb.Table(name)

    @staticmethod
    def users():
        name = ssm.get_parameter(Name=SSM_USERS)["Parameter"]["Value"]
        return dynamodb.Table(name)

    @staticmethod
    def push_review(review: dict) -> str:
        key = f"test-{uuid.uuid4()}.json"
        s3.put_object(
            Bucket=PipelineClient.raw_bucket(),
            Key=key,
            Body=json.dumps(review).encode("utf-8"),
        )
        return key

    @staticmethod
    def remove_object(key: str) -> None:
        s3.delete_object(Bucket=PipelineClient.raw_bucket(), Key=key)


def _active_lambdas():
    for name in LAMBDA_NAMES:
        awslambda.get_waiter("function_active").wait(FunctionName=name)


@pytest.fixture(autouse=True)
def lambdas_ready():
    _active_lambdas()


@pytest.fixture(scope="session", autouse=True)
def warm_start():
    _active_lambdas()
    user = f"warmup-{uuid.uuid4()}"
    key = PipelineClient.push_review({
        "reviewerID": user,
        "asin": "WARMUP",
        "reviewText": "warming up the pipeline",
        "summary": "warmup",
        "overall": 3.0,
    })
    try:
        poll_reviews(
            lambda row: row.get("userId") == user and row.get("sentiment", "pending") != "pending",
            timeout=90,
        )
    except AssertionError:
        print("WARNING: warm-up review did not finish within 90s")
    finally:
        PipelineClient.remove_object(key)


def poll_reviews(match, timeout=DEFAULT_WAIT) -> dict:
    deadline = time.time() + timeout
    table = PipelineClient.reviews()
    while time.time() < deadline:
        for row in table.scan().get("Items", []):
            if match(row):
                return row
        time.sleep(POLL_EVERY)
    raise AssertionError(f"no matching review within {timeout}s")


def poll_sentiment(user_id: str, timeout=DEFAULT_WAIT) -> dict:
    return poll_reviews(
        lambda row: row.get("userId") == user_id and row.get("sentiment", "pending") != "pending",
        timeout=timeout,
    )


def test_clean_review_full_pipeline():
    user = f"test-user-{uuid.uuid4()}"
    key = PipelineClient.push_review({
        "reviewerID": user,
        "asin": "TESTASIN0001",
        "reviewText": "This product is absolutely wonderful, I loved it.",
        "summary": "Great purchase",
        "overall": 5.0,
    })
    row = poll_sentiment(user)
    assert row["profanityCheck"] == "clean"
    assert row["sentiment"] == "positive"
    assert row["ratingSentimentAgreement"] is True
    PipelineClient.remove_object(key)


def test_rating_sentiment_disagreement_is_flagged():
    user = f"test-user-{uuid.uuid4()}"
    key = PipelineClient.push_review({
        "reviewerID": user,
        "asin": "TESTASIN0003",
        "reviewText": "Terrible, broken, awful experience, complete waste.",
        "summary": "Worst purchase ever",
        "overall": 5.0,
    })
    row = poll_sentiment(user)
    assert row["sentiment"] == "negative"
    assert row["ratingSentimentAgreement"] is False
    PipelineClient.remove_object(key)


def test_impolite_review_is_flagged():
    user = f"test-user-{uuid.uuid4()}"
    key = PipelineClient.push_review({
        "reviewerID": user,
        "asin": "TESTASIN0002",
        "reviewText": "This is a damn awful product, total waste of money.",
        "summary": "Bad",
        "overall": 1.0,
    })
    row = poll_reviews(lambda r: r.get("userId") == user)
    assert row["profanityCheck"] == "impolite"
    PipelineClient.remove_object(key)


def test_user_gets_banned_after_threshold():
    user = f"test-user-{uuid.uuid4()}"
    keys = []
    for i in range(4):
        keys.append(PipelineClient.push_review({
            "reviewerID": user,
            "asin": f"TESTASIN-BAN-{i}",
            "reviewText": "This is a damn awful product, total waste of money.",
            "summary": "Bad",
            "overall": 1.0,
        }))

    poll_reviews(lambda r: r.get("userId") == user and r.get("profanityCheck") == "impolite")

    deadline = time.time() + DEFAULT_WAIT
    profile = None
    while time.time() < deadline:
        profile = PipelineClient.users().get_item(Key={"userId": user}).get("Item")
        if profile and profile.get("impoliteCount", 0) >= 4:
            break
        time.sleep(POLL_EVERY)

    assert profile is not None
    assert profile["impoliteCount"] >= 4
    assert profile["banned"] is True
    for key in keys:
        PipelineClient.remove_object(key)
