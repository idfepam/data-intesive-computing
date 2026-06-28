"""Stage 2b: VADER sentiment scoring and rating alignment checks."""
import json
import os
import typing
from dataclasses import dataclass
from urllib.parse import unquote_plus

import boto3
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer

if typing.TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_ssm import SSMClient
    from mypy_boto3_dynamodb import DynamoDBServiceResource

LOCAL_ENDPOINT = "http://localhost:4566" if os.getenv("STAGE") == "local" else None
REGION = "us-east-1"
SSM_REVIEWS_TABLE = "/dic-reviews-app/tables/reviews"
CORPUS_DIR = "/tmp/nltk_data"
TEXT_KEYS = ("summary", "reviewText")

POSITIVE_CUTOFF = 0.05
NEGATIVE_CUTOFF = -0.05
LOW_STAR = 2
HIGH_STAR = 4

_s3: "S3Client" = boto3.client("s3", endpoint_url=LOCAL_ENDPOINT, region_name=REGION)
_ssm: "SSMClient" = boto3.client("ssm", endpoint_url=LOCAL_ENDPOINT, region_name=REGION)
_db: "DynamoDBServiceResource" = boto3.resource(
    "dynamodb", endpoint_url=LOCAL_ENDPOINT, region_name=REGION
)
_vader_ready = False


@dataclass(frozen=True)
class SentimentResult:
    label: str
    compound: float


def _bootstrap_vader() -> None:
    global _vader_ready
    if _vader_ready:
        return
    if CORPUS_DIR not in nltk.data.path:
        nltk.data.path.append(CORPUS_DIR)
    nltk.download("vader_lexicon", download_dir=CORPUS_DIR, quiet=True)
    _vader_ready = True


def _parse_s3_event(event) -> list[dict]:
    if isinstance(event, list):
        return event
    if not isinstance(event, dict):
        raise ValueError(f"unexpected event shape: {event!r}")
    if event.get("Event") == "s3:TestEvent":
        return []
    records = event.get("Records")
    if isinstance(records, list):
        return records
    if "s3" in event:
        return [event]
    raise ValueError(f"unexpected event shape: {event!r}")


def _review_text(review: dict) -> str:
    return " ".join(str(review.get(k, "")) for k in TEXT_KEYS)


def _label_from_compound(compound: float) -> str:
    if compound >= POSITIVE_CUTOFF:
        return "positive"
    if compound <= NEGATIVE_CUTOFF:
        return "negative"
    return "neutral"


def _score_text(text: str, engine: SentimentIntensityAnalyzer) -> SentimentResult:
    compound = engine.polarity_scores(text)["compound"]
    return SentimentResult(label=_label_from_compound(compound), compound=compound)


def _rating_band(overall) -> str | None:
    if overall is None:
        return None
    try:
        stars = float(overall)
    except (TypeError, ValueError):
        return None
    if stars <= LOW_STAR:
        return "negative"
    if stars >= HIGH_STAR:
        return "positive"
    return "neutral"


def _ratings_align(stars, sentiment_label: str) -> bool | None:
    expected = _rating_band(stars)
    if expected is None:
        return None
    return expected == sentiment_label


def _fetch_review(record: dict) -> dict:
    bucket = record["s3"]["bucket"]["name"]
    key = unquote_plus(record["s3"]["object"]["key"])
    print(f"[sentiment] s3://{bucket}/{key}")
    body = _s3.get_object(Bucket=bucket, Key=key)
    return json.loads(body["Body"].read())


def _persist_sentiment(table_name: str, review_id: str, result: SentimentResult, agrees) -> None:
    table = _db.Table(table_name)
    table.update_item(
        Key={"reviewId": review_id},
        UpdateExpression=(
            "SET sentiment = :lbl, sentimentScore = :scr, ratingSentimentAgreement = :agr"
        ),
        ExpressionAttributeValues={
            ":lbl": result.label,
            ":scr": str(result.compound),
            ":agr": agrees,
        },
    )


def _analyze_record(record: dict, table_name: str, engine: SentimentIntensityAnalyzer) -> dict:
    review = _fetch_review(record)
    review_id = review["reviewId"]
    result = _score_text(_review_text(review), engine)
    agrees = _ratings_align(review.get("overall"), result.label)
    _persist_sentiment(table_name, review_id, result, agrees)
    print(
        f"[sentiment] reviewId={review_id} label={result.label} "
        f"compound={result.compound:.3f} agrees={agrees}"
    )
    return {"reviewId": review_id, "sentiment": result.label}


def handler(event, context):
    _bootstrap_vader()
    reviews_table = _ssm.get_parameter(Name=SSM_REVIEWS_TABLE)["Parameter"]["Value"]
    engine = SentimentIntensityAnalyzer()
    analyzed = [_analyze_record(rec, reviews_table, engine) for rec in _parse_s3_event(event)]
    return {"statusCode": 200, "analyzed": analyzed}
