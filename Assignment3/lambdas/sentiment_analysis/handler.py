# Sentiment-analysis Lambda: runs NLTK VADER sentiment analysis on a
# review and writes the result into DynamoDB.
#
# Trigger: S3 ObjectCreated on the PROCESSED REVIEWS bucket -- the same
# trigger as profanity-check, so both run in parallel on each processed
# object rather than sentiment-analysis waiting on the DynamoDB Stream.
#
# This is an architectural change from the original design (which used
# a DynamoDB Stream INSERT trigger) made to avoid MiniStack's stream
# backlog under high load. The assignment spec allows S3 triggers:
# "Other function invocations must be triggered by S3 buckets and/or
# DynamoDB events". Both functions are now triggered by S3.
#
# Race condition: profanity-check writes to the same DynamoDB item.
# Either lambda can run first -- profanity-check uses if_not_exists for
# the sentiment field so it never overwrites a real sentiment value with
# 'pending', and this handler uses update_item so it never overwrites
# the profanityCheck field either.
import json
import os
import typing
from urllib.parse import unquote_plus

import boto3
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer

if typing.TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_ssm import SSMClient
    from mypy_boto3_dynamodb import DynamoDBServiceResource

endpoint_url = None
if os.getenv("STAGE") == "local":
    endpoint_url = "http://localhost:4566"

s3: "S3Client" = boto3.client("s3", endpoint_url=endpoint_url,
                               region_name="us-east-1")
ssm: "SSMClient" = boto3.client("ssm", endpoint_url=endpoint_url,
                                 region_name="us-east-1")
dynamodb: "DynamoDBServiceResource" = boto3.resource("dynamodb",
                                                      endpoint_url=endpoint_url,
                                                      region_name="us-east-1")

NLTK_DATA_DIR = "/tmp/nltk_data"
LOW_RATING_CUTOFF = 2
HIGH_RATING_CUTOFF = 4

_nltk_data_ready = False


def ensure_nltk_data():
    global _nltk_data_ready
    if _nltk_data_ready:
        return
    if NLTK_DATA_DIR not in nltk.data.path:
        nltk.data.path.append(NLTK_DATA_DIR)
    nltk.download("vader_lexicon", download_dir=NLTK_DATA_DIR, quiet=True)
    _nltk_data_ready = True


def get_reviews_table_name() -> str:
    parameter = ssm.get_parameter(Name="/dic-reviews-app/tables/reviews")
    return parameter["Parameter"]["Value"]


def iter_s3_records(event):
    if isinstance(event, dict):
        if event.get("Event") == "s3:TestEvent":
            return []
        if isinstance(event.get("Records"), list):
            return event["Records"]
        if "s3" in event:
            return [event]
    if isinstance(event, list):
        return event
    raise ValueError(f"unsupported S3 event payload: {event!r}")


def classify_sentiment(text: str, analyzer: SentimentIntensityAnalyzer) -> dict:
    scores = analyzer.polarity_scores(text)
    compound = scores["compound"]
    if compound >= 0.05:
        label = "positive"
    elif compound <= -0.05:
        label = "negative"
    else:
        label = "neutral"
    return {"label": label, "score": compound}


def rating_expected_label(overall) -> str | None:
    if overall is None:
        return None
    try:
        overall = float(overall)
    except (TypeError, ValueError):
        return None
    if overall <= LOW_RATING_CUTOFF:
        return "negative"
    if overall >= HIGH_RATING_CUTOFF:
        return "positive"
    return "neutral"


def handler(event, context):
    ensure_nltk_data()
    reviews_table = dynamodb.Table(get_reviews_table_name())
    analyzer = SentimentIntensityAnalyzer()

    results = []
    for record in iter_s3_records(event):
        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])
        print(f"sentiment-analyzing s3://{bucket}/{key}")

        obj = s3.get_object(Bucket=bucket, Key=key)
        review = json.loads(obj["Body"].read())

        review_id = review["reviewId"]
        text = " ".join(str(review.get(f, "")) for f in ("summary", "reviewText"))
        overall = review.get("overall")

        sentiment = classify_sentiment(text, analyzer)
        expected_label = rating_expected_label(overall)
        rating_agrees = (
            (expected_label == sentiment["label"]) if expected_label is not None else None
        )

        # update_item creates the item if it doesn't exist yet, or
        # adds only these fields if profanity-check already created it.
        reviews_table.update_item(
            Key={"reviewId": review_id},
            UpdateExpression=(
                "SET sentiment = :label, sentimentScore = :score, "
                "ratingSentimentAgreement = :agrees"
            ),
            ExpressionAttributeValues={
                ":label": sentiment["label"],
                ":score": str(sentiment["score"]),
                ":agrees": rating_agrees,
            },
        )

        print(f"reviewId={review_id} sentiment={sentiment['label']} "
              f"score={sentiment['score']:.3f} ratingAgrees={rating_agrees}")
        results.append({"reviewId": review_id, "sentiment": sentiment["label"]})

    return {"statusCode": 200, "analyzed": results}
