# Profanity-check Lambda: checks a review's text for bad words, updates
# the user's impolite-review count (banning them past the threshold),
# and writes the review's state into DynamoDB. Triggered by S3
# ObjectCreated on the processed-reviews bucket.
#
# Runs in parallel with sentiment-analysis (both triggered by the same
# processed-reviews bucket ObjectCreated event). Uses update_item with
# if_not_exists(sentiment, :pending) so it doesn't overwrite a sentiment
# value already written by sentiment-analysis if that ran first.
import json
import os
import typing
from decimal import Decimal
from urllib.parse import unquote_plus

import boto3
from profanityfilter import ProfanityFilter

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

pf = ProfanityFilter()

DEFAULT_BAN_THRESHOLD = 3
TEXT_FIELDS = ("summary", "reviewText")


def get_reviews_table_name() -> str:
    parameter = ssm.get_parameter(Name="/dic-reviews-app/tables/reviews")
    return parameter["Parameter"]["Value"]


def get_users_table_name() -> str:
    parameter = ssm.get_parameter(Name="/dic-reviews-app/tables/users")
    return parameter["Parameter"]["Value"]


def get_ban_threshold() -> int:
    try:
        parameter = ssm.get_parameter(Name="/dic-reviews-app/config/ban-threshold")
        return int(parameter["Parameter"]["Value"])
    except ssm.exceptions.ParameterNotFound:
        return DEFAULT_BAN_THRESHOLD


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


def contains_profanity(text: str) -> bool:
    return not pf.is_clean(text)


def update_user_ban_status(users_table, user_id: str, is_impolite: bool, threshold: int) -> dict:
    if not is_impolite:
        response = users_table.update_item(
            Key={"userId": user_id},
            UpdateExpression="SET impoliteCount = if_not_exists(impoliteCount, :zero), "
                              "banned = if_not_exists(banned, :false)",
            ExpressionAttributeValues={":zero": 0, ":false": False},
            ReturnValues="ALL_NEW",
        )
        return response["Attributes"]

    response = users_table.update_item(
        Key={"userId": user_id},
        UpdateExpression="SET impoliteCount = if_not_exists(impoliteCount, :zero) + :one",
        ExpressionAttributeValues={":zero": 0, ":one": 1},
        ReturnValues="ALL_NEW",
    )
    if response["Attributes"]["impoliteCount"] > threshold:
        users_table.update_item(
            Key={"userId": user_id},
            UpdateExpression="SET banned = :true",
            ExpressionAttributeValues={":true": True},
        )
        response["Attributes"]["banned"] = True
    return response["Attributes"]


def to_decimal(value):
    if value is None:
        return None
    return Decimal(str(value))


def handler(event, context):
    reviews_table = dynamodb.Table(get_reviews_table_name())
    users_table = dynamodb.Table(get_users_table_name())
    threshold = get_ban_threshold()

    results = []
    for record in iter_s3_records(event):
        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])
        print(f"profanity-checking s3://{bucket}/{key}")

        obj = s3.get_object(Bucket=bucket, Key=key)
        review = json.loads(obj["Body"].read())

        review_id = review["reviewId"]
        user_id = review.get("reviewerID", "unknown-user")
        text = " ".join(str(review.get(f, "")) for f in TEXT_FIELDS)
        is_impolite = contains_profanity(text)

        user_status = update_user_ban_status(users_table, user_id, is_impolite, threshold)

        # Use update_item (not put_item) so we don't wipe sentiment fields
        # if sentiment-analysis already ran and wrote them first.
        # if_not_exists(sentiment, :pending) only sets sentiment='pending'
        # when the field doesn't exist yet -- leaves a real value untouched.
        reviews_table.update_item(
            Key={"reviewId": review_id},
            UpdateExpression=(
                "SET userId = :uid, summary = :summary, "
                "reviewText = :reviewText, overall = :overall, "
                "profanityCheck = :pc, "
                "sentiment = if_not_exists(sentiment, :pending)"
            ),
            ExpressionAttributeValues={
                ":uid": user_id,
                ":summary": review.get("summary", ""),
                ":reviewText": review.get("reviewText", ""),
                ":overall": to_decimal(review.get("overall")),
                ":pc": "impolite" if is_impolite else "clean",
                ":pending": "pending",
            },
        )

        print(f"reviewId={review_id} user={user_id} impolite={is_impolite} "
              f"banned={user_status.get('banned', False)}")
        results.append({"reviewId": review_id, "impolite": is_impolite})

    return {"statusCode": 200, "checked": results}
