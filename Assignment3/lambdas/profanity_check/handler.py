"""Stage 2a: profanity screening, review persistence, and user moderation."""
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

LOCAL_ENDPOINT = "http://localhost:4566" if os.getenv("STAGE") == "local" else None
REGION = "us-east-1"

SSM_REVIEWS_TABLE = "/dic-reviews-app/tables/reviews"
SSM_USERS_TABLE = "/dic-reviews-app/tables/users"
SSM_BAN_LIMIT = "/dic-reviews-app/config/ban-threshold"
FALLBACK_BAN_LIMIT = 3
TEXT_KEYS = ("summary", "reviewText")

_s3: "S3Client" = boto3.client("s3", endpoint_url=LOCAL_ENDPOINT, region_name=REGION)
_ssm: "SSMClient" = boto3.client("ssm", endpoint_url=LOCAL_ENDPOINT, region_name=REGION)
_db: "DynamoDBServiceResource" = boto3.resource(
    "dynamodb", endpoint_url=LOCAL_ENDPOINT, region_name=REGION
)
_filter = ProfanityFilter()


class AppConfig:
    def __init__(self) -> None:
        self.reviews_table = _ssm.get_parameter(Name=SSM_REVIEWS_TABLE)["Parameter"]["Value"]
        self.users_table = _ssm.get_parameter(Name=SSM_USERS_TABLE)["Parameter"]["Value"]
        self.ban_limit = self._read_ban_limit()

    def _read_ban_limit(self) -> int:
        try:
            raw = _ssm.get_parameter(Name=SSM_BAN_LIMIT)["Parameter"]["Value"]
            return int(raw)
        except _ssm.exceptions.ParameterNotFound:
            return FALLBACK_BAN_LIMIT


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


def _combined_text(review: dict) -> str:
    return " ".join(str(review.get(k, "")) for k in TEXT_KEYS)


def _as_decimal(value):
    return None if value is None else Decimal(str(value))


def _vulgarity_label(is_vulgar: bool) -> str:
    return "impolite" if is_vulgar else "clean"


class UserModeration:
    def __init__(self, table, ban_limit: int) -> None:
        self._table = table
        self._ban_limit = ban_limit

    def record_review(self, user_id: str, vulgar: bool) -> dict:
        if not vulgar:
            resp = self._table.update_item(
                Key={"userId": user_id},
                UpdateExpression=(
                    "SET impoliteCount = if_not_exists(impoliteCount, :z), "
                    "banned = if_not_exists(banned, :f)"
                ),
                ExpressionAttributeValues={":z": 0, ":f": False},
                ReturnValues="ALL_NEW",
            )
            return resp["Attributes"]

        resp = self._table.update_item(
            Key={"userId": user_id},
            UpdateExpression="SET impoliteCount = if_not_exists(impoliteCount, :z) + :inc",
            ExpressionAttributeValues={":z": 0, ":inc": 1},
            ReturnValues="ALL_NEW",
        )
        attrs = resp["Attributes"]
        if attrs["impoliteCount"] > self._ban_limit:
            self._table.update_item(
                Key={"userId": user_id},
                UpdateExpression="SET banned = :t",
                ExpressionAttributeValues={":t": True},
            )
            attrs["banned"] = True
        return attrs


class ReviewStore:
    def __init__(self, table) -> None:
        self._table = table

    def upsert_moderation(self, review: dict, user_id: str, vulgar: bool) -> None:
        self._table.update_item(
            Key={"reviewId": review["reviewId"]},
            UpdateExpression=(
                "SET userId = :uid, summary = :s, reviewText = :rt, overall = :ov, "
                "profanityCheck = :pc, sentiment = if_not_exists(sentiment, :pend)"
            ),
            ExpressionAttributeValues={
                ":uid": user_id,
                ":s": review.get("summary", ""),
                ":rt": review.get("reviewText", ""),
                ":ov": _as_decimal(review.get("overall")),
                ":pc": _vulgarity_label(vulgar),
                ":pend": "pending",
            },
        )


def _fetch_review(record: dict) -> dict:
    bucket = record["s3"]["bucket"]["name"]
    key = unquote_plus(record["s3"]["object"]["key"])
    print(f"[moderation] s3://{bucket}/{key}")
    body = _s3.get_object(Bucket=bucket, Key=key)
    return json.loads(body["Body"].read())


def _handle_record(record: dict, cfg: AppConfig) -> dict:
    review = _fetch_review(record)
    review_id = review["reviewId"]
    user_id = review.get("reviewerID", "unknown-user")
    vulgar = not _filter.is_clean(_combined_text(review))

    users = UserModeration(_db.Table(cfg.users_table), cfg.ban_limit)
    user_state = users.record_review(user_id, vulgar)

    ReviewStore(_db.Table(cfg.reviews_table)).upsert_moderation(review, user_id, vulgar)

    print(
        f"[moderation] reviewId={review_id} user={user_id} "
        f"vulgar={vulgar} banned={user_state.get('banned', False)}"
    )
    return {"reviewId": review_id, "impolite": vulgar}


def handler(event, context):
    cfg = AppConfig()
    outcomes = [_handle_record(rec, cfg) for rec in _parse_s3_event(event)]
    return {"statusCode": 200, "checked": outcomes}
