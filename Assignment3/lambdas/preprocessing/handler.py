"""Stage 1: normalize review text and publish enriched JSON to the processed bucket."""
import json
import os
import typing
import uuid
from urllib.parse import unquote_plus

import boto3
import nltk
from nltk import pos_tag
from nltk.corpus import stopwords, wordnet
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import word_tokenize

if typing.TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_ssm import SSMClient

LOCAL_ENDPOINT = "http://localhost:4566" if os.getenv("STAGE") == "local" else None
SSM_PROCESSED_BUCKET = "/dic-reviews-app/buckets/processed"
REVIEW_TEXT_KEYS = ("summary", "reviewText")
CORPUS_DIR = "/tmp/nltk_data"
CORPORA = (
    "punkt",
    "punkt_tab",
    "stopwords",
    "wordnet",
    "omw-1.4",
    "averaged_perceptron_tagger_eng",
)

_s3: "S3Client" = boto3.client("s3", endpoint_url=LOCAL_ENDPOINT)
_ssm: "SSMClient" = boto3.client("ssm", endpoint_url=LOCAL_ENDPOINT)
_corpus_loaded = False


def _ssm_string(name: str) -> str:
    return _ssm.get_parameter(Name=name)["Parameter"]["Value"]


def _init_corpus() -> None:
    global _corpus_loaded
    if _corpus_loaded:
        return
    if CORPUS_DIR not in nltk.data.path:
        nltk.data.path.append(CORPUS_DIR)
    for resource in CORPORA:
        nltk.download(resource, download_dir=CORPUS_DIR, quiet=True)
    _corpus_loaded = True


def _treebank_to_wordnet(tag: str) -> str:
    prefix = tag[:1]
    mapping = {"J": wordnet.ADJ, "V": wordnet.VERB, "R": wordnet.ADV}
    return mapping.get(prefix, wordnet.NOUN)


class TextNormalizer:
    """Tokenize, drop stopwords, lemmatize with POS-aware WordNet tags."""

    def __init__(self) -> None:
        self._lemmatizer = WordNetLemmatizer()
        self._stop = set(stopwords.words("english"))

    def normalize(self, text: str) -> list[str]:
        output: list[str] = []
        for token, tag in pos_tag(word_tokenize(text)):
            lowered = token.lower()
            if not token.isalpha() or lowered in self._stop:
                continue
            wn_pos = _treebank_to_wordnet(tag)
            output.append(self._lemmatizer.lemmatize(lowered, pos=wn_pos))
        return output


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


def _merge_review_text(review: dict) -> str:
    parts = [str(review.get(key, "")) for key in REVIEW_TEXT_KEYS]
    return " ".join(parts)


def _assign_review_id(review: dict) -> str:
    return review.get("sourceId") or str(uuid.uuid4())


def _load_review(bucket: str, key: str) -> dict:
    payload = _s3.get_object(Bucket=bucket, Key=key)
    return json.loads(payload["Body"].read())


def _store_processed(bucket: str, review_id: str, document: dict) -> str:
    object_key = f"{review_id}.json"
    _s3.put_object(
        Bucket=bucket,
        Key=object_key,
        Body=json.dumps(document).encode("utf-8"),
        ContentType="application/json",
    )
    return object_key


def _process_record(record: dict, dest_bucket: str, normalizer: TextNormalizer) -> dict:
    src_bucket = record["s3"]["bucket"]["name"]
    src_key = unquote_plus(record["s3"]["object"]["key"])
    print(f"[preprocess] s3://{src_bucket}/{src_key}")

    review = _load_review(src_bucket, src_key)
    review_id = _assign_review_id(review)
    lemmas = normalizer.normalize(_merge_review_text(review))

    enriched = {"reviewId": review_id, **review, "tokens": lemmas}
    out_key = _store_processed(dest_bucket, review_id, enriched)
    print(f"[preprocess] wrote s3://{dest_bucket}/{out_key}")
    return {"reviewId": review_id, "key": out_key}


def handler(event, context):
    _init_corpus()
    dest_bucket = _ssm_string(SSM_PROCESSED_BUCKET)
    normalizer = TextNormalizer()
    handled = [_process_record(rec, dest_bucket, normalizer) for rec in _parse_s3_event(event)]
    return {"statusCode": 200, "processed": handled}
