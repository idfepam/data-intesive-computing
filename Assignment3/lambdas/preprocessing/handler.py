# Preprocessing Lambda: tokenizes, removes stopwords, and lemmatizes a
# single review's text fields, then writes the enriched record to the
# processed-reviews bucket. Triggered by S3 ObjectCreated on the
# raw-reviews bucket. This is the first stage in the chain --
# everything downstream (profanity-check, sentiment-analysis) is
# triggered by side effects of this Lambda or the next, never invoked
# directly.
#
# Style note: this handler is intentionally self-contained (no shared
# package between lambdas), matching how the MiniStack tutorial lambdas
# are structured -- each one is zipped and deployed independently, so
# duplicating a few small helpers (iter_s3_records, bucket lookups)
# across handlers is preferable to a cross-lambda import.
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

endpoint_url = None
if os.getenv("STAGE") == "local":
    endpoint_url = "http://localhost:4566"

s3: "S3Client" = boto3.client("s3", endpoint_url=endpoint_url)
ssm: "SSMClient" = boto3.client("ssm", endpoint_url=endpoint_url)

# Fields the assignment requires us to look at for every review (the
# third required field, "overall", is a numeric rating rather than
# free text -- it isn't tokenized here, but it IS used downstream, see
# lambdas/sentiment_analysis/handler.py's rating-vs-text-sentiment
# agreement check).
TEXT_FIELDS = ("summary", "reviewText")

# NLTK data packages this handler needs. Lambda's filesystem is
# read-only outside /tmp, so on cold start we download once into /tmp
# and point NLTK there for the lifetime of the container. For a
# production deployment you'd bundle these in a Lambda layer instead
# (faster cold starts, no runtime internet dependency) -- left as a
# follow-up, this works fine for the assignment's scope, ASSUMING your
# MiniStack Lambda sandbox actually has internet egress. If
# ensure_nltk_data() fails with a network error in your environment,
# that's the signal you need the layer approach instead.
NLTK_DATA_DIR = "/tmp/nltk_data"
NLTK_PACKAGES = [
    "punkt", "punkt_tab", "stopwords", "wordnet", "omw-1.4",
    "averaged_perceptron_tagger_eng",
]


_nltk_data_ready = False


def ensure_nltk_data():
    """Downloads needed NLTK packages to /tmp on first use per warm
    container. Deliberately does NOT pre-check with nltk.data.find():
    downloaded NLTK resources stay zipped (e.g. corpora/wordnet.zip)
    until something actually reads them, and find() reports a false
    "not found" for a zipped-but-perfectly-usable resource -- so a
    find()-then-download pattern here would silently redownload on
    every single invocation, cold or warm. nltk.download() is already
    fast and idempotent when data exists (~50ms/package, confirmed),
    so we just call it unconditionally, gated only by this in-memory
    flag so it still only happens once per container."""
    global _nltk_data_ready
    if _nltk_data_ready:
        return
    if NLTK_DATA_DIR not in nltk.data.path:
        nltk.data.path.append(NLTK_DATA_DIR)
    for package in NLTK_PACKAGES:
        nltk.download(package, download_dir=NLTK_DATA_DIR, quiet=True)
    _nltk_data_ready = True


def get_processed_bucket_name() -> str:
    parameter = ssm.get_parameter(Name="/dic-reviews-app/buckets/processed")
    return parameter["Parameter"]["Value"]


def wordnet_pos(treebank_tag: str) -> str:
    """Maps a Treebank POS tag (what nltk.pos_tag returns) to the
    one-letter WordNet POS WordNetLemmatizer expects. Defaults to NOUN
    for anything unrecognized, matching WordNetLemmatizer's own
    implicit default."""
    if treebank_tag.startswith("J"):
        return wordnet.ADJ
    if treebank_tag.startswith("V"):
        return wordnet.VERB
    if treebank_tag.startswith("R"):
        return wordnet.ADV
    return wordnet.NOUN


def preprocess_text(text: str, lemmatizer: WordNetLemmatizer) -> list[str]:
    """Tokenize -> POS-tag -> lowercase -> drop stopwords/non-alpha -> lemmatize.

    WordNetLemmatizer treats every word as a noun unless told
    otherwise, so without POS tagging "loved" stays "loved" instead of
    reducing to "love" -- tagging first (before lowercasing, since
    taggers are trained on naturally-cased text and do better with it)
    and passing each word's real part of speech into lemmatize() fixes
    that.
    """
    stop_words = set(stopwords.words("english"))
    tagged = pos_tag(word_tokenize(text))
    lemmas = []
    for tok, tag in tagged:
        tok_lower = tok.lower()
        if not tok.isalpha() or tok_lower in stop_words:
            continue
        lemmas.append(lemmatizer.lemmatize(tok_lower, pos=wordnet_pos(tag)))
    return lemmas


def iter_s3_records(event):
    """Normalizes the different shapes an S3-triggered event can take,
    including the s3:TestEvent S3 sends when a bucket notification is
    first configured (which has no Records and should just be ignored)."""
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


def handler(event, context):
    ensure_nltk_data()
    processed_bucket = get_processed_bucket_name()
    lemmatizer = WordNetLemmatizer()

    results = []
    for record in iter_s3_records(event):
        source_bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])
        print(f"preprocessing s3://{source_bucket}/{key}")

        obj = s3.get_object(Bucket=source_bucket, Key=key)
        review = json.loads(obj["Body"].read())

        # A driver script (see scripts/run_devset.py) can tag an upload
        # with "sourceId" to get a deterministic reviewId instead of a
        # random one -- this is what lets scripts/tally_results.py later
        # isolate devset-derived rows from ad-hoc/manual test uploads,
        # purely by reviewId prefix, with no other code changes needed
        # (profanity_check/handler.py already reads reviewId straight
        # off this processed object, see review["reviewId"] there).
        review_id = review.get("sourceId") or str(uuid.uuid4())
        raw_text = " ".join(str(review.get(f, "")) for f in TEXT_FIELDS)
        tokens = preprocess_text(raw_text, lemmatizer)

        enriched = {
            "reviewId": review_id,
            **review,
            "tokens": tokens,
        }

        dest_key = f"{review_id}.json"
        s3.put_object(
            Bucket=processed_bucket,
            Key=dest_key,
            Body=json.dumps(enriched).encode("utf-8"),
            ContentType="application/json",
        )
        print(f"wrote s3://{processed_bucket}/{dest_key}")
        results.append({"reviewId": review_id, "key": dest_key})

    return {"statusCode": 200, "processed": results}
