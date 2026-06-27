#!/usr/bin/env bash
set -euo pipefail   # fail immediately on any error, unset variable, or pipe failure
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
export MINISTACK_ENDPOINT=http://localhost:4566
AWS="aws --endpoint-url=${MINISTACK_ENDPOINT}"

GROUP_ID="${GROUP_ID:-group53}"

RAW_BUCKET="dic-reviews-${GROUP_ID}-raw"
PROCESSED_BUCKET="dic-reviews-${GROUP_ID}-processed"
REVIEWS_TABLE="dic-reviews-${GROUP_ID}-reviews"
USERS_TABLE="dic-reviews-${GROUP_ID}-users"
BAN_THRESHOLD=3

echo "[1/8] Cleaning up any existing resources..."
set +e   # cleanup errors are expected and fine -- resources may not exist yet
${AWS} s3 rb "s3://${RAW_BUCKET}" --force > /dev/null 2>&1
${AWS} s3 rb "s3://${PROCESSED_BUCKET}" --force > /dev/null 2>&1
${AWS} dynamodb delete-table --table-name "${REVIEWS_TABLE}" > /dev/null 2>&1
${AWS} dynamodb delete-table --table-name "${USERS_TABLE}" > /dev/null 2>&1
${AWS} lambda delete-function --function-name preprocessing > /dev/null 2>&1
${AWS} lambda delete-function --function-name profanity-check > /dev/null 2>&1
${AWS} lambda delete-function --function-name sentiment-analysis > /dev/null 2>&1
set -e   # back to strict mode for everything else
echo "      done."

echo "[2/8] Creating S3 buckets..."
${AWS} s3 mb "s3://${RAW_BUCKET}"
${AWS} s3 mb "s3://${PROCESSED_BUCKET}"
${AWS} ssm put-parameter --name /dic-reviews-app/buckets/raw --type "String" --value "${RAW_BUCKET}" --overwrite > /dev/null
${AWS} ssm put-parameter --name /dic-reviews-app/buckets/processed --type "String" --value "${PROCESSED_BUCKET}" --overwrite > /dev/null
echo "      done."

echo "[3/8] Creating DynamoDB tables..."
${AWS} dynamodb create-table \
 --table-name "${REVIEWS_TABLE}" \
 --attribute-definitions AttributeName=reviewId,AttributeType=S \
 --key-schema AttributeName=reviewId,KeyType=HASH \
 --billing-mode PAY_PER_REQUEST \
 --stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES > /dev/null

${AWS} dynamodb create-table \
 --table-name "${USERS_TABLE}" \
 --attribute-definitions AttributeName=userId,AttributeType=S \
 --key-schema AttributeName=userId,KeyType=HASH \
 --billing-mode PAY_PER_REQUEST > /dev/null

${AWS} ssm put-parameter --name /dic-reviews-app/tables/reviews --type "String" --value "${REVIEWS_TABLE}" --overwrite > /dev/null
${AWS} ssm put-parameter --name /dic-reviews-app/tables/users --type "String" --value "${USERS_TABLE}" --overwrite > /dev/null
${AWS} ssm put-parameter --name /dic-reviews-app/config/ban-threshold --type "String" --value "${BAN_THRESHOLD}" --overwrite > /dev/null
echo "      done."

echo "[4/8] Packaging preprocessing Lambda (downloading nltk -- may take a few minutes)..."
(
 cd lambdas/preprocessing
 rm -rf package lambda.zip
 mkdir package
 pip install -r requirements.txt -t package --platform manylinux2014_x86_64 --only-binary=:all: -q
 rm -rf package/bin
 rm -f package/handler.py
 zip -q lambda.zip handler.py
 cd package
 zip -qr ../lambda.zip *
)
echo "      done."

echo "[5/8] Deploying preprocessing Lambda..."
${AWS} lambda create-function \
 --function-name preprocessing \
 --runtime python3.11 \
 --timeout 60 \
 --memory-size 512 \
 --zip-file fileb://lambdas/preprocessing/lambda.zip \
 --handler handler.handler \
 --role arn:aws:iam::000000000000:role/lambda-role \
 --environment "{\"Variables\":{\"STAGE\":\"local\"}}" > /dev/null

PREPROCESSING_ARN=$(${AWS} lambda get-function \
 --function-name preprocessing \
 --query 'Configuration.FunctionArn' \
 --output text)
if [ -z "${PREPROCESSING_ARN}" ] || [ "${PREPROCESSING_ARN}" = "None" ]; then
  echo "ERROR: could not retrieve preprocessing Lambda ARN -- deploy likely failed above"
  exit 1
fi
echo "      done. ARN: ${PREPROCESSING_ARN}"

echo "[6/8] Packaging and deploying profanity-check Lambda..."
(
 cd lambdas/profanity_check
 rm -rf package lambda.zip
 mkdir package
 pip install -r requirements.txt -t package --platform manylinux2014_x86_64 --only-binary=:all: -q
 rm -rf package/bin
 rm -f package/handler.py
 zip -q lambda.zip handler.py
 cd package
 zip -qr ../lambda.zip *
)
${AWS} lambda create-function \
 --function-name profanity-check \
 --runtime python3.11 \
 --timeout 60 \
 --memory-size 512 \
 --zip-file fileb://lambdas/profanity_check/lambda.zip \
 --handler handler.handler \
 --role arn:aws:iam::000000000000:role/lambda-role \
 --environment "{\"Variables\":{\"STAGE\":\"local\"}}" > /dev/null

PROFANITY_ARN=$(${AWS} lambda get-function \
 --function-name profanity-check \
 --query 'Configuration.FunctionArn' \
 --output text)
if [ -z "${PROFANITY_ARN}" ] || [ "${PROFANITY_ARN}" = "None" ]; then
  echo "ERROR: could not retrieve profanity-check Lambda ARN -- deploy likely failed above"
  exit 1
fi
echo "      done. ARN: ${PROFANITY_ARN}"

echo "[7/8] Packaging and deploying sentiment-analysis Lambda..."
(
 cd lambdas/sentiment_analysis
 rm -rf package lambda.zip
 mkdir package
 pip install -r requirements.txt -t package --platform manylinux2014_x86_64 --only-binary=:all: -q
 rm -rf package/bin
 rm -f package/handler.py
 zip -q lambda.zip handler.py
 cd package
 zip -qr ../lambda.zip *
)
${AWS} lambda create-function \
 --function-name sentiment-analysis \
 --runtime python3.11 \
 --timeout 60 \
 --memory-size 512 \
 --zip-file fileb://lambdas/sentiment_analysis/lambda.zip \
 --handler handler.handler \
 --role arn:aws:iam::000000000000:role/lambda-role \
 --environment "{\"Variables\":{\"STAGE\":\"local\"}}" > /dev/null

SENTIMENT_ARN=$(${AWS} lambda get-function \
 --function-name sentiment-analysis \
 --query 'Configuration.FunctionArn' \
 --output text)
if [ -z "${SENTIMENT_ARN}" ] || [ "${SENTIMENT_ARN}" = "None" ]; then
  echo "ERROR: could not retrieve sentiment-analysis Lambda ARN -- deploy likely failed above"
  exit 1
fi
echo "      done. ARN: ${SENTIMENT_ARN}"

echo "[8/8] Wiring S3 bucket notifications..."
${AWS} s3api put-bucket-notification-configuration \
 --bucket "${RAW_BUCKET}" \
 --notification-configuration "{\"LambdaFunctionConfigurations\":
[{\"LambdaFunctionArn\": \"${PREPROCESSING_ARN}\", \"Events\":
[\"s3:ObjectCreated:*\"]}]}"

# Both profanity-check AND sentiment-analysis are triggered by the
# processed bucket -- they run in parallel on each processed object.
${AWS} s3api put-bucket-notification-configuration \
 --bucket "${PROCESSED_BUCKET}" \
 --notification-configuration "{\"LambdaFunctionConfigurations\":
[{\"LambdaFunctionArn\": \"${PROFANITY_ARN}\", \"Events\":
[\"s3:ObjectCreated:*\"]},
{\"LambdaFunctionArn\": \"${SENTIMENT_ARN}\", \"Events\":
[\"s3:ObjectCreated:*\"]}]}"
echo "      done."

echo
echo "===== All resources created successfully ====="
echo "  Raw reviews bucket:       ${RAW_BUCKET}"
echo "  Processed reviews bucket: ${PROCESSED_BUCKET}"
echo "  Reviews table:            ${REVIEWS_TABLE}"
echo "  Users table:              ${USERS_TABLE}"
echo
echo "Next steps:"
echo "  pytest tests/                    # verify the pipeline works"
echo "  ./scripts/collect_results.sh     # run the full devset"