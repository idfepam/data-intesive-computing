from pyspark.sql import SparkSession

from pyspark.ml import Pipeline

from pyspark.ml.feature import (
    RegexTokenizer,
    StopWordsRemover,
    CountVectorizer,
    IDF,
    StringIndexer,
    ChiSqSelector,
    Normalizer
)

from pyspark.ml.classification import (
    LinearSVC,
    OneVsRest
)

from pyspark.ml.tuning import (
    ParamGridBuilder,
    CrossValidator
)

from pyspark.ml.evaluation import (
    MulticlassClassificationEvaluator
)

spark = SparkSession.builder \
    .appName("Assignment2-Part3-TextClassification") \
    .getOrCreate()


# University cluster dataset path
DATA_PATH = "hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json"

# Read JSON dataset
df = spark.read.json(DATA_PATH)

# Keep only relevant columns
df = df.select("reviewText", "category")

# Remove rows with missing values
df = df.dropna()


print("Dataset Loaded Successfully")
print("Total Reviews:", df.count())

train_df, validation_df, test_df = df.randomSplit(
    [0.7, 0.15, 0.15],
    seed=42
)

print("Train Size:", train_df.count())
print("Validation Size:", validation_df.count())
print("Test Size:", test_df.count())

# TOKENIZER
tokenizer = RegexTokenizer(
    inputCol="reviewText",
    outputCol="terms",
    pattern=r"\W+",
    toLowercase=True
)


# STOPWORD REMOVAL
remover = StopWordsRemover(
    inputCol="terms",
    outputCol="filteredTerms"
)

# COUNT VECTORIZER
count_vectorizer = CountVectorizer(
    inputCol="filteredTerms",
    outputCol="rawFeatures",
    vocabSize=20000,
    minDF=5
)

# TF-IDF
idf = IDF(
    inputCol="rawFeatures",
    outputCol="tfidfFeatures"
)

# LABEL INDEXING
label_indexer = StringIndexer(
    inputCol="category",
    outputCol="label"
)


# CHI-SQUARE FEATURE SELECTION
selector = ChiSqSelector(
    featuresCol="tfidfFeatures",
    labelCol="label",
    outputCol="selectedFeatures",
    numTopFeatures=2000
)

# L2 NORMALIZATION
normalizer = Normalizer(
    inputCol="selectedFeatures",
    outputCol="normalizedFeatures",
    p=2.0
)

# SVM
lsvc = LinearSVC(
    featuresCol="normalizedFeatures",
    labelCol="label"
)

# One-vs-Rest multiclass strategy
ovr = OneVsRest(
    classifier=lsvc,
    featuresCol="normalizedFeatures",
    labelCol="label"
)

pipeline = Pipeline(stages=[
    tokenizer,
    remover,
    count_vectorizer,
    idf,
    label_indexer,
    selector,
    normalizer,
    ovr
])

paramGrid = ParamGridBuilder() \
    .addGrid(selector.numTopFeatures, [500, 2000]) \
    .addGrid(lsvc.regParam, [0.01, 0.1, 1.0]) \
    .addGrid(lsvc.standardization, [True, False]) \
    .addGrid(lsvc.maxIter, [20, 50]) \
    .build()

print("Total Parameter Combinations:", len(paramGrid))



evaluator = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="f1"
)


crossval = CrossValidator(
    estimator=pipeline,
    estimatorParamMaps=paramGrid,
    evaluator=evaluator,
    numFolds=3,
    seed=42
)

print("Training Model...")
print("This may take some time.")

cv_model = crossval.fit(train_df)

print("Training Complete")

validation_predictions = cv_model.transform(validation_df)

validation_f1 = evaluator.evaluate(validation_predictions)
print("Validation F1 Score:", validation_f1)

test_predictions = cv_model.transform(test_df)

test_f1 = evaluator.evaluate(test_predictions)


print("Test F1 Score:", test_f1)
print("Sample Predictions")
test_predictions.select(
    "reviewText",
    "category",
    "prediction"
).show(20, truncate=80)



print("Best Model Information")
best_model = cv_model.bestModel
print(best_model)


spark.stop()