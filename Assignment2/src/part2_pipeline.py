from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer, IDF, StringIndexer, ChiSqSelector


spark = SparkSession.builder.appName("Assignment2_Part2").getOrCreate()

# read input data
df = spark.read.json("../data/reviews_devset.json")
df = df.select("category", "reviewText").na.drop()

print("Rows:", df.count())

# split review text into words
tokenizer = RegexTokenizer(
    inputCol="reviewText",
    outputCol="terms",
    pattern=r"[\s\d()\[\]{}.!?,;:+=\-_\"'`~#@&*%€$§\\/]+",
    gaps=True,
    toLowercase=True,
    minTokenLength=2
)

# remove stopwords
remover = StopWordsRemover(
    inputCol="terms",
    outputCol="filteredTerms"
)

# create term-frequency vectors
count_vectorizer = CountVectorizer(
    inputCol="filteredTerms",
    outputCol="features"
)

# apply IDF weighting
idf = IDF(
    inputCol="features",
    outputCol="weightedFeatures"
)

# convert category names to numeric labels
label_indexer = StringIndexer(
    inputCol="category",
    outputCol="categoryIndex"
)

# select 2000 best features with chi-square
selector = ChiSqSelector(
    numTopFeatures=2000,
    featuresCol="weightedFeatures",
    labelCol="categoryIndex",
    outputCol="selectedFeatures"
)

pipeline = Pipeline(stages=[
    tokenizer,
    remover,
    count_vectorizer,
    idf,
    label_indexer,
    selector
])

print("Fitting pipeline...")
model = pipeline.fit(df)

# get selected terms
vocab = model.stages[2].vocabulary
selected_indices = model.stages[5].selectedFeatures

selected_terms = []
for i in selected_indices:
    selected_terms.append(vocab[i])

selected_terms = sorted(selected_terms)

# write output file
with open("../output_ds.txt", "w", encoding="utf-8") as f:
    for term in selected_terms:
        f.write(term + "\n")

print("Finally, it is done! :)")