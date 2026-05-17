from pyspark import SparkContext, SparkConf
import json
import re
from heapq import nlargest

# Initialize SparkContext
conf = SparkConf().setAppName("ChiSquareRDD")
sc = SparkContext.getOrCreate(conf)

# stopwords from the file
stopwords = set([
    "a", "aa", "able", "about", "above", "absorbs", "accord", "according", "accordingly", "across",
    "actually", "after", "afterwards", "again", "against", "ain", "album", "all", "allow", "allows",
    "almost", "alone", "along", "already", "also", "although", "always", "am", "among", "amongst",
    "an", "and", "another", "any", "anybody", "anyhow", "anyone", "anything", "anyway", "anyways",
    "anywhere", "apart", "app", "appear", "appreciate", "appropriate", "are", "aren", "around", "as",
    "aside", "ask", "asking", "associated", "at", "available", "away", "awfully", "b", "baby", "bb",
    "be", "became", "because", "become", "becomes", "becoming", "been", "before", "beforehand",
    "behind", "being", "believe", "below", "beside", "besides", "best", "better", "between", "beyond",
    "bibs", "bike", "book", "books", "both", "brief", "bulbs", "but", "by", "c", "came", "camera",
    "can", "cannot", "cant", "car", "case", "cause", "causes", "cd", "certain", "certainly", "changes",
    "clearly", "co", "coffee", "com", "come", "comes", "concerning", "consequently", "consider",
    "considering", "contain", "containing", "contains", "corresponding", "could", "couldn", "course",
    "currently", "d", "definitely", "described", "despite", "did", "didn", "different", "do", "does",
    "doesn", "dog", "dogs", "doing", "doll", "don", "done", "down", "downwards", "during", "e",
    "each", "edu", "eg", "eight", "either", "else", "elsewhere", "enough", "entirely", "especially",
    "et", "etc", "even", "ever", "every", "everybody", "everyone", "everything", "everywhere", "ex",
    "exactly", "example", "except", "f", "far", "few", "fifth", "film", "first", "five", "flavor",
    "followed", "following", "follows", "for", "former", "formerly", "forth", "four", "from", "fun",
    "further", "furthermore", "g", "game", "get", "gets", "getting", "given", "gives", "go", "goes",
    "going", "gone", "got", "gotten", "greetings", "grill", "guitar", "h", "had", "hadn", "hair",
    "happens", "hardly", "has", "hasn", "have", "haven", "having", "he", "hello", "help", "hence",
    "her", "here", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "hi", "him",
    "himself", "his", "hither", "hopefully", "how", "howbeit", "however", "i", "ie", "if", "ignored",
    "immediate", "in", "inasmuch", "inc", "indeed", "indicate", "indicated", "indicates", "ink", "inner",
    "insofar", "install", "instead", "into", "inward", "is", "isn", "it", "its", "itself", "j", "just",
    "k", "keep", "keeps", "kept", "kitchen", "knife", "know", "known", "knows", "l", "lamp", "laptop",
    "last", "lately", "later", "latter", "latterly", "least", "less", "lest", "let", "life", "like",
    "liked", "likely", "little", "ll", "look", "looking", "looks", "ltd", "m", "mainly", "many", "may",
    "maybe", "me", "mean", "meanwhile", "merely", "might", "mon", "more", "moreover", "most",
    "mostly", "movie", "mower", "much", "must", "my", "myself", "n", "name", "namely", "nd", "near",
    "nearly", "necessary", "need", "needs", "neither", "never", "nevertheless", "new", "next", "nine",
    "no", "nobody", "non", "none", "noone", "nor", "normally", "not", "nothing", "novel", "now",
    "nowhere", "o", "obviously", "of", "off", "often", "oh", "ok", "okay", "old", "on", "once", "one",
    "ones", "only", "onto", "or", "other", "others", "otherwise", "ought", "our", "ours", "ourselves",
    "out", "outside", "over", "overall", "own", "p", "particular", "particularly", "per", "perhaps",
    "phone", "placed", "please", "plus", "possible", "presumably", "printer", "probably", "product",
    "provides", "q", "que", "quite", "qv", "r", "rather", "rd", "re", "read", "really", "reasonably",
    "regarding", "regardless", "regards", "relatively", "respectively", "right", "s", "said", "same",
    "saw", "say", "saying", "says", "second", "secondly", "see", "seeing", "seem", "seemed", "seeming",
    "seems", "seen", "self", "selves", "sensible", "sent", "serious", "seriously", "seven", "several",
    "shall", "shave", "she", "shoes", "should", "shouldn", "since", "six", "skin", "so", "some",
    "somebody", "somehow", "someone", "something", "sometime", "sometimes", "somewhat", "somewhere",
    "song", "songs", "soon", "sorry", "specified", "specify", "specifying", "still", "story", "strings",
    "stroller", "sub", "such", "sup", "sure", "t", "take", "taken", "taste", "tell", "tends", "th",
    "than", "thank", "thanks", "thanx", "that", "thats", "the", "their", "theirs", "them", "themselves",
    "then", "thence", "there", "thereafter", "thereby", "therefore", "therein", "theres", "thereupon",
    "these", "they", "think", "third", "this", "thorough", "thoroughly", "those", "though", "three",
    "through", "throughout", "thru", "thus", "to", "together", "too", "took", "toward", "towards",
    "toy", "tried", "tries", "truck", "truly", "try", "trying", "twice", "two", "u", "un", "under",
    "unfortunately", "unless", "unlikely", "until", "unto", "up", "upon", "us", "use", "used", "useful",
    "uses", "using", "usually", "v", "value", "various", "ve", "very", "via", "viz", "vs", "want",
    "wants", "was", "wasn", "way", "we", "wear", "welcome", "well", "went", "were", "weren", "what",
    "whatever", "when", "whence", "whenever", "where", "whereafter", "whereas", "whereby", "wherein",
    "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whoever", "whole", "whom",
    "whose", "why", "will", "willing", "wish", "with", "within", "without", "won", "wonder", "would",
    "wouldn", "x", "y", "yes", "yet", "you", "your", "yours", "yourself", "yourselves", "z", "zero"
])

# tokenization regex (same as Assignment 1)
TOKEN_SPLIT_REGEX = re.compile(r"[ \t\d\(\)\[\]\{\}\.\!\?,;:+=\-_'\"`~#@&%\€\$§\\/]+")

# load Amazon dataset
rddfile = sc.textFile("hdfs:///user/dic25_shared/amazon-reviews/full/reviews_devset.json")

# Step 0: here we calculated total documents and documents per category
parsed = rddfile.map(json.loads).cache()
total_docs = parsed.count()
docs_per_category = parsed.map(
    lambda x: (x['category'], 1)
).reduceByKey(lambda a, b: a + b).collectAsMap()


# Step 1: Token counting (document frequency, deduplicated per document)
def map_token_stats(review):
    try:
        text = review.get('reviewText', '')
        category = review.get('category')
        if not category:
            return []

        # Tokenize, lowercase, filter stopwords and single characters
        tokens = [
            t for t in TOKEN_SPLIT_REGEX.split(text.lower())
            if t and len(t) > 1 and t not in stopwords
        ]
        # Deduplicate tokens per document
        unique_tokens = set(tokens)
        # Emit counts
        results = []
        for token in unique_tokens:
            results.append((('TOKEN_CAT', (token, category)), 1))
            results.append((('TOKEN_TOTAL', token), 1))
        return results
    except Exception:
        return []

token_counts = parsed.flatMap(map_token_stats).reduceByKey(lambda a, b: a + b)

# Step 2: Restructure data for chi-square calculation

def map_prepare_for_chi(kv):

    key, value = kv
    ktype = key[0]

    if ktype == 'TOKEN_CAT':

        token, category = key[1]

        return [
            (token, {
                'total': 0,
                'categories': {
                    category: value
                }
            })
        ]

    elif ktype == 'TOKEN_TOTAL':

        token = key[1]

        return [
            (token, {
                'total': value,
                'categories': {}
            })
        ]

    return []


def merge_dicts(a, b):

    merged = {
        'total': a['total'] + b['total'],
        'categories': dict(a['categories'])
    }

    for category, count in b['categories'].items():

        merged['categories'][category] = (
            merged['categories'].get(category, 0) + count
        )

    return merged


structured_data = token_counts \
    .flatMap(map_prepare_for_chi) \
    .reduceByKey(merge_dicts)



# Step 3: Compute chi-square values
def compute_chi_square(token, stats):

    T = stats['total']
    N = total_docs

    results = []

    for category, A in stats['categories'].items():

        C = docs_per_category.get(category, 0)

        if C == 0:
            continue

        B = T - A
        C_adj = C - A
        D = N - (A + B + C_adj)

        if any(x < 0 for x in [A, B, C_adj, D]):
            continue

        row1, row2 = A + C_adj, B + D
        col1, col2 = A + B, C_adj + D

        chi = 0.0

        for obs, r, c in [
            (A, row1, col1),
            (B, row2, col1),
            (C_adj, row1, col2),
            (D, row2, col2)
        ]:

            exp = (r * c) / N if N > 0 else 0

            if exp > 0:
                chi += (obs - exp) ** 2 / exp

        results.append((category, (chi, token)))

    return results

chi_squares = structured_data.flatMap(lambda x: compute_chi_square(x[0], x[1]))

# Step 4: Select top 75 terms per category and format output
def select_top_75(category, chi_token_pairs):

    top_75 = nlargest(
        75,
        chi_token_pairs,
        key=lambda x: x[0]
    )
    return (category, top_75)

top_terms = chi_squares.groupByKey().map(
    lambda x: select_top_75(x[0], x[1])
).sortByKey()

# Collecting results and creating merged dictionary
results = top_terms.collect()

unique_terms = sorted(set(
    token
    for _, chi_token_pairs in results
    for _, token in chi_token_pairs
))

# Writing output to file
with open('output_rdd.txt', 'w') as f:
    for category, terms in results:
        formatted = ' '.join(
            f"{token}:{chi:.4f}"
            for chi, token in terms
        )
        f.write(f"{category} {formatted}\n")
    f.write(' '.join(unique_terms) + '\n')

# Stop SparkContext
sc.stop()