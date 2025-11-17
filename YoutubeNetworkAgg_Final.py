from functools import reduce
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from graphframes import GraphFrame


# ================================
# CONFIG — EDIT AS NEEDED
# ================================
MONGO_URI  = "mongodb://localhost:27017"
DB_NAME = "415_YoutubeGroupDB"
COLLECTION_NAME = "videos"
VIDEO_ID_FEILD = "_id"
EDGES_FIELD = "related"

RUN_PAGERANK = False         # Set False if you want to skip (PageRank can be slow on 8.6M videos)
MAX_PAGERANK_ITERS = 10
PAGERANK_RESET_PROB = 0.15


# ================================
# BASIC COUNT USING PYMONGO
# ================================
def get_basic_counts(db):
    

    videos_count = db.videos.estimated_document_count()
    users_count = db.users.estimated_document_count()

    print("=== Basic Collection Counts ===")
    print(f"Database: {DB_NAME}")
    print(f"  videos: {videos_count:,}")
    print(f"  users : {users_count:,}")
    print("----------------------------------------\n")


# ================================
# SPARK SETUP + LOADING
# ================================
def build_spark_session():
    
    graphframes_pkg = "io.graphframes:graphframes-spark3_2.12:0.10.0"
    mongo_pkg       = "org.mongodb.spark:mongo-spark-connector_2.12:10.5.0"
    
    spark = (
        SparkSession.builder
        .appName("Milestone3-GraphFrames-Netwok")
        .config("spark.jars.packages", f"{graphframes_pkg},{mongo_pkg}")
        .config("spark.mongodb.read.connection.uri",  f"{MONGO_URI}/{DB_NAME}")
        .config("spark.mongodb.write.connection.uri", f"{MONGO_URI}/{DB_NAME}")
        .config("spark.driver.memory", "8g")
        .config("spark.executor.memory", "8g")
        .getOrCreate()
    )

    return spark
    
    
def load_videos_df(spark):
    df = (
        spark.read.format("mongodb")
        .option("database", DB_NAME)
        .option("collection", COLLECTION_NAME)
        .load()
    )
    # Ensure numeric types
    df = (
        df
        .withColumn("id", F.col(VIDEO_ID_FEILD).cast("string"))
        .withColumn("views", F.col("views").cast(T.LongType()))
        .withColumn("length", F.col("length").cast(T.IntegerType()))
        .withColumn("rate", F.col("rate").cast(T.DoubleType()))
        .withColumn("ratings", F.col("ratings").cast(T.LongType()))
        .withColumn("comments", F.col("comments").cast(T.LongType()))
        .withColumn("related", F.col("related").cast(T.ArrayType(T.StringType())))
    )

    return df.cache()


# ================================
# GRAPHFRAME BUILD
# ================================
def build_graph(df):
    vertices = df.select(
        F.col("id").alias("id"),
        "category",
        "views",
        "length"
    )

    edges = (
        df
        .select(
            F.col(VIDEO_ID_FEILD).alias("src"),
            F.explode_outer(F.col(EDGES_FIELD)).alias("dst")
        )
        .where(F.col("dst").isNotNull())
        .where(F.col("src") != F.col("dst"))
    )

    return GraphFrame(vertices, edges)


# ================================
# GRAPH STATISTICS
# ================================
def degree_statistics_graphframes(g):
    print("=== Degree Statistics (GraphFrames) ===")

    out_deg = g.outDegrees
    in_deg = g.inDegrees

    print("\n-- Out-degree distribution --")
    out_deg.groupBy("outDegree").count().orderBy("outDegree").show(20, False)

    print("\n-- In-degree distribution --")
    in_deg.groupBy("inDegree").count().orderBy("inDegree").show(20, False)

    print("\nTop 10 by out-degree:")
    (
        g.vertices
        .join(out_deg, "id", "left")
        .fillna({"outDegree": 0})
        .orderBy(F.desc("outDegree"))
        .select("id", "category", "views", "outDegree")
        .show(10, False)
    )

    print("\nTop 10 by in-degree:")
    (
        g.vertices
        .join(in_deg, "id", "left")
        .fillna({"inDegree": 0})
        .orderBy(F.desc("inDegree"))
        .select("id", "category", "views", "inDegree")
        .show(10, False)
    )

    print("----------------------------------------\n")


def pagerank_statistics(g):
    print("=== PageRank (GraphFrames) ===")
    print(f"resetProb={PAGERANK_RESET_PROB}, maxIter={MAX_PAGERANK_ITERS}")

    pr = g.pageRank(
        resetProbability=PAGERANK_RESET_PROB,
        maxIter=MAX_PAGERANK_ITERS
    )

    pr.vertices.select("id", "pagerank", "category", "views").orderBy(
        F.desc("pagerank")
    ).show(20, False)

    print("----------------------------------------\n")


# ================================
# CATEGORY AND NUMERIC STATS
# ================================
def categorized_statistics(df):
    print("=== Categorized Statistics ===")

    stats = (
        df.groupBy("category")
        .agg(
            F.count("*").alias("num_videos"),
            F.avg("views").alias("avg_views"),
            F.max("views").alias("max_views"),
            F.avg("length").alias("avg_length"),
            F.avg("rate").alias("avg_rating")
        )
        .orderBy(F.desc("num_videos"))
    )

    stats.show(50, False)
    print("----------------------------------------\n")


# ================================
# VIDEO LENGTH/SIZE DISTRIBUTION
# ================================
def video_size_buckets(df):
    print("=== Video Size Buckets ===")

    bucketed = (
        df.withColumn(
            "length_bucket",
            F.when(F.col("length") < 240, "short (<4 min)")
             .when(F.col("length") < 1200, "medium (4–20 min)")
             .otherwise("long (>=20 min)")
        )
        .groupBy("length_bucket")
        .agg(
            F.count("*").alias("num_videos"),
            F.avg("views").alias("avg_views"),
            F.max("views").alias("max_views"),
        )
        .orderBy("length_bucket")
    )

    bucketed.show(20, False)
    print("----------------------------------------\n")


# ================================
# VIEW COUNTS
# ================================
def view_count_statistics(df):
    print("=== View Count Statistics ===")

    stats = df.agg(
        F.count("*").alias("num_videos"),
        F.avg("views").alias("avg_views"),
        F.stddev("views").alias("stddev_views"),
        F.expr("percentile_approx(views, 0.5)").alias("median_views"),
        F.expr("percentile_approx(views, 0.9)").alias("90_percentile_views"),
        F.max("views").alias("max_views")
    )

    stats.show(20,False)

    print("\nView Buckets:")
    (
        df.withColumn(
            "number_of_views_bucket",
            F.when(F.col("views") < 1_000, "<1k")
             .when(F.col("views") < 10_000, "1k–10k")
             .when(F.col("views") < 100_000, "10k–100k")
             .when(F.col("views") < 1_000_000, "100k–1M")
             .otherwise(">=1M")
        )
        .groupBy("number_of_views_bucket")
        .count()
        .orderBy("number_of_views_bucket")
        .show(20, False)
    )

    print("----------------------------------------\n")


# ================================
# FREQUENCY OF SEARCH CONDITIONS
# ================================
def frequency_by_search_condition(df):
    print("=== Frequency by Search Condition ===")

    category = input("Category (exact name or blank for ALL): ").strip()
    min_len = input("Minimum length (blank=none): ").strip()
    max_len = input("Maximum length (blank=none): ").strip()
    min_views = input("Minimum views (blank=none): ").strip()

    conds = []

    if category:
        conds.append(F.col("category") == category)

    if min_len:
        conds.append(F.col("length") >= int(min_len))

    if max_len:
        conds.append(F.col("length") <= int(max_len))

    if min_views:
        conds.append(F.col("views") >= int(min_views))

    if conds:
        cond = reduce(lambda a, b: a & b, conds)
        filtered = df.where(cond)
    else:
        filtered = df

    total = df.count()
    hits = filtered.count()

    print(f"Total videos: {total:,}")
    print(f"Matching videos: {hits:,}\n")

    if hits > 0:
        filtered.select("id", "category", "views", "length")\
            .orderBy(F.desc("views"))\
            .show(20, False)

    print("----------------------------------------\n")


# ================================
# MAIN
# ================================
def main():
    #0) Open Mongo and create client
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    
    
    # 1) Generate count of documents in each collection.
    get_basic_counts(db)

    # 2) Build spark session and create dataframe
    spark = build_spark_session()
    df = load_videos_df(spark)

    # 3) Use graphframes to build graph that can be used by other methods
    graph = build_graph(df)
    degree_statistics_graphframes(graph)

    # 4) Generate catagory stats, video length information and view count information
    categorized_statistics(df)
    video_size_buckets(df)
    view_count_statistics(df)

    # 5) User driven search that yeilds the frequency of that search condition
    frequency_by_search_condition(df)

    spark.stop()


if __name__ == "__main__":
    main()

