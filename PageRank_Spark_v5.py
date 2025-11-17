import pymongo
from pymongo import MongoClient
from pyspark.sql import SparkSession, functions as F
from graphframes import GraphFrame


# ----------------- CONFIG -----------------
VIDEO_ID_FIELD  = "_id"         # node's value (video ID)
EDGES_FIELD     = "related"     # list of recommended video IDs (outgoing edges)
TITLE_FIELD     = "uploader"    # attribute to print (video title)
CONNECTION_URI  = "mongodb://localhost:27017"  # URI to connect to MongoDB client
DB_NAME         = "415_YoutubeGroupDB"         # name of DB
COLLECTION_NAME = "videos"                     # name of collection
# ------------------------------------------


def build_spark_session():

    graphframes_pkg = "io.graphframes:graphframes-spark3_2.12:0.10.0"
    mongo_pkg       = "org.mongodb.spark:mongo-spark-connector_2.12:10.5.0"

    spark = (
        SparkSession.builder
        .appName("Milestone3-GraphFrames-PageRank")
        # JVM side dependencies
        .config("spark.jars.packages", f"{graphframes_pkg},{mongo_pkg}")
        .config("spark.mongodb.read.connection.uri",  f"{CONNECTION_URI}/{DB_NAME}")
        .config("spark.mongodb.write.connection.uri", f"{CONNECTION_URI}/{DB_NAME}")
        # Give Spark more heap (tune these if needed)
        .config("spark.driver.memory", "8g")
        .config("spark.executor.memory", "8g")
        .getOrCreate()
    )

    return spark


def main():
    try:
        print("Building directed graph from DB...")

        # ---------- MongoDB client ----------
        client = MongoClient(CONNECTION_URI)
        database = client[DB_NAME]
        collection = database[COLLECTION_NAME]

        # ---------- Spark session ----------
        spark = build_spark_session()

        # ---------- Load data from Mongo ----------
        # only load the columns we actually need to save memory
        df = (
            spark.read.format("mongodb")
            .option("database", DB_NAME)
            .option("collection", COLLECTION_NAME)
            .load()
            .select(VIDEO_ID_FIELD, EDGES_FIELD)
        )
        
        print("Total videos loaded:", df.count())
        df = df.where(F.size("related") >= 5)
        print("Videos with >= 5 related:", df.count())
        
        
        # ---------- Build GraphFrame ----------

        # Vertices: one row per video, with id + title
        vertices = (
            df.select(
                F.col(VIDEO_ID_FIELD).cast("string").alias("id"),
                #F.col(TITLE_FIELD).cast("string").alias("title")
            )
            .dropDuplicates(["id"])
        )

        # Edges: explode each video's "related" array into (src, dst) rows
        edges_raw = (
            df.select(
                F.col(VIDEO_ID_FIELD).alias("src"),
                F.explode_outer(F.col(EDGES_FIELD)).alias("dst")
            )
        )
        
        edges = (
            edges_raw
            .select(
                F.col("src").cast("string").alias("src"),
                F.col("dst").cast("string").alias("dst")
            )
            .na.drop(subset=["dst"])             # drop rows with null dst
            .where(F.col("src") != F.col("dst")) # no self-loops
        )
        
        vertices = vertices.repartition(400, "id")
        edges    = edges.repartition(400, "src")
        
        g = GraphFrame(vertices, edges)

        # ---------- Run PageRank ----------
        print("Calculating PageRank (Spark GraphFrames)...")

        pr_result = g.pageRank(resetProbability=0.15, maxIter=10)
        pr_vertices = pr_result.vertices

        # Instead of caching + counting the full sorted DataFrame,
        # compute and collect only the top N rows to the driver.
        N = 500  # how many top results to cache on the driver

        topN_rows = (
            pr_vertices
            .select("id", "pagerank")
            .orderBy(F.col("pagerank").desc())
            .limit(N)
            .collect()
        )

        print(f"PageRank calculated! Cached top {N} results on the driver.")

        # ---------- Interactive Top-k Query ----------
        while True:
            print(
                "Reporting top k most influential videos. "
                "Please input k (or a value < 0 to exit): "
            )
            try:
                k = int(input())
            except Exception:
                print("Invalid input; please enter an integer (or < 0 to exit).")
                continue

            if k < 0:
                break

            if k == 0:
                print("k = 0, nothing to show.")
                continue

            if k > N:
                print(
                    f"k = {k} is larger than the precomputed top {N}. "
                    f"Showing top {N} instead."
                )
                k = N

            print(f"Top {k} most influential videos:")

            # Only use the pre-collected topN_rows; no new Spark job here
            for idx, row in enumerate(topN_rows[:k], start=1):
                videoID = row["id"]
                # get video title from MongoDB
                doc = collection.find_one({VIDEO_ID_FIELD: videoID})
                if doc:
                    videoTitle = doc.get(TITLE_FIELD, "(No title found)")
                else:
                    videoTitle = "(Not found in DB)"
                print(f"{idx}: {videoTitle} ({videoID})")

        # ---------- Clean up ----------
        client.close()
        spark.stop()

    except Exception as e:
        # Catch any Spark / Py4J errors and print a friendly message
        print(f"Error: {e}. Unable to proceed")


if __name__ == "__main__":
    main()
