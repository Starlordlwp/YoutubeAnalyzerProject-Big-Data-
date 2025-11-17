from pymongo import MongoClient

def main():
    client = MongoClient("mongodb://localhost:27017/")
    db = client["YoutubeGroupDB"]       
    collection = db["videos"]    # Create/use a collection
    
    print("-----------------------------------------------")
    K = int(input("Enter K (how many top videos you want to see): "))

    print(f"\nTop {K} videos by total views:\n")

    pipeline = [
        {"$group": {"_id": "$id", "total_views": {"$sum": "$views"}}},
        {"$sort": {"total_views": -1}},
        {"$limit": K}
    ]

    results = collection.aggregate(pipeline)
    count = 0
    for vid in results:
        count += 1
        print(f"{count}. Video ID: {vid.get('_id')} | Total Views: {int(vid.get('total_views', 0))}")

    if count == 0:
        print("No videos found.")

if __name__ == "__main__":
    main()
