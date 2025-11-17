from pymongo import MongoClient


def main():

    client = MongoClient("mongodb://localhost:27017/")  
    db = client["YoutubeGroupDB"]       
    collection = db["videos"]    # Create/use a collection


    category = input("Enter category name: ").strip()
    t1 = int(input("Enter minimum duration (in seconds): "))
    t2 = int(input("Enter maximum duration (in seconds): "))

    query_length = {
        "category": category,
        "length": {"$gte": t1, "$lte": t2}
    }

    print(f"\nVideos in '{category}' category with duration between {t1}–{t2} seconds:\n")
    results1 = collection.find(query_length) 
    count1 = 0
    for vid in results1:
        count1 += 1
        print(f"- ID: {vid.get('id')} | Uploader: {vid.get('uploader')} | "
              f"Length: {vid.get('length')} | Views: {vid.get('views')}")
    if count1 == 0:
        print("No matching videos found.")

    # ===== RANGE QUERY 2 =====
    # Find all videos with views between [x, y]
    print("\n-----------------------------------------------")
    x = int(input("Enter minimum number of views: "))
    y = int(input("Enter maximum number of views: "))

    query_views = {"views": {"$gte": x, "$lte": y}}

    print(f"\nVideos with views between {x}–{y}:\n")
    results2 = collection.find(query_views) 
    count2 = 0
    for vid in results2:
        count2 += 1
        print(f"- ID: {vid.get('id')} | Uploader: {vid.get('uploader')} | "
              f"Views: {vid.get('views')} | Category: {vid.get('category')}")
    if count2 == 0:
        print("No matching videos found.")
        
if __name__ == "__main__":
    main()
