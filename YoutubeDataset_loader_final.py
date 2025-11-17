from xml.etree.ElementTree import iterparse
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

XML_PATH   = "/home/witbit/Documents/youtube_dataset.xml"
MONGO_URI  = "mongodb://localhost:27017/"
DB_NAME    = "415_YoutubeGroupDB"
BATCH_SIZE = 500   

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
video_coll = db["videos"]
user_coll  = db["users"]


def video_elem_to_obj(elem):
    
    video_obj = {}
    
    # build a dict of the elements attributes
    for k, v in elem.attrib.items():
        
        if k == "id":
            video_obj[f"_{k}"] = v
        
        elif k == "uploader" or k == "category":
            video_obj[k] = v
        
        elif k == "age" or k == "length" or k == "views" or k == "ratings" or k == "comments" or k == "size_bytes" or k == "bitrate_kbps":    
            try:
                video_obj[k] = int(v)
                
            except (ValueError, TypeError):
                video_obj[k] = -1
        
        elif k == "rate":
            video_obj[k] = float(v)
            
    related_ids =[]
    
    # find the <related> child
    related_elem = elem.find("related")
    
    if related_elem is not None:
        # iterate its <id> children
        for id_elem in related_elem.findall("id"):
            ref = id_elem.get("ref")
            if ref is not None:
                related_ids.append(ref)

    video_obj["related"] = related_ids

    return video_obj    
        
def user_elem_to_obj(elem):
    
    user_obj = {} 
    
    # build a dict of the elements attributes
    for k, v in elem.attrib.items():
    
        if k == "id":
            user_obj[f"_{k}"] = v
            
        elif k == "uploads" or k == "watches" or k== "friends":
            try:
                user_obj[k] = int(v)
                
            except (ValueError, TypeError):
                user_obj[k] = -1
            
    return user_obj 


def stream_load(xml_path):
    video_buf, user_buf = [], []
    v_count = u_count = 0

    context = iterparse(xml_path, events=("end",))

    for event, elem in context:
        tag = elem.tag

        if tag == "video":
            doc = video_elem_to_obj(elem)
            video_buf.append(doc)
            if len(video_buf) >= BATCH_SIZE:
                try:
                    result = video_coll.insert_many(video_buf, ordered=False)
                    v_count += len(result.inserted_ids)
                except BulkWriteError as e:
                    v_count += e.details.get("nInserted", 0)
                video_buf.clear()
            elem.clear()

        elif tag == "user":
            doc = user_elem_to_obj(elem)
            user_buf.append(doc)
            if len(user_buf) >= BATCH_SIZE:
                try:
                    result = user_coll.insert_many(user_buf, ordered=False)
                    u_count += len(result.inserted_ids)
                except BulkWriteError as e:
                    u_count += e.details.get("nInserted", 0)
                user_buf.clear()
            elem.clear()

        elem.tail = None

    # final flush
    if video_buf:
        try:
            result = video_coll.insert_many(video_buf, ordered=False)
            v_count += len(result.inserted_ids)
        except BulkWriteError as e:
            v_count += e.details.get("nInserted", 0)

    if user_buf:
        try:
            result = user_coll.insert_many(user_buf, ordered=False)
            u_count += len(result.inserted_ids)
        except BulkWriteError as e:
            u_count += e.details.get("nInserted", 0)

    print(f"Done. Inserted videos={v_count}, users={u_count}")

if __name__ == "__main__":
    stream_load(XML_PATH)
