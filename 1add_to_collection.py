import os
import json
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

mongodb_uri = os.getenv('MONGODB_URI')

# print('MongoDB URI:', mongodb_uri)

def upload_json_to_mongodb(json_file, database_name, collection_name, num_records=20):
    #connect MongoDB
    client = MongoClient(mongodb_uri)
    db = client[database_name]
    collection = db[collection_name]
    
    with open(json_file, 'r') as f:
        data = json.load(f)
    
    """since we are going to query the db for the json with field names we need to have distinct but easy to hand in field names(or in range or list)"""
    #insertion of json data from sample_data.json
    field_count = 1 
    for record in data[:num_of_records]:
        
        """we use the distinct id as an indicator to get the coordinates"""
        #extraction of coordinates from nested json structure
        coordinates = record.get('q461geo',{}).get('coordinates')
        if coordinates:
            
            #insert record into collection with field name(incremented)
            collection.insert_one({str(field_count): coordinates})
            field_count += 1  
        

if __name__ == "__main__":
    json_file = 'sample_data.json'
    database_name = 'polygon_overlap'
    collection_name = 'polygons'
    num_of_records = 200 # could be more
    
    upload_json_to_mongodb(json_file, database_name, collection_name, num_of_records)
