import argparse
from dotenv import load_dotenv
import os 
from pymongo import MongoClient
from shapely.geometry import shape, Polygon
from multiprocessing import Pool
import time


load_dotenv()

mongodb_uri = os.getenv('MONGODB_URI')

def parse_polygon_records(record, field_names):
    polygons = []
    for field_name in field_names:
        try:
            geojson_data = record.get(field_name, [])
            if geojson_data:
                polygon = Polygon([tuple(coord) for coord in geojson_data[0]])
                polygons.append(polygon)
        except Exception as e:
            print(f"Error parsing GeoJSON data for record {record['_id']}: {e}")
            
    return polygons

def process_chunk(args):
    chunk, field_names, overlap_threshold = args
    overlaps = []
    
    client = MongoClient(mongodb_uri)
    db = client['polygon_overlap']
    collection = db['polygons']
    
    for record1 in chunk:
        polygons1 = parse_polygon_records(record1, field_names)
        for record2 in collection.find():
            polygons2 = parse_polygon_records(record2, field_names)
            for polygon1 in polygons1:
                for polygon2 in polygons2:
                    if polygon1.intersects(polygon2):
                        intersection_area = polygon1.intersection(polygon2).area
                        total_area = polygon1.area + polygon2.area - intersection_area
                        overlap_percentage = intersection_area / total_area
                        if overlap_percentage >= overlap_threshold:
                            overlaps.append((record1['_id'], record2['_id']))
                            
                            
                            #update collection with overlap log
                            log_info = {
                                "record_id": record1['_id'],
                                "overlap_with": record2['_id'],
                                "overlap_percentage": overlap_percentage
                            }
                            record1['log'] = log_info
                            
                            collection.update_one({'_id': record1['_id']}, {'$set': {'log': log_info}})
    
    client.close()
    
    return overlaps

def main():
    database_name = 'polygon_overlap'
    collection_name = 'polygons'
    field_names = [str(i) for i in range(1, 201)]  #could be more
    
    #optional_arguments
    parser = argparse.ArgumentParser(description="Detect overlaps between polygons in MongoDB collection")
    parser.add_argument("--overlap_threshold", type=float, default=0.5, help="Minimum overlap percentage threshold")
    parser.add_argument("--concurrency", type=int, default=1, help="Select Number of processes for concurrency")
    args = parser.parse_args()
    
    #get records
    try:
        client = MongoClient(mongodb_uri)
        db = client[database_name]
        collection = db[collection_name]
        records = list(collection.find())
    except Exception as e:
        print(f"Error fetching records from MongoDB collection: {e}")
        return
    
    total_records = len(records)
    
    
    #number of records per process
    records_per_process = total_records // args.concurrency
    
    #chunks
    record_chunks = [records[i:i+records_per_process] for i in range(0, total_records, records_per_process)]
    
    #progress timer
    start_time = time.time()
    processed_records = 0
    total_overlaps = 0
    
    
    #parallel_process
    with Pool(processes=args.concurrency) as pool:
        for i, chunk in enumerate(record_chunks):
            args_list = [(chunk, field_names, args.overlap_threshold)]
            results = pool.map(process_chunk, args_list)
            
            processed_records += len(chunk)
            total_overlaps += sum(len(result) for result in results)
            elapsed_time = time.time() - start_time
            time_per_record = elapsed_time / processed_records if processed_records > 0 else 0
            remaining_records = total_records - processed_records
            estimated_time_remaining = remaining_records * time_per_record if time_per_record > 0 else 0
            
            print(f"Processed {processed_records}/{total_records} records. Total Overlaps: {total_overlaps}. Elapsed Time: {elapsed_time:.2f} seconds. Estimated Time Remaining: {estimated_time_remaining:.2f} seconds")
    
    print("Total Overlaps:", total_overlaps)
    
if __name__ == "__main__":
    main()
