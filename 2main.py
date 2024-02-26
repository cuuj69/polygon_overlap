import logging
import argparse
from pymongo import MongoClient
from shapely.geometry import shape, Polygon
from shapely import errors
from multiprocessing import Pool
from shapely.errors import TopologicalError
from dotenv import load_dotenv
import time
import os
import shapely

load_dotenv()

mongodb_uri = os.getenv('MONGODB_URI')

# logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#parse polygons
def parse_polygon_records(record, field_names):
    polygons = []
    for field_name in field_names:
        try:
            geojson_data = record.get(field_name, [])
            if geojson_data:
                # construct polygon
                polygon = Polygon([tuple(coord) for coord in geojson_data[0]])
                polygons.append(polygon)
        except Exception as e:
            logger.error(f"Error parsing GeoJSON data for record {record['_id']}: {e}")
    return polygons

def log_error(message, record_id):
    try:
        print("Trying to open error_log.txt...")
        with open("error_log.txt", "a") as file:
            file.write(f"Record ID: {record_id}, Message: {message}\n")
    except Exception as e:
        logger.error(f"Error writing to error log file: {e}")


# Process each chunk
def process_chunk(args):
    chunk, field_names, overlap_threshold = args
    overlaps = []

    client = MongoClient(mongodb_uri)
    db = client['polygon_overlap']
    collection = db['polygons']

    for record1 in chunk:
        try:
            polygons1 = parse_polygon_records(record1, field_names)
            if not polygons1:
                log_error("No valid polygons found", record1['_id'])
                # update_collection with invalid record log
                collection.update_one({'_id': record1['_id']}, {'$set': {'log': 'Invalid or empty record'}})
                continue

            for record2 in collection.find():
                polygons2 = parse_polygon_records(record2, field_names)
                if not polygons2:
                    log_error("No valid polygons found", record2['_id'])
                    # update_collection with invalid record log
                    collection.update_one({'_id': record2['_id']}, {'$set': {'log': 'Invalid or empty record'}})
                    continue

                for polygon1 in polygons1:
                    for polygon2 in polygons2:
                        try:
                            # geometric operations
                            if polygon1.intersects(polygon2):
                                intersection_area = polygon1.intersection(polygon2).area
                                total_area = polygon1.area + polygon2.area - intersection_area
                                overlap_percentage = intersection_area / total_area
                                if overlap_percentage >= overlap_threshold:
                                    overlaps.append((record1['_id'], record2['_id']))

                                    # update collection with overlap log
                                    log_info = {
                                        "record_id": record1['_id'],
                                        "overlap_with": record2['_id'],
                                        "overlap_percentage": overlap_percentage
                                    }
                                    record1['log'] = log_info
                                    collection.update_one({'_id': record1['_id']}, {'$set': {'log': log_info}})
                        except (shapely.errors.TopologicalError, shapely.errors.GEOSException) as e:
                            # Log topology errors
                            log_error(f"TopologyException: {e}", record1['_id'])
                            continue  # skip
        except Exception as e:
            log_error(f"Error processing chunk: {e}", record1['_id'])
            continue  # skip

    client.close()

    return overlaps


def main():
    # db_connection
    database_name = 'polygon_overlap'
    collection_name = 'polygons'
    field_names = [str(i) for i in range(1, 21)]  # Could be more

    # optional_arguments
    parser = argparse.ArgumentParser(description="Detect overlaps between polygons in MongoDB collection")
    parser.add_argument("--overlap_threshold", type=float, default=0.5, help="Minimum overlap percentage threshold")
    parser.add_argument("--concurrency", type=int, default=1, help="Select Number of processes for concurrency")
    args = parser.parse_args()

    # get records from MongoDB
    try:
        client = MongoClient(mongodb_uri)
        db = client[database_name]
        collection = db[collection_name]
        records = list(collection.find())
    except Exception as e:
        logger.error(f"Error fetching records from MongoDB collection: {e}")
        return

    total_records = len(records)

    # split for concurrency
    records_per_process = total_records // args.concurrency
    record_chunks = [records[i:i + records_per_process] for i in range(0, total_records, records_per_process)]

    # init_start_timer
    start_time = time.time()
    processed_records = 0
    total_overlaps = 0

    # parallel_process
    with Pool(processes=args.concurrency) as pool:
        for i, chunk in enumerate(record_chunks):
            args_list = [(chunk, field_names, args.overlap_threshold)]
            results = pool.map(process_chunk, args_list)

            # counters
            processed_records += len(chunk)
            total_overlaps += sum(len(result) for result in results)

            # benchmark progress
            elapsed_time = time.time() - start_time
            time_per_record = elapsed_time / processed_records if processed_records > 0 else 0
            remaining_records = total_records - processed_records
            estimated_time_remaining = remaining_records * time_per_record if time_per_record > 0 else 0

            print(
                f"Processed {processed_records}/{total_records} records. Total Overlaps: {total_overlaps}. Elapsed Time: {elapsed_time:.2f} seconds. Estimated Time Remaining: {estimated_time_remaining:.2f} seconds")

    # 
    print("Total Overlaps:", total_overlaps)


if __name__ == "__main__":
    main()
