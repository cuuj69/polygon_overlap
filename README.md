# Polygon Overlap Detection

This Python script detects overlaps between polygons stored in a MongoDB collection. It uses the Shapely library to perform geometric calculations and multiprocessing to improve performance.

## Setup Instructions

1. Clone this repository to your local machine.
git clone https://github.com/cuuj69/polygon_overlap.git

2. Navigate to the project directory.
cd polygon-overlap

3. Install dependencies using pip.
pip install -r requirements.txt

4. Create a `.env` file in the project root directory and set the `MONGODB_URI` variable to your MongoDB connection string.
MONGODB_URI=your_mongodb_connection_string


## Running the Script

To run the script, open a terminal or command prompt and execute the following command:

python main.py [--overlap-threshold THRESHOLD] [--concurrency CONCURRENCY]


### Optional Arguments:

- `--overlap-threshold`: Specifies the minimum overlap percentage threshold (default is 0.5).
- `--concurrency`: Specifies the number of processes for concurrency (default is 1).

## Understanding Output

The script will output progress information to the console as it processes records and detects overlaps. The output includes the following details:

- **Processed Records**: Number of records processed out of the total.
- **Total Overlaps**: Number of overlaps detected.
- **Elapsed Time**: Time elapsed since the script started.
- **Estimated Time Remaining**: Estimated time remaining based on the current processing speed.

Additionally, any errors encountered during processing, such as self-intersecting polygons or invalid records, will be logged to an `error_log.txt` file. Each entry in the error log contains the record ID and a message describing the error.

The script also updates the MongoDB database with overlap information. Each record in the collection will have a `log` field containing details about overlaps detected, including the record ID, ID of the overlapping record, and the overlap percentage.

"log": {
"record_id": "record1_id",
"overlap_with": "record2_id",
"overlap_percentage": 0.75
}

## Author

- William Jefferson Mensah
- GitHub: [William Jefferson Mensah](https://github.com/cuuj69)


## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.