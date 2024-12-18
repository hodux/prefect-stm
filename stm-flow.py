from prefect import flow, task
import requests
from google.protobuf.json_format import MessageToDict
import gtfs_realtime_pb2
from pymongo import MongoClient
import pandas as pd

# - Mathieu
# Prefect flow to automate pushing prepared data to MongoDB every few minutes
# (Configurable in create-stm-deployment.py)
# After creating a python venv and installing the libraries from requirements.txt,
# run the flow on a prefect server with the deployment file provided. Here's how,
# - Compile .proto file -
# protoc --proto_path=. --python_out=. gtfs-realtime.proto
# - Run the prefect server -
# prefect server start
# - Create a work pool and start a worker in a separate terminal -
# prefect work-pool create --type process stm-work-pool
# prefect worker start --pool stm-work-pool
# - Create the deployment and schedule it -
# python create-stm-deployment.py
# prefect deployment run 'push_stm_data/stm-deployment'

MONGO_URI = "mongodb+srv://user:alpha123@cid-stm-test.skq19.mongodb.net/?retryWrites=true&w=majority&appName=CID-STM-test"
DB_NAME = "gtfs_cleaned"
DB_COLLECTION_VEHICLES = "vehicle_positions"
DB_COLLECTION_TRIPS = "trip_updates"
DB_COLLECTION_ETAT = "etat_service"
API_KEY = "l71ef0fae2198f46b6b4c98923f7f3d8f2"

ETAT_API_URL = "https://api.stm.info/pub/od/i3/v2/messages/etatservice"
ETAT_HEADERS = {
    "Content-Type": "application/json",
    "apiKey": API_KEY
}

# Proto routes
VEHICLES_API_URL = "https://api.stm.info/pub/od/gtfs-rt/ic/v2/vehiclePositions"
TRIPS_API_URL = "https://api.stm.info/pub/od/gtfs-rt/ic/v2/tripUpdates"
HEADERS = {
    "Content-Type": "application/x-protobuf",
    "apiKey": API_KEY
}

@task(retries=2)
def fetch_protobuf_data(api_url: str, headers: dict):
    response = requests.get(api_url, headers=headers)
    if response.status_code == 200:
        return response.content
    else:
        print(f"Failed to fetch data: {response.status_code}")

@task(retries=2)
def fetch_json_data(api_url: str, headers: dict):
    response = requests.get(api_url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch etat service data: {response.status_code}")

@task
def parse_to_dict(data: bytes):
    # Parse
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(data)

    # To Dict
    dict_data = MessageToDict(
        feed,
        including_default_value_fields=True,
        preserving_proto_field_name=True
    )
    return dict_data

@task
def clean_etat_data(data: str):
    informed_entities_df = pd.json_normalize(
        data['alerts'],
        record_path=['informed_entities'],
        meta=['cause', 'effect', ['active_periods', 'start'], ['active_periods', 'end']],
        errors='ignore'
    )

    description_texts_df = pd.json_normalize(
        data['alerts'],
        record_path=['description_texts'],
        meta=['cause', 'effect', ['active_periods', 'start'], ['active_periods', 'end']],
        errors='ignore'
    )
    description_texts_df_fr = description_texts_df[description_texts_df['language'] == 'fr']

    final_df = pd.merge(informed_entities_df,
                        description_texts_df_fr[['language', 'text']],
                        left_index=True,
                        right_index=True,
                        how='left')

    etat_output = final_df[['route_short_name', 'direction_id', 'text']]

    etat_output = etat_output.to_dict(orient="records")
    return etat_output

@task
def clean_vehicles_data(data: dict):
    vehicle_details = []
    for entity in data['entity']:
        vehicle_data = {
            'id': entity['id'],
            'vehicle_position_longitude': entity['vehicle']['position']['longitude'],
            'vehicle_position_latitude': entity['vehicle']['position']['latitude'],
            'vehicle_trip_trip_id': entity['vehicle']['trip']['trip_id'],
            'vehicle_trip_route_id': entity['vehicle']['trip']['route_id'],
            'vehicle_trip_start_date': entity['vehicle']['trip']['start_date'],
            'vehicle_current_stop_sequence': entity['vehicle']['current_stop_sequence'],
            'vehicle_current_status': entity['vehicle']['current_status'],
            'vehicle_occupancy_status': entity['vehicle']['occupancy_status']
        }
        vehicle_details.append(vehicle_data)

    vehicle_details

    return vehicle_details

@task
def clean_trips_data(data: dict):
    trip_updates_df = pd.json_normalize(
        data['entity'],
        sep='_', 
        record_path=['trip_update', 'stop_time_update'],
        meta=['id'],
        errors='ignore'
    )

    trip_details = []
    for entity in data['entity']:
        trip_data = {
            'trip_id': entity['trip_update']['trip']['trip_id'],
            'route_id': entity['trip_update']['trip']['route_id'],
            'direction_id': entity['trip_update']['trip']['direction_id'],
            'id': entity['id']
        }
        trip_details.append(trip_data)

    trip_df = pd.DataFrame(trip_details)

    final_trip_df = pd.merge(trip_updates_df, trip_df, on='id', how='left')

    trip_output_df = final_trip_df[['trip_id', 'route_id', 'stop_sequence', 
                                    'arrival_time', 'departure_time', 'stop_id', 
                                    'departure_occupancy_status']]

    trip_output_dict = trip_output_df.to_dict(orient='records')
    return trip_output_dict

@task(retries=2)
def push_to_mongo(data: dict, mongo_uri: str, db_name: str, db_collection: str) :
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[db_collection]

    if isinstance(data, list):
        collection.insert_many(data)
    else:
        collection.insert_one(data)

@flow
def push_stm_data():
    # etat_data ended up not being used, ignoring it to improve performance

    # Fetching process
    # etat_data = fetch_json_data(ETAT_API_URL, ETAT_HEADERS)
    vehicles_protobuf_data = fetch_protobuf_data(VEHICLES_API_URL, HEADERS)
    trips_protobuf_data = fetch_protobuf_data(TRIPS_API_URL, HEADERS)

    # Parsing protobuf data
    vehicles_dict_data = parse_to_dict(vehicles_protobuf_data)
    trips_dict_data = parse_to_dict(trips_protobuf_data)

    # Cleaning process
    # cleaned_etat_data = clean_etat_data(etat_data)
    cleaned_vehicles_data = clean_vehicles_data(vehicles_dict_data)
    cleaned_trips_data = clean_trips_data(trips_dict_data)

    # Pushing to mongo
    # push_to_mongo(cleaned_etat_data, MONGO_URI, DB_NAME, DB_COLLECTION_ETAT)
    push_to_mongo(cleaned_vehicles_data, MONGO_URI, DB_NAME, DB_COLLECTION_VEHICLES)
    push_to_mongo(cleaned_trips_data, MONGO_URI, DB_NAME, DB_COLLECTION_TRIPS)

if __name__ == "__main__":
    push_stm_data()
