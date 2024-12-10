from prefect import flow, task
import requests
from google.protobuf.json_format import MessageToDict
import gtfs_realtime_pb2
from pymongo import MongoClient

# Prefect flow to automate pushing prepared data every 5 minutes into Mongo Atlas - Mathieu
# Run on a prefect server with a workflow using the deployment file provided

# - Install libraries and compile .proto file -
# pip install -U pymongo requests pandas prefect
# pip install protobuf==3.20.3
# protoc --proto_path=. --python_out=. gtfs-realtime.proto

MONGO_URI = "mongodb+srv://user:alpha123@cid-stm-test.skq19.mongodb.net/?retryWrites=true&w=majority&appName=CID-STM-test"
DB_NAME = "gtfs"
DB_COLLECTION_VEHICLES = "vehicle_positions"
DB_COLLECTION_TRIPS = "trip_updates"
DB_COLLECTION_ETAT = "etat_service"

ETAT_API_URL = "https://api.stm.info/pub/od/i3/v2/messages/etatservice"
ETAT_HEADERS = {
    "apiKey": "l71ef0fae2198f46b6b4c98923f7f3d8f2"
}

# Proto routes
VEHICLES_API_URL = "https://api.stm.info/pub/od/gtfs-rt/ic/v2/vehiclePositions"
TRIPS_API_URL = "https://api.stm.info/pub/od/gtfs-rt/ic/v2/tripUpdates"
HEADERS = {
    "Content-Type": "application/x-protobuf",
    "apiKey": "l71ef0fae2198f46b6b4c98923f7f3d8f2"
}

@task(retries=2)
def fetch_api_data(api_url: str, headers: dict):
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

# @task
# def save_protobuf(data: bytes):
#     # Write protobuf
#     with open('vehiclePositions.pb', 'wb') as f:
#         f.write(data)
#     print("Created vehiclePositions.pb")

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

@task(retries=2)
def push_to_mongo(data: dict, mongo_uri: str, db_name: str, db_collection: str) :
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[db_collection]

    result = collection.insert_one(data)
    print(f"Inserted document with ID: {result.inserted_id}")

@flow
def push_stm_data():
    vehicles_protobuf_data = fetch_api_data(VEHICLES_API_URL, HEADERS)
    trips_protobuf_data = fetch_api_data(TRIPS_API_URL, HEADERS)
    etat_data = fetch_json_data(ETAT_API_URL, ETAT_HEADERS)

    # save_protobuf(protobuf_data)
    vehicles_dict_data = parse_to_dict(vehicles_protobuf_data)
    trips_dict_data = parse_to_dict(trips_protobuf_data)

    push_to_mongo(vehicles_dict_data, MONGO_URI, DB_NAME, DB_COLLECTION_VEHICLES)
    push_to_mongo(trips_dict_data, MONGO_URI, DB_NAME, DB_COLLECTION_TRIPS)
    push_to_mongo(etat_data, MONGO_URI, DB_NAME, DB_COLLECTION_ETAT)


if __name__ == "__main__":
    push_stm_data()
