import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text
import yaml
from datetime import datetime

random.seed(100)

class AWSDBConnector:
    """
    A class to manage connections to an AWS RDS MySQL database.
    The class reads credentials from a YAML file and creates a SQLAlchemy engine for database interactions.
    """

    def __init__(self):
        """Initialize the connector by reading database credentials and setting up connection parameters."""
        self.CONFIG = self.read_db_creds('db_creds.yaml')
        self.HOST = self.CONFIG["RDS_HOST"]
        self.USER = self.CONFIG["RDS_USER"]
        self.PASSWORD = self.CONFIG["RDS_PASSWORD"]
        self.DATABASE = self.CONFIG["RDS_DATABASE"]
        self.PORT = self.CONFIG["RDS_PORT"]

    def create_db_connector(self):
        """
        Create and return a SQLAlchemy engine for connecting to the AWS RDS MySQL database.

        Returns:
            engine (sqlalchemy.engine.base.Engine): SQLAlchemy engine object for database interactions.
        """
        engine = sqlalchemy.create_engine(
            f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4"
        )
        return engine

    def read_db_creds(self, file_path):
        """
        Read database credentials from a YAML file.

        Args:
            file_path (str): Path to the YAML file containing database credentials.

        Returns:
            dict: A dictionary of database credentials.
        """
        with open(file_path, 'r') as file:
            config_data = yaml.safe_load(file)
        return config_data


new_connector = AWSDBConnector()


def send_to_kafka(api_url, data):
    """
    Send data to a Kafka topic using HTTP POST requests.

    Args:
        api_url (str): The API endpoint URL for the Kafka topic.
        data (dict): The data to be sent to the Kafka topic.

    Returns:
        None
    """
    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
    payload = {
        "records": [
            {"value": data}
        ]
    }
    response = requests.post(api_url, headers=headers, data=json.dumps(payload, default=str))
    if response.status_code != 200:
        print(f"Failed to send data to {api_url}: {response.status_code} {response.text}")
    else:
        print(f"Data sent to {api_url}: {data}")


def serialize_data(data):
    """
    Convert datetime objects in the data dictionary to ISO 8601 string format for serialization.

    Args:
        data (dict): Data to be serialized, potentially containing datetime objects.

    Returns:
        dict: Serialized data with datetime objects converted to string format.
    """
    if isinstance(data, dict):
        return {k: serialize_data(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [serialize_data(i) for i in data]
    elif isinstance(data, datetime):
        return data.isoformat()
    return data


def send_to_stream_via_api_gateway(stream_name, data):
    """
    Send data to an AWS Kinesis stream via API Gateway.

    Args:
        stream_name (str): The name of the Kinesis stream.
        data (dict): The data to send to the stream.

    Returns:
        None
    """
    serialized_data = serialize_data(data)
    invoke_url = f"https://kktr24jwyi.execute-api.us-east-1.amazonaws.com/dev/streams/{stream_name}/record"

    payload = json.dumps({
        "StreamName": stream_name,
        "Data": serialized_data,
        "PartitionKey": "partition-key"
    })

    headers = {'Content-Type': 'application/json'}
    response = requests.put(invoke_url, headers=headers, data=payload)

    if response.status_code == 200:
        print(f"Data successfully sent to {stream_name} via API Gateway.")
    else:
        print(f"Failed to send data to {stream_name}: {response.status_code} {response.text}")


def send_to_stream_via_api_gateway_log(stream_name, data):
    """
    Send data to an AWS Kinesis stream via API Gateway with detailed logging.

    Args:
        stream_name (str): The name of the Kinesis stream.
        data (dict): The data to send to the stream.

    Returns:
        None
    """
    API_GATEWAY_INVOKE_URL = f"https://kktr24jwyi.execute-api.us-east-1.amazonaws.com/dev/streams/{stream_name}/record"
    try:
        print(f"Preparing to send data to stream: {stream_name}")
        serialized_data = serialize_data(data)
        print(f"Serialized Data: {json.dumps(serialized_data)}")

        payload = {
            "StreamName": stream_name,
            "Data": serialized_data,
            "PartitionKey": "partition-key"
        }

        print(f"Payload: {json.dumps(payload)}")
        headers = {'Content-Type': 'application/json'}
        response = requests.put(API_GATEWAY_INVOKE_URL, headers=headers, data=json.dumps(payload))

        print(f"API Gateway Response: {response.status_code} - {response.text}")

        if response.status_code == 200:
            print(f"Data successfully sent to {stream_name} via API Gateway.")
        else:
            print(f"Failed to send data to {stream_name}. Response: {response.status_code}, {response.text}")

    except Exception as e:
        print(f"Error sending data to {stream_name}: {e}")


def run_infinite_post_data_loop():
    """
    Continuously fetch data from AWS RDS and send it to Kinesis streams via API Gateway.

    The function selects random rows from Pinterest, geolocation, and user data tables in the database,
    and sends the data to corresponding streams.

    Returns:
        None
    """
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:
            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            for row in user_selected_row:
                user_result = dict(row._mapping)

            print(f'{pin_result} pin_result')
            print(f'{geo_result} geo_result')
            print(f'{user_result} user_result')

            send_to_stream_via_api_gateway_log('streaming-124f98f775af-pin', pin_result)
            send_to_stream_via_api_gateway_log('streaming-124f98f775af-geo', geo_result)
            send_to_stream_via_api_gateway_log('streaming-124f98f775af-user', user_result)


if __name__ == "__main__":
    """
    Main entry point of the script. It starts an infinite loop that posts data to Kinesis streams via API Gateway.
    """
    run_infinite_post_data_loop()
    print('Working')
