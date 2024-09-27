import requests
from time import sleep
import random
import json
import sqlalchemy
from sqlalchemy import text
import yaml

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

# API endpoints
pin_api_url = "http://ec2-3-81-136-16.compute-1.amazonaws.com:8082/topics/124f98f775af.pin"
geo_api_url = "http://ec2-3-81-136-16.compute-1.amazonaws.com:8082/topics/124f98f775af.geo"
user_api_url = "http://ec2-3-81-136-16.compute-1.amazonaws.com:8082/topics/124f98f775af.user"


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


def run_infinite_post_data_loop():
    """
    Continuously fetch data from AWS RDS and send it to Kafka topics.

    This function selects random rows from Pinterest, geolocation, and user data tables in the database,
    and sends the data to the corresponding Kafka topics.

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

            # Send data to Kafka topics
            send_to_kafka(pin_api_url, pin_result)
            send_to_kafka(geo_api_url, geo_result)
            send_to_kafka(user_api_url, user_result)


if __name__ == "__main__":
    """
    Main entry point of the script. It starts an infinite loop that posts data to Kafka topics.
    """
    run_infinite_post_data_loop()
    print('Working')
