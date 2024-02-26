import random
import json
import requests
import sqlalchemy
import yaml
from time import sleep
from sqlalchemy import create_engine, text

random.seed(100)

class AWSDBConnector:
    '''
    AWSDBConnector - a class for connecting to AWS RDS database.
    Contains methods for reading in database credentials (yaml) files and initiating an engine connector.
    '''
    def read_db_creds(self, filename: str):
        '''
        Method to read in the AWS RDS or local database credentials yaml file, located in the 'Credentials' directory.
        
        Args:
            filename (str): Name of the yaml file containing connection information to AWS RDS database located in 'Credentials' directory
        
        Returns:
            dict: Dictionary of AWS RDS or local database credentials
        '''
        with open(f'Credentials/{filename}.yaml', 'r') as f:
            db_creds = yaml.safe_load(f)
        return db_creds
    
    def create_db_connector(self, creds: dict):
        '''
        Method to create a connection to the AWS RDS database with sqlalchemy engine connector.
        
        Args:
            creds (dict): Dictionary of database credentials
        
        Returns:
            engine: Connected sqlalchemy engine
        '''
        engine = create_engine(f"mysql+pymysql://{creds['USER']}:{creds['PASSWORD']}@{creds['HOST']}:{creds['PORT']}/{creds['DATABASE']}?charset=utf8mb4")
        return engine
    

# Creating instances of class
new_connector = AWSDBConnector()
creds = new_connector.read_db_creds("pinterest_creds.yaml")
RDS_connector = new_connector.create_db_connector(creds)


# Pinterest data from API to Kafka topics in S3 buckets
def send_to_kafka_topic(url, key, data, headers):
    '''
    Sends the data to the specified Kafka topics.
    
    Args:
    url (str): The URL of the Kafka topic directory in AWS S3 bucket.
    key (str): The key of the Kafka topic.
    data (dict): The data to be sent.
    headers (dict): The headers for the request.
    '''
    payload = json.dumps({"records": [{"value": data}]}, default=str)
    response = requests.post(f"{url}{key}", data=payload, headers=headers)
    if response.status_code == 200:
        print(f"Message successfully sent to Kafka {key} topic.")
    else:
        print(f"Failed to send message to Kafka {key} topic. Status code:", response.status_code)

def Pinterest_data_kafka_data_batch():
    '''
    Fetches data from different tables in the database and sends it to different Kafka topics.
    '''
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = RDS_connector
        url = "https://nlhg5rjpwj.execute-api.us-east-1.amazonaws.com/test/topics/"
        headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
        tables = {
            "pinterest_data": "124714cdee67.pin",
            "geolocation_data": "124714cdee67.geo",
            "user_data": "124714cdee67.user"
        }
        
        with engine.connect() as connection:
            for table, key in tables.items():
                data_string = text(f"SELECT * FROM {table} LIMIT {random_row}, 1")
                selected_row = connection.execute(data_string)
                for row in selected_row:
                    result = dict(row._mapping)
                    print(result)
                    send_to_kafka_topic(url, key, result, headers)


# Pinterest data from API to databricks with AWS Kinesis
                    
def send_record_to_kinesis(stream_name, data, url, headers):
    '''
    Sends a record to Kinesis data stream.

    Args:
        stream_name (str): The name of the Kinesis stream.
        data (dict): The data to be sent to the stream.
        url (str): The URL for the Kinesis stream.
        headers (dict): The headers for the HTTP request.

    Returns:
        None
    '''
    payload = json.dumps({
        "StreamName": stream_name,
        "Data": data,
        "PartitionKey": "test"
    }, default=str)
    response = requests.request("PUT", f"{url}{stream_name}/record", headers=headers, data=payload)
    status = "successfully" if response.status_code == 200 else f"Failed. Status code: {response.status_code}"
    print(f"Record {status} sent to Kinesis {stream_name} Stream.")
    if response.status_code != 200:
        sleep(1)

def get_random_row(table_name, random_row, connection):
    '''
    Retrieves a random row from a given table.

    Args:
        table_name (str): The name of the table to retrieve the row from.
        random_row (int): The index of the random row.
        connection: The database connection.

    Returns:
        The selected row from the table.
    '''
    string = f"SELECT * FROM {table_name} LIMIT {random_row}, 1"
    return connection.execute(text(string))

def Pinterest_data_kinesis_data_streams():
    '''
    Obtains data from Pinterest AWS RDS database in an infinite loop, and sends to Kinesis Data Streams with invoke URL.
    '''
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        url = "https://nlhg5rjpwj.execute-api.us-east-1.amazonaws.com/test_streams/streams/"
        headers = {'Content-Type': 'application/json'}
        engine = RDS_connector

        with engine.connect() as connection:
            tables = ["pinterest_data", "geolocation_data", "user_data"]
            stream_names = ["streaming-124714cdee67-pin", "streaming-124714cdee67-geo", "streaming-124714cdee67-user"]
            for table, stream_name in zip(tables, stream_names):
                selected_row = get_random_row(table, random_row, connection)
                for row in selected_row:
                    result = dict(row._mapping)
                    send_record_to_kinesis(stream_name, result, url, headers)

if __name__ == "__main__":
    print('Working')