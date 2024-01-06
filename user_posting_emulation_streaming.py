import requests
import random
from multiprocessing import Process
import json
import sqlalchemy
from sqlalchemy import text
from json import dumps
from json import loads
from time import sleep

random.seed(100)

class AWSDBConnector:

    def __init__(self):

        self.HOST = "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
        self.USER = 'project_user'
        self.PASSWORD = ':t%;yCY3Yjg'
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306
        
    def create_db_connector(self):
        '''
        Method to create a connection to the AWS RDS database
        '''
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine
    
new_connector = AWSDBConnector()

def send_data_kinesis_data_streams():
    '''
    Obtaining data from Pinterest AWS RDS database in an infinite loop, and sending to Kinesis Data Streams
    with invoke URL.
    '''                
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            # Make a POST request to the API Invoke URL
            url = "https://nlhg5rjpwj.execute-api.us-east-1.amazonaws.com/test_streams/streams/"
            headers = {'Content-Type': 'application/json'}

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)
                pin_payload = json.dumps({
                "StreamName": "streaming-124714cdee67-pin",
                "Data": pin_result,
                "PartitionKey": "test"
                }, default = str)
                print(pin_payload)

                pin_response = requests.request("PUT", f"{url}streaming-124714cdee67-pin/record", headers=headers, data=pin_payload)

                if pin_response.status_code == 200:
                    print("Record successfully sent to Kinesis Pin Stream.")
                else:
                    print("Failed to send record to Kinesis Pin Stream. Status code:", pin_response.status_code)
                    sleep(1)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)
                geo_payload = json.dumps({
                "StreamName": "streaming-124714cdee67-geo",
                "Data": geo_result,
                "PartitionKey": "test"
                }, default = str)
                print(geo_payload)
                geo_response = requests.request("PUT", f"{url}streaming-124714cdee67-geo/record", headers=headers, data=geo_payload)

                if geo_response.status_code == 200:
                    print("Record successfully sent to Kinesis Geo Stream.")
                else:
                    print("Failed to send record to Kinesis Geo Stream. Status code:", geo_response.status_code)
                    sleep(1)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
                user_payload = json.dumps({
                "StreamName": "streaming-124714cdee67-user",
                "Data": user_result,
                "PartitionKey": "test"
                }, default = str)
                print(user_payload)

                user_response = requests.request("PUT", f"{url}streaming-124714cdee67-user/record", headers=headers, data=user_payload)

                if user_response.status_code == 200:
                    print("Record successfully sent to Kinesis User Stream.")
                else:
                    print("Failed to send record to Kinesis User Stream. Status code:", user_response.status_code)
                    sleep(1)

if __name__ == "__main__":
    print('Working')
    send_data_kinesis_data_streams()