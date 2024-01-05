import requests
from time import sleep
import random
from multiprocessing import Process
import json
import sqlalchemy
from sqlalchemy import text
from json import dumps
from json import loads

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

def run_infinite_post_data_loop():                
    #t_end = time.time() + 20 #run for 20 seconds
    #while time.time() < t_end:
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
                print(pin_result)
                pin_payload = json.dumps({
                    "StreamName": "streaming-124714cdee67-pin",
                    "Data": {"value": [pin_result]
                            },
                            "PartitionKey": "partition-0"
                            }, default = str)
                print(pin_payload)

                pin_response = requests.request("PUT", f"{url}streaming-124714cdee67-pin/record", headers=headers, data=pin_payload)

                if pin_response.status_code == 200:
                    print("Record successfully sent to Kinesis Pin Stream.")
                else:
                    print("Failed to send record to Kinesis Pin Stream. Status code:", pin_response.status_code)
                    sleep(0.5)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)
                print(geo_result)
                geo_payload = json.dumps({
                    "StreamName": "streaming-124714cdee67-geo",
                    "Data": {"value": [geo_result]
                            },
                            "PartitionKey": "partition-0"
                            }, default = str)
                print(geo_payload)
                geo_response = requests.request("PUT", f"{url}streaming-124714cdee67-geo/record", headers=headers, data=geo_payload)

                if geo_response.status_code == 200:
                    print("Record successfully sent to Kinesis Geo Stream.")
                else:
                    print("Failed to send record to Kinesis Geo Stream. Status code:", geo_response.status_code)
                    sleep(0.5)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
                print(user_result)
                user_payload = json.dumps({
                    "StreamName": "streaming-124714cdee67-user",
                    "Data": {"value": [user_result]
                            },
                            "PartitionKey": "partition-0"
                            }, default = str)
                print(user_payload)

                user_response = requests.request("PUT", f"{url}streaming-124714cdee67-user/record", headers=headers, data=user_payload)

                if user_response.status_code == 200:
                    print("Record successfully sent to Kinesis User Stream.")
                else:
                    print("Failed to send record to Kinesis User Stream. Status code:", user_response.status_code)
                    sleep(0.5)

if __name__ == "__main__":
    print('Working')
    run_infinite_post_data_loop()