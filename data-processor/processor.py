import json


def process_data(topic, data):
    data_dict: dict = json.loads(data)

    print(data_dict)
