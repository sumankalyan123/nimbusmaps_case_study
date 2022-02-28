from kafka import KafkaProducer
import json
import time
import requests


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=json_serializer)


def main():
    while 1 == 1:
        response = requests.get('http://127.0.0.1:8000/get_data/')
        data = response.json()
        print(data)
        producer.send("DecisionTopic", data)
        time.sleep(3)


if __name__ == "__main__":
    main()
