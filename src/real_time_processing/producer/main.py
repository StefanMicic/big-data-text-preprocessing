import json
import os
import time
from itertools import zip_longest

import kafka.errors
import pandas as pd
from kafka import KafkaProducer

SUBREDDIT = os.environ["SUBREDDIT"]
KAFKA_BROKER = os.environ["KAFKA_BROKER"]
TOPIC = "subreddit-" + SUBREDDIT

while True:
    try:
        producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER.split(","))
        print("Connected to Kafka!")
        break
    except kafka.errors.NoBrokersAvailable as e:
        print(e)
        time.sleep(3)

f = open('train.json')
json_data = json.load(f)
data = pd.read_csv("train.csv")
for (index, row), i in zip_longest(data.iterrows(), json_data.values()):
    print("== csv ==>", TOPIC, index, row.text)
    producer.send(
        TOPIC,
        key=bytes(str(row[0]), "utf-8"),
        value=bytes(f"{row[0]} {row[5]}", "utf-8"),
    )
    try:
        print("== json ==>", TOPIC, i['reviewText'], i['overall'])
        producer.send(
            TOPIC,
            key=bytes(str(i['overall']), "utf-8"),
            value=bytes(f"{i['overall']} {i['reviewText']}", "utf-8"),
        )
    except TypeError:
        pass
    time.sleep(1)

f.close()
