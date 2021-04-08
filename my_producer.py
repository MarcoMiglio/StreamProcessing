from confluent_kafka import Producer
import socket
import time
import numpy as np
import pandas as pd
import json

conf = {'bootstrap.servers': "localhost:9092"}

producer = Producer(conf)

df = pd.read_csv('data_test/data_000001.txt')
data = df.to_dict('records')

for i in range (0, len(data)):
  producer.produce('test', json.dumps(data[i]))
  producer.poll()
