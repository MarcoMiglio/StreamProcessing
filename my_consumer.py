from confluent_kafka import Consumer
import json


c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'latest',
})

c.subscribe(['test'])

while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    dic = json.loads(msg.value().decode('utf-8'))

    print('Received message: {}'.format(dic))
    print('Partition: ', msg.partition())

c.close()