from kafka import KafkaProducer
import time

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

with open('/home/train/datasets/nyc_taxi.csv') as f:
    line = f.read()

for line in line.split('\n'):
    time.sleep(0.5)

    if 'vendor_id' in line:
        producer.send(topic='taxi', value=line.encode('utf-8'))
 
producer.flush()