import json
from kafka import KafkaConsumer

server = 'localhost:9092'
topic_name = 'green-trips'

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',
    group_id='green-trips-console',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    consumer_timeout_ms=3000  # stops after 3s of no new messages
)

print(f"Listening to {topic_name}...")

count = 0
total = 0

for message in consumer:
    record = message.value
    total += 1
    if record['trip_distance'] > 5.0:
        count += 1

print(f'Total trips: {total}')
print(f'Trips with distance > 5.0 km: {count}')

consumer.close()