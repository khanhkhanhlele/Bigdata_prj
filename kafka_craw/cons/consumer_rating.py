import json
import subprocess
from kafka import KafkaConsumer
import os

def deserializer(message):
    return json.loads(message.decode('utf-8'))

bootstrap_servers = ['localhost:9092', 'localhost:9093', 'localhost:9094']

consumer = KafkaConsumer(
    'rating_complete',
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=deserializer
)

filename = 'rating_complete.csv'


if __name__ == '__main__':

    try:
        message_count = 0
        batch_size = 50

        for message in consumer:
            print(f"Consumed message: {message.value}")

            # Ghi message vào tệp hiện tại
            with open(filename, 'a') as file:
                file.write(json.dumps(message.value) + '\n')

            message_count += 1

            # Nếu đã đạt đến số lượng messages cần, đẩy tệp vào HDFS và reset biến đếm
            if message_count % batch_size == 0:
                os.system(f'docker cp {filename} namenode:/')
                # Đẩy tệp vào HDFS
                subprocess.run(['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-appendToFile', filename, '/data/'])
                


    except KeyboardInterrupt:
        consumer.close()
