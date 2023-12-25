import os
from bs4 import BeautifulSoup
import requests
import time
from datetime import datetime
import json
from zipfile import ZipFile
import pandas as pd
import shutil
import zipfile
import random
from kafka import KafkaProducer


BASE_PATH = "craw"
HTML_PATH = BASE_PATH + "/html"
USER_PATH = BASE_PATH + "/users"


def serializer(message):
    return json.dumps(message).encode('utf-8')
# Thêm địa chỉ của tất cả các broker vào danh sách dưới đây
bootstrap_servers = ['localhost:9092', 'localhost:9093', 'localhost:9094']

# Key serializer
def key_serializer(key):
    return str(key).encode('utf-8')



# Kafka Producer with custom partitioner
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=serializer,
    key_serializer=key_serializer,
)

if not os.path.exists(f"{BASE_PATH}/rating_complete.csv"):
    with open(f"{BASE_PATH}/rating_complete.csv", "w", encoding="UTF-8") as file:
        file.write("user_id,anime_id,rating\n")


unique_anime = set()
all_users = sorted(os.listdir(USER_PATH), key=lambda x:int(x.split(".")[0]))

with open(f"{BASE_PATH}/rating_complete.csv", "a") as f1:

    for i, user_file in enumerate(all_users):
        if not user_file.endswith(".csv"):
            continue

        print(f"\r{i+1}/{len(all_users)}", end="")

        user_id = user_file.split(".")[0]
        with open(f"{USER_PATH}/{user_file}", "r") as file:
            file.readline()
            for line in file:
                anime_id, score, watching_status, _ = line.strip().split(",")
                if int(watching_status) == 2 and (score) != 0:
                    temp = f"{user_id},{anime_id},{score}\n"
                    producer.send('rating_complete', value=temp, key = random.choice(range(0,11)))
                    f1.write(temp)