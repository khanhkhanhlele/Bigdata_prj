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


def extract_zip(input_zip):
    input_zip = ZipFile(input_zip)
    return {name: input_zip.read(name) for name in input_zip.namelist()}

KEYS = ['MAL_ID', 'Name', 'Score', 'Genders', 'English name', 'Japanese name', 'Type', 'Episodes',
        'Aired', 'Premiered', 'Producers', 'Licensors', 'Studios', 'Source', 'Duration', 'Rating',
        'Ranked', 'Popularity', 'Members', 'Favorites', 'Watching', 'Completed', 'On-Hold', 'Dropped',
        'Plan to Watch', 'Score-10', 'Score-9', 'Score-8', 'Score-7', 'Score-6', 'Score-5', 'Score-4',
        'Score-3', 'Score-2', 'Score-1']

def get_name(info):
    try:
        return info.find("h1", {"class": "title-name h1_bold_none"}).text.strip()
    except: 
        return ""

def get_english_name(info):
    try:
        span = info.findAll("span", {"class": "dark_text"})
        return span.parent.text.strip()
    except:
        return ""

def get_table(a_soup):
    try:
        return a_soup.find("div", {"class": "po-r js-statistics-info di-ib"})
    except:
        return ""

def get_score(stats):
    try:
        score = stats.find("span", {"itemprop": "ratingValue"})
        if score is None:
            return "Unknown"
        return score.text.strip()
    except:
        return ""

def get_gender(sum_info):
    try:
        text = ", ".join(
            [x.text.strip() for x in sum_info.findAll("span", {"itemprop": "genre"})]
        )
        return text
    except:
        return ""

def get_description(sum_info):
    try:
        return sum_info.find("td", {"class": "borderClass", "width": "225"})
    except:
        return ""

def get_all_stats(soup):
    try:
        return soup.find("div", {"id": "horiznav_nav"}).parent.findAll(
            "div", {"class": "spaceit_pad"}
        )
    except:
        return ""

def get_info_anime(anime_id):
    data = extract_zip(f"craw/html/{anime_id}.zip")
    anime_info = data["stats.html"].decode()
    soup = BeautifulSoup(anime_info, "html.parser")

    stats = get_table(soup)
    description = get_description(soup)
    anime_info = {key: "Unknown" for key in KEYS}

    anime_info["MAL_ID"] = anime_id
    anime_info["Name"] = get_name(soup)
    anime_info["Score"] = get_score(stats)
    anime_info["Genders"] = get_gender(description)

    for d in description.findAll("span", {"class": "dark_text"}):
        information = [x.strip().replace(" ", " ") for x in d.parent.text.split(":")]
        category, value = information[0], ":".join(information[1:])
        value.replace("\t", "")

        if category in ["Broadcast", "Synonyms", "Genres", "Score", "Status"]:
            continue

        if category in ["Ranked"]:
            value = value.split("\n")[0]
        if category in ["Producers", "Licensors", "Studios"]:
            value = ", ".join([x.strip() for x in value.split(",")])
        if category in ["Ranked", "Popularity"]:
            value = value.replace("#", "")
        if category in ["Members", "Favorites"]:
            value = value.replace(",", "")
        if category in ["English", "Japanese"]:
            category += " name"

        anime_info[category] = value

    # Stats (Watching, Completed, On-Hold, Dropped, Plan to Watch)
    for d in get_all_stats(soup)[:5]:
        category, value = [x.strip().replace(" ", " ") for x in d.text.split(":")]
        value = value.replace(",", "")
        anime_info[category] = value

    # Stast votes per score
    for d in get_all_stats(soup)[6:]:
        score = d.parent.parent.find("td", {"class": "score-label"}).text.strip()
        value = [x.strip().replace(" ", " ") for x in d.text.split("%")][1].strip(
            "(votes)"
        )
        label = f"Score-{score}"
        anime_info[label] = value.strip()

    for key, value in anime_info.items():
        if str(value) in ["?", "None found, add some", "None", "N/A", "Not available"]:
            anime_info[key] = "Unknown"
    return anime_info


anime_revised = set()
exist_file = os.path.exists(f"{BASE_PATH}/anime.tsv")
actual_data = pd.DataFrame()
if exist_file:
    # If the file exist, include new data.
    actual_data = pd.read_csv(f"{BASE_PATH}/anime.tsv", sep="\t")
    anime_revised = list(actual_data.MAL_ID.unique())

actual_data.head()
total_data = []
zips = os.listdir(HTML_PATH)
for i, anime in enumerate(zips):
    if not ".zip" in anime:
        print(1)
        continue

    anime_id = int(anime.strip(".zip"))

    # if int(anime_id) in anime_revised:
    #     print(2)

    #     continue

    print(f"\r{i+1}/{len(zips)} ({anime_id})", end="")

    anime_id = anime.strip(".zip")
    info = get_info_anime(anime_id)
    producer.send('anime_tsv', value=info, key = random.choice(range(0,11)))
    total_data.append(info)

if len(total_data):
    df = pd.DataFrame.from_dict(total_data)
    df["MAL_ID"] = pd.to_numeric(df["MAL_ID"])
    df = df.sort_values(by="MAL_ID").reset_index(drop=True)

    if exist_file:
        df = (
            pd.concat([actual_data, df]).sort_values(by="MAL_ID").reset_index(drop=True)
        )

else:
    df = actual_data


df.to_csv(f"{BASE_PATH}/anime.tsv", index=False, sep="\t", encoding="UTF-8")

