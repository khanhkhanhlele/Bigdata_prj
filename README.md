# Big_Data

# Anime Analysis and Recommendation System

## Introduction
Project on the Topic of ‘Storage and Processing of Big Data’

## Data Preparation
Our data is crawled from [MyAnimeList](https://myanimelist.net/) and [Kaggle](https://www.kaggle.com/datasets/hernan4444/anime-recommendation-database-2020?select=rating_complete.csv&fbclid=IwAR37KBNhDMUmDlL2he0iLylicmXE4KjugeiNUarZjhUH-oqHNOtHkYVjvQ4)

## Document
[Slide](https://husteduvn-my.sharepoint.com/:p:/g/personal/khanh_ln200316_sis_hust_edu_vn/EVHZfF_BTQZOpt8AKefpUdcBLJ8hRu-4niXxZaTpanND0w?e=OCfdgz)
[Report](https://husteduvn-my.sharepoint.com/:w:/g/personal/khanh_ln200316_sis_hust_edu_vn/EenyblT4C-BKvOXZqb15Z6IBTnkAHUUl4gpxbqLCm0Z6ug?e=lR1RVr)

## Flow

### Start the system

```
docker-compose up -d
```
### 1. Crawl
### 2. Kafka
### 3. HDFS
### 4. Spark
### 5. Elasticsearch
### 6. Kibana
Copy files from local machine into Spark master, for data analysis, we need to copy `elasticsearch` file into Spark master:
```
docker cp src spark-master:/
docker cp elasticsearch-hadoop-7.15.1.jar spark-master:elasticsearch-hadoop-7.15.1.jar
```

Copy files into namenode:
```
docker cp ../data/short_anime_ratings.csv namenode:/
docker cp ../data/rating_complete.csv namenode:/
docker cp ../data/long_anime_ratings.csv namenode:/
```

Push data to HDFS:
```
docker exec -it namenode /bin/bash
hdfs dfs -mkdir /data/
hdfs dfs -mkdir /model/
hdfs dfs -mkdir /result/
hdfs dfs -put short_anime_ratings.csv /data/
hdfs dfs -put rating_complete.csv /data/
hdfs dfs -put long_anime_ratings.csv /data/
exit
```

Go into Spark master container:
```
docker exec -it spark-master /bin/bash
```

Make directory to save models and results:
```
mkdir -p /result/model
mkdir -p result/spark_nodes/
mkdir -p /result/model
mkdir -p result/spark_nodes/
mkdir -p result/read_file/
```

To use Spark ML, we need to install `numpy` in Spark master node. However, it requires g++. To add all requirement successfully, we have to use a virtual environment to prevent conflict with other files:
```
python3 -m venv pyspark_venv
source pyspark_venv/bin/activate
apk update
apk add make automake gcc g++ subversion python3-dev
pip3 install numpy venv-pack
```

To run a Python file, we use `spark-submit`. `elasticsearch` is optional, add when using `elasticsearch`:
```
spark/bin/spark-submit --master spark://spark-master:7077 --jars elasticsearch-hadoop-7.15.1.jar --driver-class-path elasticsearch-hadoop-7.15.1.jar src/als_anime.py
```
