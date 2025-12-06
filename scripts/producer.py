import time
import json
import pandas as pd
from kafka import KafkaProducer
import os

# Konfigurasi
KAFKA_SERVER = 'kafka:9092'

def wait_for_kafka():
    while True:
        try:
            # api_version=(0, 10, 1) adalah KUNCI agar tidak error NoBrokersAvailable
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                api_version=(0, 10, 1) 
            )
            print("Connected to Kafka!")
            return producer
        except Exception as e:
            print(f"Waiting for Kafka... Error: {e}")
            time.sleep(5)

print("Connecting to Kafka...")
producer = wait_for_kafka()

print("Reading CSV Data...")
# Pastikan nama file di folder data hurufnya kecil semua (superstore.csv / tweets.csv)
try:
    df_sales = pd.read_csv('/app/data/superstore.csv', encoding='windows-1252')
    df_tweets = pd.read_csv('/app/data/tweets.csv')
except Exception as e:
    print(f"Error baca file: {e}")
    exit()

print(">>> START STREAMING DATA <<<")
iter_sales = df_sales.iterrows()
iter_tweets = df_tweets.iterrows()

try:
    while True:
        # Kirim Sales
        try:
            _, sale = next(iter_sales)
            producer.send('superstore_sales', sale.to_dict())
            print(f"[SALES] Sent Order: {sale['Order ID']}")
        except StopIteration: pass

        # Kirim Tweet
        try:
            _, tweet = next(iter_tweets)
            producer.send('customer_tweets', tweet.to_dict())
            print(f"[TWEET] Sent Tweet ID: {tweet['tweet_id']}")
        except StopIteration: pass
        
        time.sleep(0.5) # Speed streaming
except KeyboardInterrupt:
    print("Stopped.")