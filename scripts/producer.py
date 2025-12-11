import time
import json
import pandas as pd
import os
from kafka import KafkaProducer

# --- KONFIGURASI ---
KAFKA_SERVER = 'kafka:9092'
STATE_FILE = '/app/data/producer_state.json'  # File ingatan (disimpan di volume)

def wait_for_kafka():
    while True:
        try:
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

# --- FUNGSI CHECKPOINT (STATE MANAGEMENT) ---
def load_state():
    """Membaca posisi terakhir dari file JSON"""
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                return json.load(f)
        except:
            return {'sales_index': 0, 'tweets_index': 0}
    return {'sales_index': 0, 'tweets_index': 0}

def save_state(sales_idx, tweets_idx):
    """Menyimpan posisi terakhir ke file JSON"""
    with open(STATE_FILE, 'w') as f:
        json.dump({'sales_index': sales_idx, 'tweets_index': tweets_idx}, f)

# --- MAIN PROGRAM ---
print("Connecting to Kafka...")
producer = wait_for_kafka()

print("Reading CSV Data...")
try:
    # Load Data
    df_sales = pd.read_csv('/app/data/superstore.csv', encoding='windows-1252')
    df_tweets = pd.read_csv('/app/data/tweets.csv')
    
    # Load State (Ingatan Terakhir)
    state = load_state()
    start_sales = state.get('sales_index', 0)
    start_tweets = state.get('tweets_index', 0)
    
    print(f">>> RESUMING STREAM <<<")
    print(f"Sales starting at row: {start_sales}")
    print(f"Tweets starting at row: {start_tweets}")

    # Slice Data (Skip data yang sudah dikirim)
    # Kita hanya mengambil data mulai dari index terakhir
    df_sales_remaining = df_sales.iloc[start_sales:]
    df_tweets_remaining = df_tweets.iloc[start_tweets:]

    # Buat Iterator baru dari data sisa
    iter_sales = df_sales_remaining.iterrows()
    iter_tweets = df_tweets_remaining.iterrows()
    
    # Counter lokal untuk tracking
    current_sales_idx = start_sales
    current_tweets_idx = start_tweets

except Exception as e:
    print(f"Error initializing: {e}")
    exit()

print(">>> START STREAMING <<<")

try:
    while True:
        # Kirim Sales
        try:
            idx, sale = next(iter_sales)
            producer.send('superstore_sales', sale.to_dict())
            print(f"[SALES] Row {current_sales_idx} Sent: {sale['Order ID']}")
            current_sales_idx += 1
        except StopIteration:
            print("[SALES] Data Habis.")
            pass

        # Kirim Tweet
        try:
            idx, tweet = next(iter_tweets)
            producer.send('customer_tweets', tweet.to_dict())
            print(f"[TWEET] Row {current_tweets_idx} Sent ID: {tweet['tweet_id']}")
            current_tweets_idx += 1
        except StopIteration:
            print("[TWEET] Data Habis.")
            pass

        # Simpan State (Checkpointing Producer)
        # Kita simpan setiap kali kirim agar kalau mati mendadak, ingatannya update
        save_state(current_sales_idx, current_tweets_idx)

        time.sleep(0.5) # Speed control

except KeyboardInterrupt:
    print("\nStopped by User.")
    save_state(current_sales_idx, current_tweets_idx) # Save terakhir sebelum mati
    print(f"State saved at: Sales={current_sales_idx}, Tweets={current_tweets_idx}")