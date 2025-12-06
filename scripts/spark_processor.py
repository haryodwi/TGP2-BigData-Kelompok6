import sys
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from textblob import TextBlob
import gender_guesser.detector as gender

# --- AI SETUP ---
d = gender.Detector()

def get_gender(name):
    if not name: return "Unknown"
    try:
        first = str(name).split(" ")[0]
        first = re.sub(r'[^a-zA-Z]', '', first)
        if not first: return "Unknown"
        g = d.get_gender(first)
        if 'male' in g: return 'Male'
        elif 'female' in g: return 'Female'
        else: return 'Unknown'
    except: return 'Unknown'

def get_sentiment(text):
    try:
        return TextBlob(str(text)).sentiment.polarity
    except:
        return 0.0

def get_brand_category(text):
    text_lower = str(text).lower()
    
    # --- MAPPING AKURAT BERDASARKAN GAMBAR TABEL BAPAK ---
    # Format: Regex Pattern : (Nama Brand, Kategori Industri)
    brand_map = {
        # 1. TEKNOLOGI & ELEKTRONIK
        r'@applesupport|\bapple\b|\biphone\b|\bipad\b|\bmacbook\b': ('Apple', 'Technology'),
        r'@google|\bgoogle\b|\bandroid\b': ('Google', 'Technology'),
        
        # 2. GAMING & KONSOL
        r'@askplaystation|\bplaystation\b|\bps4\b|\bps5\b|\bsony\b': ('Sony', 'Gaming & Console'),
        r'@xboxsupport|\bxbox\b|\bmicrosoft\b': ('Microsoft', 'Gaming & Console'),
        
        # 3. STREAMING & HIBURAN
        r'@spotifycares|\bspotify\b': ('Spotify', 'Music & Video Streaming'),
        r'@hulu_support|\bhulu\b': ('Hulu', 'Music & Video Streaming'),
        
        # 4. RITEL & E-COMMERCE
        r'@amazonhelp|\bamazon\b|\bprime\b': ('Amazon', 'Retail & Supermarket'),
        r'@tesco|\btesco\b': ('Tesco', 'Retail & Supermarket'),
        r'@sainsburys|\bsainsbury': ('Sainsburys', 'Retail & Supermarket'),
        
        # 5. TELEKOMUNIKASI & INTERNET
        r'@comcastcares|\bcomcast\b|\bxfinity\b': ('Comcast', 'Telecom & Internet'),
        r'@tmobilehelp|\bt-mobile\b|\btmobile\b': ('T-Mobile', 'Telecom & Internet'),
        r'@sprintcare|\bsprint\b': ('Sprint', 'Telecom & Internet'),
        r'@ask_spectrum|\bspectrum\b': ('Spectrum', 'Telecom & Internet'),
        
        # 6. MASKAPAI PENERBANGAN (AIRLINES)
        r'@americanair|\bamerican airlines\b': ('American Air', 'Airlines'),
        r'@british_airways|\bbritish airways\b': ('British Airways', 'Airlines'),
        r'@southwestair|\bsouthwest\b': ('Southwest', 'Airlines'),
        r'@delta|\bdelta\b': ('Delta', 'Airlines'),
        r'@virgintrains|\bvirgin\b': ('Virgin', 'Airlines'),
        
        # 7. TRANSPORTASI & LOGISTIK
        r'@uber_support|\buber\b': ('Uber', 'Transportation & Logistics'),
        r'@upshelp|\bups\b': ('UPS', 'Transportation & Logistics'),
        
        # 8. RESTORAN
        r'@chipotletweets|\bchipotle\b': ('Chipotle', 'Food & Beverages')
    }

    # Logika Pencocokan
    for pattern, info in brand_map.items():
        if re.search(pattern, text_lower):
            return [info[0], info[1]]

    # Smart Fallback (Jika tidak ada brand spesifik disebut)
    if 'phone' in text_lower or 'wifi' in text_lower or 'app' in text_lower: 
        return ["General Tech", "Technology"]
    if 'flight' in text_lower or 'plane' in text_lower or 'delay' in text_lower: 
        return ["General Travel", "Airlines"]
    if 'delivery' in text_lower or 'package' in text_lower: 
        return ["General Service", "Logistics & Shipping"]
    
    return ["Unknown", "Uncategorized"]

# --- SPARK CONFIG (SUPPORT MINIO/S3 + DELTA) ---
spark = SparkSession.builder \
    .appName("LakehouseEngineV4_Final") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.6.0,io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Register UDF
gender_udf = udf(get_gender, StringType())
sentiment_udf = udf(get_sentiment, FloatType())
brand_udf = udf(get_brand_category, ArrayType(StringType()))

# --- STREAMING INPUT ---
df_sales = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "superstore_sales") \
    .option("startingOffsets", "earliest") \
    .load()

df_tweets = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "customer_tweets") \
    .option("startingOffsets", "earliest") \
    .load()

# --- TRANSFORMATION ---
# 1. Sales
sales_schema = StructType() \
    .add("Order ID", StringType()).add("Order Date", StringType()) \
    .add("Customer Name", StringType()).add("Category", StringType()) \
    .add("Sales", FloatType()).add("Quantity", IntegerType()) \
    .add("Profit", FloatType()).add("State", StringType())

clean_sales = df_sales.select(from_json(col("value").cast("string"), sales_schema).alias("data")).select("data.*") \
    .withColumn("gender_predicted", gender_udf(col("Customer Name"))) \
    .withColumn("parsed_date", to_date(col("Order Date"), "M/d/yyyy"))

# 2. Tweets (FILTERED Inbound=True & Clean Brands)
tweet_schema = StructType() \
    .add("tweet_id", StringType()) \
    .add("inbound", BooleanType()) \
    .add("created_at", StringType()) \
    .add("text", StringType())

clean_tweets = df_tweets.select(from_json(col("value").cast("string"), tweet_schema).alias("data")).select("data.*") \
    .filter(col("inbound") == True) \
    .withColumn("sentiment_score", sentiment_udf(col("text"))) \
    .withColumn("brand_info", brand_udf(col("text"))) \
    .withColumn("brand", col("brand_info")[0]) \
    .withColumn("category", col("brand_info")[1]) \
    .withColumn("sentiment_label", when(col("sentiment_score") > 0, "Positive")
                                  .when(col("sentiment_score") < 0, "Negative")
                                  .otherwise("Neutral")) \
    .withColumn("parsed_date", to_timestamp(col("created_at"), "EEE MMM dd HH:mm:ss Z yyyy"))

# --- OUTPUT (DEBUG MODE: POSTGRES) ---
def write_pg(df, table):
    try:
        # Write to Data Warehouse (Postgres)
        df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/warehouse_db") \
            .option("dbtable", table) \
            .option("user", "admin").option("password", "password123") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append").save()
        print(f"✅ [SUCCESS] Batch written to {table}")
    except Exception as e:
        print(f"❌ [ERROR] Failed writing to {table}: {e}")

q1 = clean_sales.writeStream.foreachBatch(lambda df, id: write_pg(df, "fact_sales")).start()
q2 = clean_tweets.writeStream.foreachBatch(lambda df, id: write_pg(df, "fact_tweets")).start()

print(">>> ENGINE V4 (FINAL ACCURACY) STARTED <<<")
spark.streams.awaitAnyTermination()