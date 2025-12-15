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
        
        if g == 'female' or g == 'mostly_female':
            return 'Female'
        elif g == 'male' or g == 'mostly_male':
            return 'Male'
        else:
            return 'Unknown'
            
    except: return 'Unknown'

def get_sentiment(text):
    try:
        return TextBlob(str(text)).sentiment.polarity
    except:
        return 0.0

def get_brand_category(text):
    text_lower = str(text).lower()
    
    # --- MAPPING V8 (FINAL + DISCOUNT FIX) ---
    brand_map = {
        # 1. TEKNOLOGI
        r'[@#]?applesupport|[@#]?\bapple\b|\biphone\b|\bipad\b|\bmacbook\b|\bios\b|\bairpods\b': ('Apple', 'Technology'),
        r'[@#]?google|\bgoogle\b|\bandroid\b|\bpixel\b': ('Google', 'Technology'),
        r'[@#]?microsofthelps|\@office\w*|\@azuresupport|\bmicrosoft\b|\boffice365\b|\bexcel\b': ('Microsoft', 'Technology'),
        r'[@#]?adobecare|\badobe\b|\bphotoshop\b': ('Adobe', 'Technology'),
        r'[@#]?hpsupport|\bhp\b|\bhewlett\b': ('HP', 'Technology'),
        r'[@#]?dellcares|\bdell\b': ('Dell', 'Technology'),
        r'[@#]?dropboxsupport|\bdropbox\b': ('Dropbox', 'Technology'),
        r'[@#]?godaddy|\bgodaddy\b': ('GoDaddy', 'Technology'),
        r'[@#]?nortonsupport|\bnorton\b': ('Norton', 'Technology'),
        r'[@#]?askseagate|\bseagate\b': ('Seagate', 'Technology'),
        r'[@#]?yahoocare|\byahoo\b': ('Yahoo', 'Technology'),
        r'[@#]?twittersupport|\btwitter\b': ('Twitter', 'Technology'),
        r'[@#]?airbnb|\bairbnb\b': ('Airbnb', 'Technology'),

        # 2. BANKING & FINANCIAL
        r'[@#]?askamex|\bamex\b|\bamerican express\b': ('American Express', 'Banking & Financial'),
        r'[@#]?askpaypal|\bpaypal\b': ('PayPal', 'Banking & Financial'),
        r'[@#]?chasesupport|\bchase\b|\bchase bank\b': ('Chase', 'Banking & Financial'),
        r'[@#]?bofa|\bbank of america\b': ('Bank of America', 'Banking & Financial'),
        r'[@#]?ask_wellsfargo|\bwells fargo\b': ('Wells Fargo', 'Banking & Financial'),
        r'[@#]?askciti|\bciti\b|\bcitibank\b': ('Citibank', 'Banking & Financial'),
        r'[@#]?askrbc|\brbc\b': ('RBC', 'Banking & Financial'),

        # 3. RETAIL & E-COMMERCE
        r'[@#]?amazon\w*|\bprime\b|\bkindle\b|\bfire\s*tv\s*stick\b': ('Amazon', 'Retail & E-commerce'),
        r'[@#]?askebay|\bebay\b': ('eBay', 'Retail & E-commerce'),
        r'[@#]?asktarget|\btarget\b': ('Target', 'Retail & E-commerce'),
        r'[@#]?walmart|\bwalmart\b': ('Walmart', 'Retail & E-commerce'),
        r'[@#]?tesco|\btesco\b': ('Tesco', 'Retail & E-commerce'),
        r'[@#]?sainsburys|\bsainsbury': ('Sainsburys', 'Retail & E-commerce'),
        r'[@#]?marksandspencer|\bmarks & spencer\b|\bm&s\b': ('Marks & Spencer', 'Retail & E-commerce'),
        r'[@#]?morrisons|\bmorrisons\b': ('Morrisons', 'Retail & E-commerce'),
        r'[@#]?nike\w*|\bnike\b': ('Nike', 'Retail & E-commerce'),
        r'[@#]?argoshelpers|\bargos\b': ('Argos', 'Retail & E-commerce'),
        r'[@#]?neweggservice|\bnewegg\b': ('Newegg', 'Retail & E-commerce'),
        r'[@#]?kimpton|\bkimpton\b': ('Kimpton', 'Retail & E-commerce'),
        r'[@#]?ihgservice|\bihg\b': ('IHG Hotels', 'Retail & E-commerce'),
        r'[@#]?hiltonhelp|\bhilton\b': ('Hilton', 'Retail & E-commerce'),

        # 4. TELECOM & INTERNET
        r'[@#]?verizon\w*|\bverizon\b': ('Verizon', 'Telecom & Internet'),
        r'[@#]?comcastcares|\bcomcast\b|\bxfinity\b': ('Comcast', 'Telecom & Internet'),
        r'[@#]?tmobilehelp|\bt-mobile\b|\btmobile\b': ('T-Mobile', 'Telecom & Internet'),
        r'[@#]?sprintcare|\bsprint\b': ('Sprint', 'Telecom & Internet'),
        r'[@#]?ask_spectrum|\bspectrum\b': ('Spectrum', 'Telecom & Internet'),
        r'[@#]?coxhelp|\bcox\b': ('Cox', 'Telecom & Internet'),
        r'[@#]?glocare|\bglo\b|\bglobacom\b': ('Glo', 'Telecom & Internet'),
        r'[@#]?safaricom_care|\bsafaricom\b': ('Safaricom', 'Telecom & Internet'),
        r'[@#]?02|\bo2\b': ('O2', 'Telecom & Internet'),
        r'[@#]?airtel_care|\bairtel\b': ('Airtel', 'Telecom & Internet'),
        r'[@#]?boostcare|\bboost mobile\b': ('Boost Mobile', 'Telecom & Internet'),
        r'[@#]?centurylinkhelp|\bcenturylink\b': ('CenturyLink', 'Telecom & Internet'),
        r'[@#]?att|\batt\b': ('AT&T', 'Telecom & Internet'),
        r'[@#]?uscellularcares|\bus cellular\b': ('US Cellular', 'Telecom & Internet'),

        # 5. FOOD & BEVERAGES
        r'[@#]?ubereats|\bubereats\b': ('Uber Eats', 'Food & Beverages'),
        r'[@#]?mcdonalds|\bmcdonalds\b|\bmcd\b': ('McDonalds', 'Food & Beverages'),
        r'[@#]?askpapajohns|\bpapa johns\b': ('Papa Johns', 'Food & Beverages'),
        r'[@#]?askpanera|\bpanera\b': ('Panera Bread', 'Food & Beverages'),
        r'[@#]?dunkindonuts|\bdunkin\b': ('Dunkin', 'Food & Beverages'),
        r'[@#]?kfc\w*|\bkfc\b': ('KFC', 'Food & Beverages'),
        r'[@#]?jackbox|\bjack in the box\b': ('Jack in the Box', 'Food & Beverages'),
        r'[@#]?greggsofficial|\bgreggs\b': ('Greggs', 'Food & Beverages'),
        r'[@#]?tacobellteam|\btaco bell\b': ('Taco Bell', 'Food & Beverages'),
        r'[@#]?chipotletweets|\bchipotle\b': ('Chipotle', 'Food & Beverages'),
        r'[@#]?arbyscares|\barbys\b': ('Arbys', 'Food & Beverages'),
        r'\blunch\b|\bdinner\b|\bbreakfast\b|\bmeal\b|\bsnack\b': ('General Food', 'Food & Beverages'),
        r'\bchicken\b|\bfried chicken\b|\bwings\b|\bnuggets\b': ('General Food', 'Food & Beverages'),
        r'\bpizza\b|\bpepperoni\b|\bparmesan\b|\bmozzarella\b': ('General Food', 'Food & Beverages'),
        r'\burger\b|\bfries\b|\bsandwich\b|\bcheddar\b|\bbeef\b|\bsteak\b': ('General Food', 'Food & Beverages'),
        r'\bcoffee\b|\blatte\b|\bespresso\b|\btea\b|\bdrink\b': ('General Food', 'Food & Beverages'),

        # 6. TRANSPORTATION & LOGISTICS
        r'[@#]?americanair|\bamerican airlines\b': ('American Air', 'Transportation'),
        r'[@#]?british_airways|\bbritish airways\b|\bba\b': ('British Airways', 'Transportation'),
        r'[@#]?southwestair|\bsouthwest\b': ('Southwest', 'Transportation'),
        r'[@#]?delta|\bdelta\b': ('Delta', 'Transportation'),
        r'[@#]?alaskaair|\balaska airlines\b': ('Alaska Air', 'Transportation'),
        r'[@#]?virginatlantic|\bvirgin atlantic\b': ('Virgin Atlantic', 'Transportation'),
        r'[@#]?airasia\w*|\bairasia\b': ('AirAsia', 'Transportation'),
        r'[@#]?jetblue|\bjetblue\b': ('JetBlue', 'Transportation'),
        r'[@#]?virgintrains|\bvirgin trains\b': ('Virgin Trains', 'Transportation'),
        r'[@#]?gwrhelp|\bgreat western railway\b': ('GWR', 'Transportation'),
        r'[@#]?sw_help|\bsouth western railway\b': ('SW Railway', 'Transportation'),
        r'[@#]?nationalrailenq|\bnational rail\b': ('National Rail', 'Transportation'),
        r'[@#]?londonmidland|\blondon midland\b': ('London Midland', 'Transportation'),
        r'[@#]?asklyft|\blyft\b': ('Lyft', 'Transportation'),
        
        # 7. LOGISTICS & SHIPPING
        r'[@#]?uber_support|\buber\b': ('Uber', 'Logistics & Shipping'),
        r'[@#]?upshelp|\bups\b': ('UPS', 'Logistics & Shipping'),
        r'[@#]?usps|\busps\b|\bpost office\b': ('USPS', 'Logistics & Shipping'),
        r'[@#]?fedex|\bfedex\b': ('FedEx', 'Logistics & Shipping'),
        r'[@#]?postmates_help|\bpostmates\b': ('Postmates', 'Logistics & Shipping'),
        r'[@#]?doordash|\bdoordash\b': ('DoorDash', 'Logistics & Shipping'),

        # 8. GAMING & ENTERTAINMENT
        r'[@#]?askplaystation|\bplaystation\b|\bps4\b|\bps5\b|\bsony\b': ('Sony', 'Entertainment'),
        r'[@#]?xboxsupport|\bxbox\b': ('Xbox', 'Entertainment'),
        r'[@#]?atviassist|\bactivision\b': ('Activision', 'Entertainment'),
        r'[@#]?spotifycares|\bspotify\b': ('Spotify', 'Entertainment'),
        r'[@#]?hulu_support|\bhulu\b': ('Hulu', 'Entertainment')
    }

    # LOGIKA 1: Cek Mapping Spesifik
    for pattern, info in brand_map.items():
        if re.search(pattern, text_lower):
            return [info[0], info[1]]

    # LOGIKA 2: Smart Fallback (Kontekstual)
    if 'car' in text_lower or 'vehicle' in text_lower or 'driver' in text_lower:
        return ["General Transport", "Transportation"]
    if 'airline' in text_lower or 'boarded' in text_lower or 'flight' in text_lower: 
        return ["General Airline", "Transportation"]
    if 'lte' in text_lower or 'residential' in text_lower or 'wifi' in text_lower or 'internet' in text_lower or 'broadband' in text_lower:
        return ["General ISP", "Telecom & Internet"]
    if 'gps' in text_lower or 'delivery' in text_lower or 'package' in text_lower or 'parcel' in text_lower: 
        return ["General Courier", "Logistics & Shipping"]
    if 'app' in text_lower or 'login' in text_lower or 'update' in text_lower:
        return ["General Tech", "Technology"]
    
    return ["Unknown", "Uncategorized"]

# --- SPARK CONFIG ---
spark = SparkSession.builder \
    .appName("LakehouseEngineV8_Fixed") \
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

gender_udf = udf(get_gender, StringType())
sentiment_udf = udf(get_sentiment, FloatType())
brand_udf = udf(get_brand_category, ArrayType(StringType()))

# --- STREAMING ---
df_sales = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka:9092").option("subscribe", "superstore_sales").option("startingOffsets", "earliest").option("maxRatePerPartition", "10").load()
df_tweets = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka:9092").option("subscribe", "customer_tweets").option("startingOffsets", "earliest").option("maxRatePerPartition", "10").load()

# --- TRANSFORM ---
# Skema Lengkap Sales (TERMASUK DISCOUNT)
sales_schema = StructType() \
    .add("Order ID", StringType()) \
    .add("Order Date", StringType()) \
    .add("Customer Name", StringType()) \
    .add("Category", StringType()) \
    .add("Sub-Category", StringType()) \
    .add("Sales", FloatType()) \
    .add("Quantity", IntegerType()) \
    .add("Discount", FloatType()) \
    .add("Profit", FloatType()) \
    .add("State", StringType())

# Update logic JSON parser
clean_sales = df_sales.select(from_json(col("value").cast("string"), sales_schema).alias("data")).select("data.*").withColumn("gender_predicted", gender_udf(col("Customer Name"))).withColumn("parsed_date", to_date(col("Order Date"), "M/d/yyyy"))

# ... (Sisanya sama) ...
tweet_schema = StructType().add("tweet_id", StringType()).add("inbound", BooleanType()).add("created_at", StringType()).add("text", StringType())
clean_tweets = df_tweets.select(from_json(col("value").cast("string"), tweet_schema).alias("data")).select("data.*").filter(col("inbound") == True).withColumn("sentiment_score", sentiment_udf(col("text"))).withColumn("brand_info", brand_udf(col("text"))).withColumn("brand", col("brand_info")[0]).withColumn("category", col("brand_info")[1]).withColumn("sentiment_label", when(col("sentiment_score") > 0, "Positive").when(col("sentiment_score") < 0, "Negative").otherwise("Neutral")).withColumn("parsed_date", to_timestamp(col("created_at"), "EEE MMM dd HH:mm:ss Z yyyy"))

# --- OUTPUT ---
def write_pg(df, table):
    try:
        df.write.format("jdbc").option("url", "jdbc:postgresql://postgres:5432/warehouse_db").option("dbtable", table).option("user", "admin").option("password", "password123").option("driver", "org.postgresql.Driver").mode("append").save()
        print(f"✅ [SUCCESS] Batch written to {table}")
    except Exception as e:
        print(f"❌ [ERROR] Failed writing to {table}: {e}")

# --- SEBELUMNYA (V8 - Belum ada checkpoint) ---
# q1 = clean_sales.writeStream.foreachBatch(lambda df, id: write_pg(df, "fact_sales")).start()
# q2 = clean_tweets.writeStream.foreachBatch(lambda df, id: write_pg(df, "fact_tweets")).start()

# --- SESUDAHNYA (V9 - Checkpoint MinIO Aktif) ---
# Kita simpan "bookmark" pembacaan di MinIO (s3a://lakehouse/...)

q1 = clean_sales.writeStream \
    .foreachBatch(lambda df, id: write_pg(df, "fact_sales")) \
    .option("checkpointLocation", "s3a://lakehouse/checkpoints/sales") \
    .start()

q2 = clean_tweets.writeStream \
    .foreachBatch(lambda df, id: write_pg(df, "fact_tweets")) \
    .option("checkpointLocation", "s3a://lakehouse/checkpoints/tweets") \
    .start()

print(">>> ENGINE V8 (FINAL + DISCOUNT FIX) STARTED <<<")
spark.streams.awaitAnyTermination()