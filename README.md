# 🚀 Real-time Big Data Analytics - Kelompok 6 (TGP2)

Repository ini berisi source code untuk pipeline analisis data *real-time* menggunakan arsitektur **Modern Data Lakehouse**. Sistem ini menggabungkan data transaksi penjualan (Superstore) dan sentimen media sosial (Twitter) untuk menganalisis korelasi antara komplain pelanggan dan omzet penjualan.

## 🏗️ Arsitektur Sistem

* **Ingestion:** Python Producer -> Apache Kafka (KRaft Mode)
* **Processing:** Apache Spark Structured Streaming (Python) + AI Enrichment (Gender & Sentiment Analysis)
* **Storage:** MinIO (Data Lake) & PostgreSQL (Data Warehouse)
* **Visualization:** Metabase

---

## 🛠️ Prasyarat (Wajib)

Sebelum memulai, pastikan laptop kalian sudah terinstall:

1.  **Docker Desktop** (Pastikan statusnya *Running*).
2.  **Git**.
3.  **(Khusus Windows)** Pastikan fitur **WSL 2** sudah aktif.
4.  RAM minimal 4GB-6GB free.

---

## 📥 Cara Install (Step-by-Step)

### 1. Clone Repository
Buka terminal/PowerShell di folder tujuan, lalu jalankan:

```python
git clone [https://github.com/haryodwi/TGP2-BigData-Kelompok6.git](https://github.com/haryodwi/TGP2-BigData-Kelompok6.git)
```
```python
cd TGP2-BigData-Kelompok6
```

###2. ⚠️ Download Dataset Besar (PENTING!)
Karena limitasi GitHub (Maks 100MB), file tweets.csv (500MB+) tidak disertakan di repo ini. Kalian harus download manual.

Download file tweets.csv dari link ini: 👉 https://www.kaggle.com/datasets/thoughtvector/customer-support-on-twitter/data

Copy file twcs.csv tersebut ke dalam folder data/ di dalam project ini dan ubah menjadi tweets.csv.

Pastikan struktur foldernya menjadi:
```python
data/superstore.csv (Sudah ada)
data/tweets.csv (Baru didownload)
```
 

###3. Jalankan Container
Bangun dan nyalakan semua service (Kafka, Spark, Postgres, Metabase, MinIO):

```python
docker compose up -d --build
```


Tunggu sekitar 2-5 menit hingga semua selesai download dan statusnya "Running".

###4. Setup Infrastruktur (Hanya Sekali di Awal)
Copy-paste perintah ini ke terminal untuk menyiapkan Database dan Topik Kafka:


##### 1. Buat Database untuk Metabase

```python
docker compose exec postgres psql -U admin -d postgres -c "CREATE DATABASE metabase;"
```

#####  2. Buat Topik Kafka

```python
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic superstore_sales --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

```python
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic customer_tweets --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```


###5. Restore Dashboard Jadi 📊 (skip dulu karena dashboard belum ada)
Agar kalian tidak perlu bikin grafik dari nol, restore konfigurasi dashboard yang sudah jadi:

```python
cat metabase_dashboard.sql | docker compose exec -T postgres psql -U admin -d metabase ```
(Catatan: Jika perintah cat tidak jalan di PowerShell Windows biasa, gunakan Git Bash atau Command Prompt).

▶️ Cara Menjalankan Program
Kalian butuh membuka 2 Terminal berbeda agar sistem berjalan.

Terminal 1: Pengirim Data (Producer)
Terminal ini bertugas mengirim data dummy secara terus-menerus ke Kafka.

```python
docker compose exec spark python3 /app/scripts/producer.py```


Kalian akan melihat log berjalan: [SALES] Sent Order... dan [TWEET] Sent Tweet...

Terminal 2: Pemroses Data (Spark Engine)
Terminal ini adalah otak yang memproses data, mendeteksi sentimen, gender, dan brand secara real-time.

```python
docker compose exec spark bash -c "export PYSPARK_PYTHON=python3 && /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.6.0,io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4 /app/scripts/spark_processor.py"```
Tunggu sampai muncul pesan hijau ✅ [SUCCESS] Batch written to...

#🌐 Akses Dashboard

Buka browser: http://localhost:3000
Login dengan kredensial berikut :

Email: kelompok6@gmail.com 
Password: kelompok6  

Masuk ke Collection -> Our Analytics -> Buka Dashboard "Executive Market Pulse". (belum ada)

#❓ Troubleshooting Common Issues
Error: "tweets.csv not found" -> Cek langkah nomor 2. Pastikan nama file persis tweets.csv (huruf kecil semua).
Metabase kosong/tidak ada tabel? -> Masuk ke Admin Settings -> Databases -> Data Warehouse -> Klik "Sync database schema now".
Container Spark mati sendiri (Exit 137)? -> RAM laptop penuh. Coba restart Docker Desktop atau naikkan alokasi RAM di settingan Docker ke 4GB.




