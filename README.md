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

```bash
git clone [https://github.com/haryodwi/TGP2-BigData-Kelompok6.git](https://github.com/haryodwi/TGP2-BigData-Kelompok6.git)

cd TGP2-BigData-Kelompok6


**2. ⚠️ Download Dataset Besar (PENTING!)**
Karena limitasi GitHub (Maks 100MB), file tweets.csv (500MB+) tidak disertakan di repo ini. Kalian harus download manual.

Download file tweets.csv dari link ini: 👉 https://www.kaggle.com/datasets/thoughtvector/customer-support-on-twitter/data

Copy file twcs.csv tersebut ke dalam folder data/ di dalam project ini dan ubah menjadi tweets.csv.

Pastikan struktur foldernya menjadi:

data/superstore.csv (Sudah ada)
data/tweets.csv (Baru didownload)

Tentu, Pak Haryo! Ini adalah versi README.md yang sudah diperbarui dengan link repository GitHub Bapak yang asli.

Silakan Copy seluruh kode di bawah ini, lalu Paste (timpa) ke dalam file README.md di folder project Bapak. Jangan lupa untuk memasukkan Link Google Drive dataset tweets.csv pada langkah nomor 2.

Markdown

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

```bash
git clone [https://github.com/haryodwi/TGP2-BigData-Kelompok6.git](https://github.com/haryodwi/TGP2-BigData-Kelompok6.git)
cd TGP2-BigData-Kelompok6
2. ⚠️ Download Dataset Besar (PENTING!)
Karena limitasi GitHub (Maks 100MB), file tweets.csv (500MB+) tidak disertakan di repo ini. Kalian harus download manual.

Download file tweets.csv dari link ini: 👉 [MASUKKAN LINK GOOGLE DRIVE/ONEDRIVE BAPAK DISINI]

Copy file tersebut ke dalam folder data/ di dalam project ini.

Pastikan struktur foldernya menjadi:

data/superstore.csv (Sudah ada)

data/tweets.csv (Baru didownload)

**3. Jalankan Container**
Bangun dan nyalakan semua service (Kafka, Spark, Postgres, Metabase, MinIO):

Bash

docker compose up -d --build


Tunggu sekitar 2-5 menit hingga semua selesai download dan statusnya "Running".

4. Setup Infrastruktur (Hanya Sekali di Awal)
Copy-paste perintah ini ke terminal untuk menyiapkan Database dan Topik Kafka:

Bash

# 1. Buat Database untuk Metabase
docker compose exec postgres psql -U admin -d postgres -c "CREATE DATABASE metabase;"

# 2. Buat Topik Kafka
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic superstore_sales --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic customer_tweets --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
5. Restore Dashboard Jadi 📊
Agar kalian tidak perlu bikin grafik dari nol, restore konfigurasi dashboard yang sudah jadi:

Bash

cat metabase_dashboard.sql | docker compose exec -T postgres psql -U admin -d metabase
(Catatan: Jika perintah cat tidak jalan di PowerShell Windows biasa, gunakan Git Bash atau Command Prompt).

▶️ Cara Menjalankan Program
Kalian butuh membuka 2 Terminal berbeda agar sistem berjalan.

Terminal 1: Pengirim Data (Producer)
Terminal ini bertugas mengirim data dummy secara terus-menerus ke Kafka.

Bash

docker compose exec spark python3 /app/scripts/producer.py
Kalian akan melihat log berjalan: [SALES] Sent Order... dan [TWEET] Sent Tweet...

Terminal 2: Pemroses Data (Spark Engine)
Terminal ini adalah otak yang memproses data, mendeteksi sentimen, gender, dan brand secara real-time.

Bash

docker compose exec spark bash -c "export PYSPARK_PYTHON=python3 && /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.6.0,io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4 /app/scripts/spark_processor.py"
Tunggu sampai muncul pesan hijau ✅ [SUCCESS] Batch written to...

🌐 Akses Dashboard
Buka browser: http://localhost:3000

Login dengan kredensial berikut (Hasil Restore):

Email: admin@test.com (atau sesuai email Metabase Pak Haryo)

Password: password123 (atau sesuai password Metabase Pak Haryo)

Masuk ke Collection -> Our Analytics -> Buka Dashboard "Executive Market Pulse".

❓ Troubleshooting Common Issues
Error: "tweets.csv not found" -> Cek langkah nomor 2. Pastikan nama file persis tweets.csv (huruf kecil semua).

Metabase kosong/tidak ada tabel? -> Masuk ke Admin Settings -> Databases -> Data Warehouse -> Klik "Sync database schema now".

Container Spark mati sendiri (Exit 137)? -> RAM laptop penuh. Coba restart Docker Desktop atau naikkan alokasi RAM di settingan Docker ke 4GB.




