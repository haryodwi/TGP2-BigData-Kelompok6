FROM apache/spark:3.5.1
USER root

# Upgrade PIP
RUN pip install --upgrade pip

# Install Library Python Lengkap
# (Kafka, Pandas, Database, AI Text Processing, Delta Lake)
RUN pip install kafka-python pandas sqlalchemy psycopg2-binary textblob gender-guesser delta-spark==3.1.0

USER root