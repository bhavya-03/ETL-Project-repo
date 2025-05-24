import boto3
import pandas as pd
import psycopg2
from io import StringIO

# AWS S3 details
s3_bucket = 'bhavya-rds-data-pipeline-demo'
s3_key = 'sales_data.csv'

# Download CSV from S3
s3 = boto3.client('s3')
response = s3.get_object(Bucket=s3_bucket, Key=s3_key)
csv_content = response['Body'].read().decode('utf-8')

# Load into pandas DataFrame
df = pd.read_csv(StringIO(csv_content))

# PostgreSQL RDS connection info
conn = psycopg2.connect(
    host='demo-database.cdy8sigyufrd.us-east-2.rds.amazonaws.com',
    user='dbadmin',
    password='Success_Money$1997',
    port=5432
)
cur = conn.cursor()

# Insert data into RDS
for _, row in df.iterrows():
    cur.execute("""
        INSERT INTO sales_data (order_id, customer_id, order_date, total_amount) VALUES (%d, %d, %d, %d, %d)
    """, tuple(row))

conn.commit()
cur.close()
conn.close()

print("✅ Data successfully loaded from S3 to PostgreSQL RDS.")
