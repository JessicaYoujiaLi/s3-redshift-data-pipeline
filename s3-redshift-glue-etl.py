import sys
import logging
import datetime
import boto3
import json
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job
from pyspark.sql.functions import col
from awsglue.dynamicframe import DynamicFrame  # Added import for DynamicFrame

# Initialize logging
logging.basicConfig(level=logging.INFO)

# Constants for Last Load Time Tracking
LAST_LOAD_BUCKET = 'proj-destination-2024'  # Replace with your bucket name
LAST_LOAD_OBJECT = 'last_load_time.txt'

# Function to get the last load time from S3
def get_last_load_time(s3_client, bucket_name, object_key):
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        last_load_time_str = response['Body'].read().decode('utf-8')
        return datetime.datetime.strptime(last_load_time_str, "%Y-%m-%d %H:%M:%S")
    except s3_client.exceptions.NoSuchKey:
        return None

# Function to set the last load time in S3
def set_last_load_time(s3_client, bucket_name, object_key, last_load_time):
    s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=last_load_time.strftime("%Y-%m-%d %H:%M:%S"))

# Initialize AWS Secrets Manager client
secrets_manager = boto3.client('secretsmanager', region_name='us-east-1')

try:
    # Retrieve Redshift credentials from Secrets Manager
    redshift_secret_name = 'my-redshift-credentials'
    logging.info(f"Retrieving Redshift credentials from Secrets Manager: {redshift_secret_name}")
    redshift_secret = secrets_manager.get_secret_value(SecretId=redshift_secret_name)
    redshift_credentials = json.loads(redshift_secret['SecretString'])
    logging.info("Redshift credentials retrieved successfully.")
except boto3.exceptions.ClientError as e:
    logging.error(f"Error accessing AWS Secrets Manager: {e}")
    sys.exit(1)

# Initialize the GlueContext and SparkContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Define your data sources and targets
s3_source_path = "s3://proj-destination-2024/cleaned_heart_disease.csv/"
redshift_database = "dev"
redshift_table = "heart_disease_csv"
logging.info(f"Data source: {s3_source_path}")
logging.info(f"Data target: Redshift database {redshift_database}, table {redshift_table}")

# Initialize S3 client
s3_client = boto3.client('s3')

# Get the last load time
last_load_time = get_last_load_time(s3_client, LAST_LOAD_BUCKET, LAST_LOAD_OBJECT)

try:
    # Read data from S3
    logging.info("Reading data from S3...")
    dynamic_frame = glueContext.create_dynamic_frame.from_options(
        format_options={"paths": [s3_source_path], "withHeader": True},
        format="csv",
        connection_type="s3"
    )
    logging.info("Data read successfully from S3.")
    
    # Convert to DataFrame for filtering
    data_frame = dynamic_frame.toDF()
    
    # Filter data based on last load time
    if last_load_time is not None:
        logging.info(f"Filtering data newer than {last_load_time}")
        data_frame = data_frame.filter(col("last_modified") > last_load_time)
    
    # Convert back to DynamicFrame
    datasource_filtered = DynamicFrame.fromDF(data_frame, glueContext, "datasource_filtered")

except Exception as e:
    logging.error(f"Error reading data from S3: {e}")
    sys.exit(1)

try:
    # Write data to Redshift
    logging.info("Writing data to Redshift...")
    glueContext.write_dynamic_frame.from_catalog(
        frame=datasource_filtered,
        database=redshift_database,
        table_name=redshift_table,
        connection_type="Redshift",
        connection_options={
            "url": redshift_credentials['host'],
            "dbtable": redshift_table,
            "user": redshift_credentials['username'],
            "password": redshift_credentials['password']
        },
        transformation_ctx="datasink"
    )
    logging.info("Data written successfully to Redshift.")
except Exception as e:
    logging.error(f"Error writing data to Redshift: {e}")
    sys.exit(1)

# Update the last load time
current_time = datetime.datetime.now()
set_last_load_time(s3_client, LAST_LOAD_BUCKET, LAST_LOAD_OBJECT, current_time)

job.commit()
logging.info("Glue job completed successfully.")
