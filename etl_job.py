import boto3
import pandas as pd
import os
import logging
import redshift_connector
from botocore.exceptions import ClientError


# Initialize Logger for logging information and errors
logging.basicConfig(level=logging.INFO)

# Environment Variables for AWS and Redshift Credentials
# These are fetched from the system's environment variables for security
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
REDSHIFT_HOST = os.getenv('REDSHIFT_HOST')
DATABASE_NAME = os.getenv('DATABASE_NAME')
USER = os.getenv('USER')
PASSWORD = os.getenv('PASSWORD')
AWS_REGION = 'us-east-1'

# S3 Bucket Details
SOURCE_BUCKET_NAME = 'test-glue-2024'
DESTINATION_BUCKET_NAME = 'proj-destination-2024'
SOURCE_FILE_NAME = 'heart_disease.csv'
DESTINATION_FILE_NAME = 'cleaned_heart_disease.csv'


def download_file_from_s3(bucket, file_name):
    """
    Downloads a file from an S3 bucket.
    :param bucket: Name of the S3 bucket.
    :param file_name: Name of the file to download.
    """
    try:
        s3_client.download_file(bucket, file_name, file_name)
        logging.info(f"Successfully downloaded {file_name} from S3 bucket {bucket}")
    except Exception as e:
        logging.error(f"Error downloading file from S3: {e}")
        raise

def upload_file_to_s3(bucket, file_name):
    """
    Uploads a file to an S3 bucket.
    :param bucket: Name of the S3 bucket.
    :param file_name: Name of the file to upload.
    """
    try:
        s3_client.upload_file(file_name, bucket, file_name)
        logging.info(f"Successfully uploaded {file_name} to S3 bucket {bucket}")
    except Exception as e:
        logging.error(f"Error uploading file to S3: {e}")
        raise

def clean_column_names(df):
    """
    Cleans the column names of a DataFrame by removing null characters.
    :param df: Pandas DataFrame with the original column names.
    """
    cleaned_columns = {}
    for col in df.columns:
        cleaned_col = col.replace('\0', '') 
        cleaned_columns[col] = cleaned_col
    df.rename(columns=cleaned_columns, inplace=True)

def validate_data(df):
    """
    Validates the DataFrame for missing values.
    :param df: Pandas DataFrame to validate.
    :raises ValueError: If missing values are found.
    """
    if df.isnull().sum().any():
        raise ValueError("Data contains missing values.")
    
    logging.info("Data validation passed.")


def process_data(file_name):
    """
    Processes the CSV data file. Steps include reading the file, cleaning column names,
    removing duplicates, converting binary variables, and validating the data.
    :param file_name: Name of the CSV file to process.
    :return: Processed Pandas DataFrame.
    """

    # Load CSV into DataFrame
    try:
        df = pd.read_csv(file_name, delimiter=None, engine='python', error_bad_lines=False)
    except UnicodeDecodeError:
        df = pd.read_csv(file_name, encoding='iso-8859-1', delimiter=None, engine='python', error_bad_lines=False)
    except pd.errors.ParserError as e:
        logging.error(f"Error parsing CSV file: {e}")
        raise

    # Clean Column Names
    clean_column_names(df)

    # Remove Duplicates
    df.drop_duplicates(inplace=True)

    # Convert Binary Variables (Yes/No) to Numerical Binary (0/1)
    binary_columns = [col for col in df.columns if df[col].dropna().value_counts().index.isin(['Yes', 'No']).all()]
    for col in binary_columns:
        df[col] = df[col].map({'Yes': 1, 'No': 0})

    # Data Validation
    validate_data(df)

    # Save Cleaned Data to Local System
    df.to_csv(DESTINATION_FILE_NAME, index=False)

    return df

def create_redshift_table(df):
    """
    Creates a table in Redshift based on the schema of the provided DataFrame.
    :param df: Pandas DataFrame used to define the Redshift table schema.
    """
    # Generate Table Schema
    heart_disease_schema = pd.io.sql.get_schema(df, 'heart_disease')

    # Connect to Redshift and Create Table
    try:
        with redshift_connector.connect(
            host=REDSHIFT_HOST,
            database=DATABASE_NAME,
            user=USER,
            password=PASSWORD,
            port=5439
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute(heart_disease_schema)
                logging.info("Table created in Redshift")
    except Exception as e:
        logging.error(f"Error creating table in Redshift: {e}")
        raise

# Main Execution Block
if __name__ == '__main__':
    # Connect to S3
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )

    # Download, Process, and Upload Data
    download_file_from_s3(SOURCE_BUCKET_NAME, SOURCE_FILE_NAME)
    processed_df = process_data(SOURCE_FILE_NAME)
    upload_file_to_s3(DESTINATION_BUCKET_NAME, DESTINATION_FILE_NAME)

    # Create Table in Redshift
    create_redshift_table(processed_df)
