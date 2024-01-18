# s3-redshift-data-pipeline
A Python-based data processing pipeline for transferring, cleaning, and loading CSV data from AWS S3 to Redshift. Supports incremental loading and error handling.

## Dataset
The dataset used in this project (`heart_2020.csv`) is from Kaggle. You can access the dataset using the following link:
[Personal Key Indicators of Heart Disease Dataset on Kaggle](https://www.kaggle.com/datasets/kamilpytlak/personal-key-indicators-of-heart-disease)

The dataset provides information about various health indicators and the presence or absence of heart disease. It includes features such as age, gender, cholesterol levels, blood pressure, and more.

## Prerequisites
Before running this project, ensure you have the following prerequisites set up:

### AWS Configuration
1. **AWS Account:** Make sure you have an active AWS account.
2. **S3 Buckets:** Create two S3 buckets - one for the source CSV files and another for storing the last load time information.
3. **Redshift Cluster:** Set up an Amazon Redshift cluster and ensure it's accessible for data loading.
4. **IAM Roles and Policies:** Configure IAM roles with permissions for accessing S3, Glue, and Redshift.
5. **AWS Secrets Manager:** Store your AWS credentials (for Redshift and S3) in the AWS Secrets Manager.
6. **AWS Glue:** Set up an AWS Glue environment to run your ETL jobs.

### Local Environment
1. **Python:** Ensure you have Python installed on your local machine.
2. **Required Libraries:** Install the necessary Python libraries (`boto3`, `pandas`, `redshift_connector`, `awsglue`, etc.).
3. **Integrated Development Environment (IDE):** Have an IDE (like Visual Studio Code) ready for developing and running the Python scripts.

## Setup Instructions
1. Clone the repository to your local machine.
2. Install the required Python libraries by running `pip install -r requirements.txt` 
3. Set up the required environment variables for AWS access keys, Redshift database connection details, and S3 bucket names.

## Running the Scripts
1. **ETL Job (`etl_job.py`):**
   - This script downloads data from the S3 source bucket, performs data cleaning and transformation, and uploads the processed data back to a destination S3 bucket.
   - Run this script using the command: `python etl_job.py`.

2. **AWS Glue Job (`s3-redshift-glue-etl.py`):**
   - This script reads the processed data from the S3 destination bucket, filters it for incremental loading, and loads it into the Redshift database.
   - Deploy this script as a job in the AWS Glue console and trigger it manually or on a schedule.

## Additional Information
- Make sure to configure your AWS environment properly before running these scripts.
- Modify the scripts as per your specific data schema and processing requirements.
- Regularly monitor and optimize the performance of the pipeline and AWS resources.**
