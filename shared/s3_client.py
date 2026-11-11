import boto3
from dotenv import load_dotenv

load_dotenv()

# Create a reusable S3 client
s3_client = boto3.client("s3")
