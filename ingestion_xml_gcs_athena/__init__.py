from dotenv import load_dotenv
import os

load_dotenv()

SERVER = os.getenv('SERVER')
DATABASE = os.getenv('DATABASE')
QUERY = os.getenv('QUERY')
BUCKET_NAME = os.getenv('BUCKET_NAME')
BRONZE_BUCKET = os.getenv('BRONZE_BUCKET')
SILVER_BUCKET = os.getenv('SILVER_BUCKET')
RAW_BUCKET= os.getenv('RAW_BUCKET')