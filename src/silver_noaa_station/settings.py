import os

from dotenv import load_dotenv

load_dotenv()
LOG_LEVEL = "DEBUG"

# GCP
PROJECT = os.environ["PROJECT"]
BRONZE_DATASET = os.environ["BRONZE_DATASET"]
SILVER_DATASET = os.environ["SILVER_DATASET"]
