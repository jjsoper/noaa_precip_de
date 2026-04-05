import os

from dotenv import load_dotenv

load_dotenv()
LOG_LEVEL = "DEBUG"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# GCP
PROJECT = os.environ["PROJECT"]
DATASET = os.environ["DATASET"]
BUCKET = os.environ["BUCKET"]
