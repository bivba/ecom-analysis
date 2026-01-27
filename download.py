
import pandas as pd
from dotenv import load_dotenv
import os
from pathlib import Path
import json

load_dotenv()

print("Loading environment variables...")
username = os.getenv("KAGGLE_USERNAME")
key = os.getenv("KAGGLE_KEY")

# username = os.getenv("KAGGLE_USERNAME")
# key = os.getenv("KAGGLE__KEY")

kaggle_dir = 'C:\\Users\\danil\\.kaggle'
os.makedirs(kaggle_dir, exist_ok=True)

kaggle_json_path = os.path.join(kaggle_dir, 'kaggle.json')

with open(kaggle_json_path, 'w') as f:
    json.dump({"username": username, "key": key}, f)


from kaggle.api.kaggle_api_extended import KaggleApi
api = KaggleApi()
api.authenticate()

api.dataset_download_files('mkechinov/ecommerce-behavior-data-from-multi-category-store', path='./raw_csv', unzip=True, force=True, quiet=False)

print("Dataset downloaded and extracted successfully!")