import kagglehub
import os

def load_data(string):
    DATASET_ROOT_DIR = string
    path = kagglehub.dataset_download("zahidmughal2343/supplement-sales-data")
    print("Path to dataset files:", path)
    if not os.path.exists(DATASET_ROOT_DIR):
        os.makedirs(DATASET_ROOT_DIR)
    os.system("cp -r {}/* {}".format(path, DATASET_ROOT_DIR))
    print("Path to dataset files:", path)

load_data("/opt/airflow/data")