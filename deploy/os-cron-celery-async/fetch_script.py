# ! pip install "celery[redis]" opensearch-py

from celery import Celery
from dotenv import load_dotenv
from inference_script import process_data
from connectors.os_query import extract_rca_path, get_rca_paths, get_realtime_incidents

load_dotenv()

def fetch_and_enqueue_data():
    incidents_df = get_realtime_incidents()
    for idx, row in incidents_df.iterrows():
        id = row['incidentId']
        rca_data = get_rca_paths(id)
        g = extract_rca_path(rca_data)
        process_data.delay(g)

if __name__ == '__main__':
    fetch_and_enqueue_data()
