# ! pip install "celery[redis]" opensearch-py opensearch-dsl
from celery import Celery
from dotenv import load_dotenv
from tasks import process_data
from connectors.os_query import extract_rca_path, get_events_data, get_rca_paths, get_realtime_incidents

load_dotenv()

def fetch_and_enqueue_data():
    incidents_df = get_realtime_incidents("2023-12-11 00:00:00", "2023-12-11 06:00:00")
    for idx, row in incidents_df.iterrows():
        id = row['incidentId']
        rca_data = get_rca_paths(id)
        g, event_identifiers = extract_rca_path(rca_data)
        raw_events = get_events_data(event_identifiers)
        raw_events['merge_col'] = raw_events['groups.appName'] + '___' + raw_events['groups.groupName'] + '___' + raw_events['hostname'] + '___' + raw_events['metricname']
        raw_events = raw_events[raw_events['merge_col'].isin(g)]
        raw_events = raw_events[['hostname', '@timestamp', 'metricname', 'eventid', 'tags', 'name', 'groups.groupName', 'groups.appName']]
        raw_events.sort_values(by=['@timestamp'], inplace=True)
        raw_events.drop_duplicates(subset=['name', 'metricname', 'groups.groupName', 'groups.appName'], keep='first', inplace=True)
        raw_events = raw_events.to_dict(orient='records')
        process_data.delay(id, g)

if __name__ == '__main__':
    fetch_and_enqueue_data()
