import os
import hashlib
import pandas as pd
from opensearchpy import OpenSearch, helpers
from datetime import datetime, timedelta
from connectors.connection_manager import QueryHelper, ConnectionParams, QueryParams, GroupBY

def get_os_connection_params() -> ConnectionParams:
    c = ConnectionParams()
    c.ip_string = os.environ.get("OPENSEARCH_NODES")
    c.user_id = os.environ.get("OPENSEARCH_USERNAME")
    c.password = os.environ.get("OPENSEARCH_PASSWORD")
    c.timeout_in_minutes = 15
    c.max_no_persistent_connections = 15
    c.sniff_flag = 'True'
    c.http_compress_on_flag = 'True'
    return c

def get_rca_paths(incident_id: str):
    # index_name = "heal_rca_att_2023.w*"
    index_name = os.environ.get("OPENSEARCH_RCA_INDEX")
    query_helper = QueryHelper(get_os_connection_params())
    include_columns = ["rcaStatus", "rcaPath"]
    q = QueryParams()
    q.index_name = index_name
    q.include_cols = include_columns
    q.match_columns = QueryParams.Match('incidentId', incident_id)
    df = query_helper.search_data(q)
    if not df.empty:
        df.drop(columns="_id", inplace=True)
    return df

def extract_rca_path(rca_path):
    # g = nx.DiGraph()
    g = []
    event_identifiers = []
    for path in rca_path["rcaPath"][0]:
        prev_node = 'START'
        for elm in path['kpis']:
            affected_kpi = elm.get('affectedKpi')
            event_identifiers.extend(elm.get('eventIdentifiers'))
            if affected_kpi == 'START':
                # g.append(affected_kpi)
                # g.add_node(afftected_kpi, size=25, label='Start', color = "#C70039")
                continue
            g.append(affected_kpi)
            # g.add_node(afftected_kpi, size=15, color = "#48d1cc", label=get_label(afftected_kpi))
            # g.add_edge(prev_node, afftected_kpi, color = "#4169e1")
            prev_node = affected_kpi
    return g, event_identifiers

def get_realtime_incidents(from_date: str, to_date: str):
    from_date = pd.to_datetime(from_date)
    to_date = pd.to_datetime(to_date)
    query_helper = QueryHelper(get_os_connection_params())
    include_columns = ["@timestamp", "incidentId", "incidentStartTime", "incidentLastUpdatedTime", "status",
                       "eventCount", "incidentType", "userTag"]
    q = QueryParams()
    result_df = pd.DataFrame(columns=include_columns)
    user_tag = os.environ.get("OPENSEARCH_INCIDENTS_USER_TAG")
    q.time_column = 'incidentLastUpdatedTime'
    q.include_cols = include_columns
    q.match_columns = QueryParams.Match("relation", "parent")
    q.match_columns = QueryParams.Match("userTag", user_tag)
    for frm, to in date_batches(from_date, to_date, batch_in_days=1, date_in_epoch=False):
        # q.index_name = "heal_correlation_att_realtime_2023.w*"
        q.index_name = os.environ.get("OPENSEARCH_INCIDENTS_INDEX")
        q.time_range_filter = QueryParams.TimeFilter(frm, to)
        df = query_helper.search_data(q)
        if not df.empty:
            result_df = pd.concat([result_df, df])
    if not result_df.empty:
        result_df['incidentStartTime'] = pd.to_datetime(result_df['incidentStartTime'])
        result_df['incidentLastUpdatedTime'] = pd.to_datetime(result_df['incidentLastUpdatedTime'])

    return result_df

def get_llm_response(from_date: str, to_date: str):
    from_date = pd.to_datetime(from_date)
    to_date = pd.to_datetime(to_date)
    query_helper = QueryHelper(get_os_connection_params())
    include_columns = ["@timestamp", "incidentId", "events", "rootCause"]
    q = QueryParams()
    result_df = pd.DataFrame(columns=include_columns)
    q.time_column = '@timestamp'
    q.include_cols = include_columns
    for frm, to in date_batches(from_date, to_date, batch_in_days=1, date_in_epoch=False):
        # q.index_name = "heal_correlation_att_realtime_2023.w*"
        q.index_name = os.environ.get("OPENSEARCH_RESULT_INDEX")
        q.time_range_filter = QueryParams.TimeFilter(frm, to)
        df = query_helper.search_data(q)
        if not df.empty:
            result_df = pd.concat([result_df, df])
    if not result_df.empty:
        result_df['@timestamp'] = pd.to_datetime(result_df['@timestamp'])

    return result_df

def get_events_data(event_identifiers: list):
    # index_name = settings.OPENSEARCH_SOURCE_DATA_INDEX
    # index_name = "atnt-aioneops-normalized-anomalies-v1-*"
    index_name = os.environ.get("OPENSEARCH_EVENTS_INDEX")
    q = QueryParams()
    q.index_name = index_name
    # q.time_range_filter = QueryParams.TimeFilter(from_date, to_date)
    # q.include_cols = ["@timestamp", "eventId", "metricName", "entityId", "entityName", "impactLevel"]
    query_helper = QueryHelper(get_os_connection_params())
    q.match_columns = QueryParams.Match("eventid", list(set(event_identifiers)), in_match_type="terms")
    df = query_helper.search_data(q)
    return df

def date_batches(from_date: datetime, to_date: datetime, batch_in_days, date_in_epoch=False):
    if isinstance(from_date, str):
        from_date = pd.to_datetime(from_date)
        to_date = pd.to_datetime(to_date)
    frm = from_date
    break_flag = False
    while True:
        to = frm + timedelta(days=batch_in_days)
        if to >= to_date:
            break_flag = True
            to = to_date
        # yield frm, to - timedelta(minutes=1)
        if date_in_epoch:
            yield int((frm - pd.Timestamp(datetime.utcfromtimestamp(0))).total_seconds()) * 1000, \
                  int((to - pd.Timestamp(datetime.utcfromtimestamp(0))).total_seconds()) * 1000
        else:
            yield frm.strftime('%Y-%m-%d %H:%M:%S'), to.strftime('%Y-%m-%d %H:%M:%S')
        frm = to
        if break_flag:
            break

def get_hash(data):
    return hashlib.md5(data.encode()).hexdigest()

def insert_into_os(tup):
    os_client = OpenSearch(
        hosts=os.environ.get("OPENSEARCH_NODES"),
        # hosts=[{"host": "192.168.13.40", "port": 9200}],
        http_auth=(os.environ.get("OPENSEARCH_USERNAME"), os.environ.get("OPENSEARCH_PASSWORD")),
        use_ssl=False,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False,
        include_in_root=True,
        timeout=120
        )
            
    actions = []
    chain_id = tup[0]

    index_name = os.environ.get("OPENSEARCH_RESULT_INDEX")
    ts = pd.to_datetime(datetime.utcnow())
    actions.append(
        {
            '_index': index_name,
            '_type': 'incident',
            '_id': get_hash(chain_id),
            '_source': {
                "incidentId": chain_id,
                "events": tup[1],
                "rootCause": tup[2],
                # "@timestamp": pd.to_datetime(datetime.now()).replace(microsecond=0)
                # "@timestamp": pd.to_datetime(datetime.utcnow())
                "@timestamp": ts
            }
        }
    )
    helpers.bulk(os_client, actions)