import json
import ssl

import pandas as pd
import logging
from typing import List
from opensearch_dsl import Search, Q, A
from opensearchpy.client import OpenSearch
from opensearchpy.connection import create_ssl_context
from opensearchpy.helpers.errors import ScanError

logger = logging.getLogger(__name__)


class OpenSearchResultYield:
    """
    This Class has methods using which you can fetch the data, once you have your query object formed and connections made.
    """

    @staticmethod
    def search_result_yield_in_batches(os_client, search_params, query=None, block_size=10000,
                                       scroll_context_span_bw_requests="5m",
                                       raise_on_error=True,
                                       preserve_order=False,
                                       request_timeout=30,
                                       clear_scroll=True,
                                       scroll_kwargs=None,
                                       slice_kwargs=None,
                                       **kwargs):
        index_name = search_params.index_name
        """
        Code taken from opensearchpy.helpers.actions.scan, the only difference is it yields entire block instead of each hit.
        This will help in not expiring the scan context.


        :arg query: body for the :meth:`~opensearchpy.OpenSearch.search` api
        :arg scroll_context_span_bw_requests: Specify how long a consistent view of the index should be
            maintained for scrolled search. If your parsing of result of one scroll takes more time than this,
            scroll context will get expired.
        :arg raise_on_error: raises an exception (``ScanError``) if an error is
            encountered (some shards fail to execute). By default we raise.
        :arg preserve_order: don't set the ``search_type`` to ``scan`` - this will
            cause the scroll to paginate with preserving the order. Note that this
            can be an extremely expensive operation and can easily lead to
            unpredictable results, use with caution.
        :arg block_size: size (per shard) of the batch send at each iteration.
        :arg request_timeout: explicit timeout for each call to ``scan``
        :arg clear_scroll: explicitly calls delete on the scroll id via the clear
            scroll API at the end of the method on completion or error, defaults
            to true.
        :arg scroll_kwargs: additional kwargs to be passed to
            :meth:`~opensearchpy.OpenSearch.scroll`

        Any additional keyword arguments will be passed to the initial
        :meth:`~opensearchpy.OpenSearch.search` call::

            scan(client,
                query={"query": {"match": {"title": "python"}}},
                index="orders-*",
                doc_type="books"
            )


        """
        scroll_kwargs = scroll_kwargs or {}

        if not preserve_order:
            query = query.copy() if query else {}
            query["sort"] = "_doc"

        # Grab options that should be propagated to every
        # API call within this helper instead of just 'search()'
        transport_kwargs = {}
        for key in ("headers", "api_key", "http_auth"):
            if key in kwargs:
                transport_kwargs[key] = kwargs[key]

        # If the user is using 'scroll_kwargs' we want
        # to propagate there too, but to not break backwards
        # compatibility we'll not override anything already given.
        if scroll_kwargs is not None and transport_kwargs:
            for key, val in transport_kwargs.items():
                scroll_kwargs.setdefault(key, val)

        if slice_kwargs:
            slice_id = slice_kwargs['id']
            max_slices = slice_kwargs['max']
            query.update({"slice": {'id': slice_id, 'max': max_slices}})

        resp = os_client.search(
            index=index_name, body=query, scroll=scroll_context_span_bw_requests, size=block_size,
            request_timeout=request_timeout,
            **kwargs
        )
        scroll_id = resp.get("_scroll_id")

        try:
            while scroll_id and resp["hits"]["hits"]:
                def doc_parse(doc):
                    doc['_source'].update({'_id': doc['_id']})
                    return doc['_source']

                redata = map(doc_parse, resp['hits']['hits'])
                df = OpenSearchResultYield.parse_docs_to_df(redata, search_params)
                yield df

                # Default to 0 if the value isn't included in the response
                shards_successful = resp["_shards"].get("successful", 0)
                shards_skipped = resp["_shards"].get("skipped", 0)
                shards_total = resp["_shards"].get("total", 0)

                # check if we have any errors
                if (shards_successful + shards_skipped) < shards_total:
                    shards_message = "Scroll request has only succeeded on %d (+%d skipped) shards out of %d."
                    logger.warning(
                        shards_message, shards_successful, shards_skipped, shards_total,
                    )
                    if raise_on_error:
                        raise ScanError(
                            scroll_id,
                            shards_message
                            % (
                                shards_successful,
                                shards_skipped,
                                shards_total,
                            ),
                        )
                resp = os_client.scroll(
                    body={"scroll_id": scroll_id, "scroll": scroll_context_span_bw_requests}, **scroll_kwargs
                )
                scroll_id = resp.get("_scroll_id")

        finally:
            if scroll_id and clear_scroll:
                os_client.clear_scroll(
                    body={"scroll_id": [scroll_id]}, ignore=(404,), **transport_kwargs
                )

    @staticmethod
    def parse_docs_to_df(docs, search_params):
        return pd.json_normalize(docs, sep='.')

    @staticmethod
    def parse_default_agg_dict(curr_level_data_dict, ma=None, col_name_builder=None, base_col_name=None,
                               metrics_ordered_lst=None, **kwargs):
        pass

    @staticmethod
    def join_open_search_agg_keys(*args, agg_join_str='__'):
        result = agg_join_str.join(filter(None, args))
        return None if result == '' else result

    @staticmethod
    def get_fields(curr_data_dict, current_bucket):
        val = {}
        if "key" in curr_data_dict:
            val = {current_bucket: curr_data_dict["key"]}
        return val

    @staticmethod
    def aggregation_result_yield(resp_data, agg_ordered_lst: list, metrics_ordered_lst: list, index_col: str,
                                 keys_seperator: str = '__'):
        """
        If no metric aggregations are performed, it will return dict.
        else it will return pandas dataframe.
        """
        agg_result = resp_data.aggregations
        col_name_builder = OpenSearchResultYield.ColumnBuilder()
        result_dict = OpenSearchResultYield.__aggregation_result_yield(agg_result, agg_ordered_lst, metrics_ordered_lst,
                                                                       index_col, col_name_builder,
                                                                       keys_seperator=keys_seperator)
        if len(metrics_ordered_lst) != 0:
            result = pd.DataFrame(result_dict.values())
        else:
            result = result_dict
        return result

    @staticmethod
    def __aggregation_result_yield(curr_level_data_dict: dict, agg_ordered_lst: list, metrics_ordered_lst: list,
                                   index_col: str,
                                   col_name_builder,
                                   current_ts: str = '', result_data_dict: dict = None,
                                   keys_seperator: str = '__') -> dict:
        """
        PRIVATE method not to be used.
        current_level_data_dict: pass resp.aggregations value
        agg_ordered_lst: bucket aggregations list
        metrics_order_lst: metric aggregations.
                            By default you can give 'doc_count' metric.
                            If len(metric_order_lst) >0 :: you can convert the result dict into a data frame.
                            else you will receive a dictionary with 'keys' as key where you will find bucket keys
                            seperated by agg_key_seperator.
        index_col: Will become main keys of result dict.
        col_name_builder: Give an ColumnBuilder object, this will be used for avoiding overriding.
        result_data_dict: At the start it will be empty dict

        """
        if result_data_dict is None:
            result_data_dict = dict()
        curr_bucket = ''
        available_buckets_lst = list(filter(lambda bucket_name: bucket_name in curr_level_data_dict, agg_ordered_lst))
        if len(available_buckets_lst) > 0:
            curr_bucket = available_buckets_lst[0]
            curr_level_data_dict = curr_level_data_dict[curr_bucket]
        if "buckets" not in curr_level_data_dict:
            # reached metric aggregations stage: Completed Bucket chains for a record
            base_col_name_keys = [col_name_builder.get_param(key) for key in agg_ordered_lst if key != index_col]
            base_col_name = OpenSearchResultYield.join_open_search_agg_keys(*base_col_name_keys,
                                                                            agg_join_str=keys_seperator)
            if len(metrics_ordered_lst) != 0:
                for ma in curr_level_data_dict:
                    if ma in metrics_ordered_lst:
                        result_data_dict[current_ts].update(
                            {OpenSearchResultYield.join_open_search_agg_keys(base_col_name, ma,
                                                                             agg_join_str=keys_seperator):
                                 curr_level_data_dict[ma]['value']})
            else:
                if 'keys' not in result_data_dict[current_ts]:
                    result_data_dict[current_ts]['keys'] = []
                result_data_dict[current_ts]['keys'].append({base_col_name: curr_level_data_dict['doc_count']})
        else:
            # buckets present in curr level dict
            for curr_bucket_value in curr_level_data_dict["buckets"]:
                next_available_bucket_list = list(
                    filter(lambda bucket_name: bucket_name in curr_bucket_value, agg_ordered_lst))
                if curr_bucket == index_col:
                    current_ts = curr_bucket_value["key"]
                    if current_ts not in result_data_dict:
                        if len(metrics_ordered_lst) > 0 or len(next_available_bucket_list) > 0:
                            result_data_dict.update({current_ts: {index_col: current_ts}})
                        else:
                            result_data_dict.update({current_ts: {index_col: current_ts, 'docs_count': curr_bucket_value['doc_count']}})


                else:
                    col_name_builder.set_param(curr_bucket, curr_bucket_value["key"])
                OpenSearchResultYield.__aggregation_result_yield(curr_bucket_value, agg_ordered_lst,
                                                                 metrics_ordered_lst, index_col,
                                                                 col_name_builder,
                                                                 current_ts, result_data_dict,
                                                                 keys_seperator=keys_seperator)

        return result_data_dict

    class ColumnBuilder:
        def set_param(self, k, val):
            setattr(self, k, str(val))

        def get_param(self, k):
            try:
                param_value = getattr(self, k)
            except AttributeError:
                param_value = ''
            return param_value


class BucketTypes:
    DateHistogram = 'date_histogram'
    Terms = 'terms'


class BucketAttributes:
    DefaultBucketSize = 9999
    TimeInterval = 'interval'


class MetricAggregation:
    def __init__(self, aggregation_type, output_name, column_name=None, **kwargs):
        self.aggregation_type = aggregation_type
        self.__column_name = column_name
        self.output_name = output_name
        self.kwargs = kwargs
        if self.__column_name is not None:
            self.kwargs.update(dict(field=self.__column_name))


class FilterColumn:
    def __init__(self, column_name, match_type="term", in_match_type="match_phrase", **kwargs):
        # term for exact match of string, match for string in field value.
        self.column_name = column_name
        self.match_type = match_type
        self.in_match_type = in_match_type  # "terms"
        self.format = kwargs.pop('format', None)
        self.final_exp = None

    def get_final_exp(self):
        return self.final_exp

    def __gt__(self, other):
        if self.format is None:
            self.final_exp = Q(self.match_type, **{self.column_name: {"gt": other}})
        else:
            self.final_exp = Q(self.match_type, **{self.column_name: {"gt": other, "format":self.format}})
        return self

    def __lt__(self, other):
        if self.format is None:
            self.final_exp = Q(self.match_type, **{self.column_name: {"lt": other}})
        else:
            self.final_exp = Q(self.match_type, **{self.column_name: {"lt": other, "format":self.format}})
        return self

    def __ge__(self, other):
        if self.format is None:
            self.final_exp = Q(self.match_type, **{self.column_name: {"gte": other}})
        else:
            self.final_exp = Q(self.match_type, **{self.column_name: {"gte": other, "format":self.format}})
        return self

    def __le__(self, other):
        if self.format is None:
            self.final_exp = Q(self.match_type, **{self.column_name: {"lte": other}})
        else:

            self.final_exp = Q(self.match_type, **{self.column_name: {"lte": other, "format":self.format}})
        return self

    def __in_between__(self, from_value, to_value):

        if self.format is None:
            self.final_exp = Q(self.match_type, **{self.column_name: {"gt": from_value, "lt": to_value}})
        else:
            self.final_exp = Q(self.match_type, **{self.column_name: {"gt": from_value, "lt": to_value}})
        return self

    def __eq__(self, other):
        self.final_exp = Q(self.match_type, **{self.column_name: other})
        return self

    def __ne__(self, other):
        self.final_exp = ~Q(self.match_type, **{self.column_name: other})
        return self

    def __and__(self, other):
        self.final_exp = self.final_exp & other.final_exp
        return self

    def __or__(self, other):
        self.final_exp = self.final_exp | other.final_exp
        return self

    def contains(self, contain_lst):
        return self.__contains__(contain_lst)

    def range_in_between(self, from_value, to_value):
        return self.__in_between__(from_value, to_value)

    def __contains__(self, contain_lst):
        # self.final_exp = Q(self.in_match_type, **{self.column_name: contain_lst})
        self.final_exp = None
        if self.in_match_type == "terms":
            self.final_exp = Q(self.in_match_type, **{self.column_name: contain_lst})
        else:
            for contain_val in contain_lst:
                if self.final_exp:
                    self.final_exp = self.final_exp | Q(self.in_match_type, **{self.column_name: contain_val})
                else:
                    self.final_exp = Q(self.in_match_type, **{self.column_name: contain_val})
        return self


class ScriptField:
    def __init__(self, field_name, script, **kwargs):
        self.field_name = field_name
        self.script = script
        self.kwargs = kwargs


class GroupBY:
    def __init__(self, bucket_type, bucket_name, column_name=None, **kwargs):
        self.bucket_type = bucket_type
        self.__column_name = column_name
        self.bucket_name = bucket_name
        self.kwargs = kwargs
        if self.__column_name is not None:
            self.kwargs.update(dict(field=self.__column_name))


class OpenSearchQueryBuilder:  # facade
    def __init__(self, search_obj=None):
        self.final_bucket_grp = None
        if search_obj is None:
            self.search = Search()
        else:
            self.search = search_obj

    # set attributes
    def set_client(self, client):
        self.search = self.search.using(client=client)

    def set_index(self, index_name):
        self.search = self.search.index(index_name)

    def add_match_text_filters(self, match_expression):
        self.search = QueryBuilder(self.search).add_match_text_filters(match_expression)

    def set_time_range_filter(self, column, from_time, to_time, format="strict_date_optional_time||epoch_millis"):
        self.search = QueryBuilder(self.search).time_range_filter(column, from_time, to_time)

    def set_range_filter(self, range_exp):
        self.search = QueryBuilder(self.search).range_filter(range_exp)

    def set_records_size(self, records_size):
        self.search = QueryBuilder(self.search).records_size(records_size)

    def set_filter_by_columns(self, include_cols):
        self.search = QueryBuilder(self.search).filter_by_columns(include_cols)

    def set_excludes(self, excludes_cols):
        self.search = QueryBuilder(self.search).excludes(excludes_cols)

    def set_group_by_cols_chain(self, bucketing_chain):
        self.search, self.final_bucket_grp = AggsBuilder(self.search).group_by_cols_chain(bucketing_chain)

    def set_aggregate_by(self, metric_chain):
        AggsBuilder(self.search).aggregate_by(metric_chain, self.final_bucket_grp)

    # you can jump into any aspect builders by using these properties
    @property
    def query(self):
        return QueryBuilder(self.search)

    @property
    def aggregations(self):
        return AggsBuilder(self.search)

    def get_search(self):
        return self.search

    def build(self):
        # return self.search
        return self

    def __str__(self) -> str:
        return f'search query: {json.dumps(self.search.to_dict())}'


class AggsBuilder(OpenSearchQueryBuilder):
    def __init__(self, search_obj):
        super().__init__(search_obj)
        self.search = search_obj

    def group_by_cols_chain(self, bucketing_chain_lst):
        """
        When chaining multiple aggregations, there is a difference between what .bucket() and .metric() methods return -
         .bucket() returns the newly defined bucket while .metric() returns its parent bucket to allow further chaining.
        """
        prev_bucket = None
        for grp in bucketing_chain_lst:
            if isinstance(grp, GroupBY):
                grp_kwargs = grp.kwargs
                a = A(grp.bucket_type, **grp_kwargs)
                name = grp.bucket_name
                if not prev_bucket:
                    self.search.aggs.bucket(name, a)
                    prev_bucket = self.search.aggs[name]
                else:
                    prev_bucket.bucket(name, a)
                    prev_bucket = prev_bucket[name]
        return self.search, prev_bucket

    def aggregate_by(self, aggregations_lst, current_aggs):
        if not current_aggs:
            current_aggs = self.search.aggs
        for agg_metric in aggregations_lst:
            metric_kwargs = agg_metric.kwargs
            current_aggs.metric(agg_metric.output_name, agg_metric.aggregation_type, **metric_kwargs)
        return self

    def __str__(self) -> str:
        return f'search query: {json.dumps(self.search.to_dict())}'


class QueryBuilder:
    def __init__(self, search_obj):
        self.search = search_obj

    def add_filters(self):
        """
        GET name_of_index/_search
        {
          "query": {
            "bool": {
              "must": [
                {One or more queries can be specified here.
                A document MUST match all of these queries to be considered as a hit.}
              ],
              "must_not": [
                {A document must NOT match any of the queries specified here.
                It it does, it is excluded from the search results.}
              ],
              "should": [
                {A document does not have to match any queries specified here.
                However, it if it does match, this document is given a higher score.
                should clause does not add or exclude more hits.}
              ],
              "filter": [
                {These filters(queries) place documents in either yes or no category.
                Ones that fall into the yes category are included in the hits. }
              ]
            }
          }
        }
        """
        pass

    def add_match_text_filters(self, match_expression):
        # covered must, must_not, should of bool expression
        return self.search.query(match_expression)

    def filter_by_columns(self, include_cols=None):
        if include_cols:
            self.search = self.search.source(include_cols)
        return self.search

    def excludes(self, excludes_cols):
        if excludes_cols:
            self.search = self.search.source(excludes=excludes_cols)
        return self.search

    def time_range_filter(self, col, from_time, to_time, format="yyyy-MM-dd HH:mm:ss||yyyy-MM-dd HH:mm:ss.SSS||epoch_millis"):
        q = Q('range', **{col: {"gt": from_time, "lte": to_time, "format": format}})
        return self.search.query(q)

    def range_filter(self, range_exp):
        # if from_value is not None and to_value is not None:
        #     q = Q('range', **{col: {"gt": from_value, "lte": to_value}})
        # elif from_value is not None:
        #
        #     q = Q('range', **{col: {"gt": from_value}})
        # elif to_value is not None:
        #
        #     q = Q('range', **{col: {"lte": to_value}})
        # else:
        #     raise Exception('both from value and to value is specified as None')
        return self.search.query(range_exp)

    def records_size(self, num_records):
        return self.search.extra(size=num_records)

    def add_script_fields(self, script_fields_lst):
        for script_field in script_fields_lst:
            if isinstance(script_field, ScriptField):
                self.search = self.search.script_fields(**{script_field.field_name: script_field.script})

    def __str__(self) -> str:
        return f'search query: {json.dumps(self.search.to_dict())}'


class ConnectionParams:
    def __init__(self):
        self._ip_string: str = None
        self._http_compress_on_flag: str = None
        self._user_id: str = None
        self._password: str = None
        self._timeout_in_minutes: int = 10
        self._max_no_persistent_connections: int = 10
        self._sniff_flag: str = False
        self._certificates_path: str = None

    @property
    def certificates_path(self):
        return self.certificates_path

    @certificates_path.setter
    def certificates_path(self, value: str):
        self._certificates_path = value

    @property
    def sniff_flag(self):
        return self._sniff_flag

    @sniff_flag.setter
    def sniff_flag(self, value: str):
        self._sniff_flag = value

    @property
    def max_no_persistent_connections(self):
        return self._max_no_persistent_connections

    @max_no_persistent_connections.setter
    def max_no_persistent_connections(self, value: int):
        self._max_no_persistent_connections = value

    @property
    def timeout_in_minutes(self):
        return self._timeout_in_minutes

    @timeout_in_minutes.setter
    def timeout_in_minutes(self, value: int):
        self._timeout_in_minutes = value

    @property
    def ip_string(self):
        return self._ip_string

    @ip_string.setter
    def ip_string(self, value: str):
        self._ip_string = value

    @property
    def http_compress_on_flag(self):
        return self._http_compress_on_flag

    @http_compress_on_flag.setter
    def http_compress_on_flag(self, value: str):
        self._http_compress_on_flag = value

    @property
    def user_id(self):
        return self._user_id

    @user_id.setter
    def user_id(self, value: str):
        self._user_id = value

    @property
    def password(self):
        return self._password

    @password.setter
    def password(self, value: str):
        self._password = value


class QueryParams:

    def __init__(self):
        self._index_name: str = None
        self._time_column: str = None
        self._time_slice: str = None
        self._num_of_records: int = None
        self._include_cols: List[str] = None
        self._excludes_cols: List[str] = []
        self._match_columns: List[QueryParams.Match] = []
        self._range_filters: List[QueryParams.RangeFilter] = []
        self._contains_columns: List[QueryParams.InFilter] = None
        self._group_by_columns: List[QueryParams.Grp] = []
        self._metric_aggregations: List[QueryParams.MetricAgg] = []
        self._time_range_filter: QueryParams.TimeFilter = None

    @property
    def num_of_records(self):
        return self._num_of_records

    @num_of_records.setter
    def num_of_records(self, value):
        self._num_of_records = value

    @property
    def index_name(self):
        return self._index_name

    @index_name.setter
    def index_name(self, value):
        self._index_name = value

    @property
    def include_cols(self):
        return self._include_cols

    @include_cols.setter
    def include_cols(self, value):
        self._include_cols = value

    @property
    def excludes_cols(self):
        return self._excludes_cols

    @excludes_cols.setter
    def excludes_cols(self, value):
        self._excludes_cols = value

    @property
    def time_column(self):
        return self._time_column

    @time_column.setter
    def time_column(self, value):
        self._time_column = value

    @property
    def time_slice(self):
        return self._time_slice

    @time_slice.setter
    def time_slice(self, value):
        self._time_slice = value

    @property
    def match_columns(self):
        return self._match_columns

    @match_columns.setter
    def match_columns(self, value):
        self._match_columns.append(value)

    @property
    def range_filters(self):
        return self._range_filters

    @range_filters.setter
    def range_filters(self, value):
        self._range_filters.append(value)

    @property
    def metric_aggregations(self):
        return self._metric_aggregations

    @property
    def time_range_filter(self):
        return self._time_range_filter

    @time_range_filter.setter
    def time_range_filter(self, value):
        self._time_range_filter = value

    @metric_aggregations.setter
    def metric_aggregations(self, value):
        self._metric_aggregations = value

    @property
    def group_by_columns(self):
        return self._group_by_columns

    @group_by_columns.setter
    def group_by_columns(self, value):
        self._group_by_columns = value

    @property
    def contains_columns(self):
        return self._contains_columns

    @contains_columns.setter
    def contains_columns(self, value):
        self._contains_columns = value

    class MetricAgg:
        def __init__(self, col, agg, **kwargs):
            self.col = col
            self.agg = agg
            self.kwargs = kwargs

    class Grp:
        def __init__(self, col, is_time_col=False, **kwargs):
            self.col = col
            self.is_time_col = is_time_col
            self.kwargs = kwargs

    class Match:
        def __init__(self, col, val, partial_match=False, **kwargs):
            self.col = col
            self.val = val
            self.partial_match = partial_match
            self.kwargs = kwargs
            if self.partial_match:
                self.kwargs.update(dict(match_type="regexp"))

    class InFilter:
        def __init__(self, col, value, **kwargs):
            self.col = col
            self.val = value
            self.kwargs = kwargs

    class TimeFilter:
        def __init__(self, from_time, to_time, **kwargs):
            self.from_time = from_time
            self.to_time = to_time
            self.kwargs = kwargs

    class RangeFilter:
        def __init__(self, column, from_value=None, to_value=None, **kwargs):
            self.column = column
            self.from_value = from_value
            self.to_value = to_value
            self.kwargs = kwargs

    class TimeSlice:
        Minutely = '1m'


class OpenSearchConnector:
    def __init__(self):
        self.parameter_options = dict()
        self.open_search_hosts = None
        self.open_search_protocol = None
        self.open_search_port = None
        self.user_id = None
        self.password = None
        self.timeout_in_secs = 10
        self.max_no_persistent_connections = 10
        self.sniff_options = dict()
        self.certificates_path = None

    def set_use_ssl(self, use_ssl_flag):
        use_ssl_flag = eval(use_ssl_flag)
        # ssl_assert_hostname = False
        # ssl_show_warn = False
        if use_ssl_flag:
            ssl_context = create_ssl_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

            self.parameter_options.update(
                dict(use_ssl=use_ssl_flag, ssl_context=ssl_context, ssl_assert_hostname=False, ssl_show_warn=True))
        return self

    def set_open_search_nodes(self, nodes):
        self.parameter_options.update(dict(hosts=nodes))
        return self

    def set_http_compress_on(self, http_compress_flag):
        http_compress_flag = eval(http_compress_flag)
        self.parameter_options.update(dict(http_compress=http_compress_flag))
        return self

    def set_certificates_path(self, certificates_path):
        # self.certificates_path = certificates_path
        self.parameter_options.update(dict(ca_certs=certificates_path))
        return self

    def set_timeout_in_minutes(self, timeout_in_mins):
        timeout_in_secs = timeout_in_mins * 60
        self.parameter_options.update(dict(timeout=timeout_in_secs))
        return self

    def set_open_search_protocol(self, open_search_protocol):
        self.open_search_protocol = open_search_protocol
        return self

    def set_user_id_password(self, user_id, password):
        # self.user_id = user_id
        self.parameter_options.update(dict(http_auth=(user_id, password)))
        return self

    def set_persistent_connections_per_node(self, max_size):
        """
        https://elasticsearch-py.readthedocs.io/en/master/#persistent-connections 56
        elasticsearch-py uses persistent connections inside of individual connection pools (one per each configured or sniffed node)

        https://elasticsearch-py.readthedocs.io/en/master/#thread-safety 41
        By default we allow urllib3 to open up to 10 connections to each node, if your application calls for more parallelism, use the maxsize parameter to raise the limit:

        #maxsize parameter for connection poolsize
        es = Opensearch(["host1", "host2"], maxsize=25)

        """
        self.parameter_options.update(dict(maxsize=max_size))
        self.max_no_persistent_connections = max_size
        return self

    def set_sniffer_on(self, sniffer_flag):
        """
        https://elasticsearch-py.readthedocs.io/en/master/#sniffing
        If your application is long-running consider turning on Sniffing 22 to make sure the client is up to date on the cluster location.
        """
        sniffer_flag = eval(sniffer_flag)
        if sniffer_flag:
            sniffer_params = {
                # you can specify to sniff on startup to inspect the cluster and load
                # balance across all nodes
                'sniff_on_start': False,
                # you can also sniff periodically and/or after failure:
                'sniff_on_connection_fail': True
            }
            self.parameter_options.update(sniffer_params)
            self.sniff_options = sniffer_params
        return self

    def get_open_search_protocol(self):
        return self.open_search_protocol

    def get_user_id(self):
        return self.user_id

    def get_password(self):
        return self.password

    def create(self):
        return OpenSearch(**self.parameter_options)


class QueryHelper:
    def __init__(self, connection_params=None):
        self.connection_params: ConnectionParams = connection_params
        self.opensearch_client = None
        self.os_query_builder = None
        self.search_params: QueryParams = None

    def index_exists(self, index_name: str):
        return self.get_connection().indices.exists(index_name)

    def index_doc(self, document: dict, index_name: str, id=None):
        document = json.dumps(document)
        if id:
            self.get_connection().index(
                index=index_name,
                id=id,
                body=document
            )
        else:
            self.get_connection().index(
                index=index_name,
                body=document
            )

    def create_index(self, index_name: str, mapping: dict):
        """
        {
          "mappings": {
            "properties": {
              "year":    { "type" : "text" },
              "age":     { "type" : "integer" },
              "director":{ "type" : "text" }
            }
          }
        }
        """

        self.get_connection().indices.create(index=index_name, ignore=400, body=mapping)
        logger.info(f'index {index_name} got created with following schema: {json.dumps(mapping)}')

    def get_index_mapping(self, index_name):
        return json.dumps(self.get_connection().indices.get_mapping(index=index_name), indent=1)

    def get_connection(self):
        if not self.opensearch_client:
            ip_string = self.connection_params.ip_string
            user_name = self.connection_params.user_id
            password = self.connection_params.password
            connection_time_out_in_mins = self.connection_params.timeout_in_minutes
            sniffer_flag = self.connection_params.sniff_flag
            connections_per_node = self.connection_params.max_no_persistent_connections
            http_compress_on_flag = self.connection_params.http_compress_on_flag
            open_search_client = OpenSearchConnector() \
                .set_open_search_nodes(ip_string) \
                .set_user_id_password(user_name, password) \
                .set_timeout_in_minutes(connection_time_out_in_mins) \
                .set_sniffer_on(sniffer_flag) \
                .set_persistent_connections_per_node(connections_per_node) \
                .set_http_compress_on(http_compress_on_flag).set_use_ssl("False") \
                .create()
            self.opensearch_client = open_search_client
        return self.opensearch_client

    def ping_opensearch(self):
        return self.get_connection().ping()

    def get_search_obj(self, search_params: QueryParams):
        self.os_query_builder = OpenSearchQueryBuilder()
        os_client = self.get_connection()
        self.os_query_builder.set_client(os_client)
        final_search_obj = None
        if search_params is not None:
            self.os_query_builder.set_index(search_params.index_name)
            self.add_query(search_params)
            self.add_aggregations(search_params)
            final_builder = self.os_query_builder.build()
            final_search_obj = final_builder.get_search()
        return final_search_obj

    def add_query(self, search_params):
        txt_match_exp = self.get_text_filter_exp(search_params)
        num_of_records = search_params.num_of_records
        time_range_obj = search_params.time_range_filter
        include_cols = search_params.include_cols
        query_builder = self.os_query_builder.query

        if time_range_obj:
            time_col = search_params.time_column
            query_builder.time_range_filter(col=time_col, from_time=time_range_obj.from_time,
                                            to_time=time_range_obj.to_time)
        if txt_match_exp:
            query_builder.add_match_text_filters(txt_match_exp)

        if num_of_records:
            query_builder.records_size(num_of_records)

        if include_cols:
            query_builder.filter_by_columns(include_cols)

        self.os_query_builder = query_builder

    def get_text_filter_exp(self, search_params):
        expr = None
        if search_params.match_columns:
            for match in search_params.match_columns:
                if isinstance(match.val, list):
                    e = FilterColumn(match.col, **match.kwargs).contains(match.val)
                else:
                    e = (FilterColumn(match.col, **match.kwargs) == match.val)
                if not expr:
                    expr = e
                else:
                    expr = expr & e
        return expr.get_final_exp()

    def add_aggregations(self, search_params):
        time_col = search_params.time_column
        time_slice = search_params.time_slice
        aggs_lst = search_params.group_by_columns
        metric_aggs_lst = search_params.metric_aggregations
        aggs_builder = self.os_query_builder.aggregations
        if aggs_lst:
            bucketing_chain = []
            for agg_obj in aggs_lst:
                if isinstance(agg_obj, QueryParams.Grp):
                    agg_col = agg_obj.col
                    if agg_col != time_col:
                        bucketing_chain.append(GroupBY(column_name=agg_col,
                                                       bucket_type=BucketTypes.Terms,
                                                       bucket_name=agg_obj.col,
                                                       size=BucketAttributes.DefaultBucketSize, **agg_obj.kwargs))
                    else:
                        bucketing_chain.append(GroupBY(column_name=agg_col,
                                                       bucket_type=BucketTypes.DateHistogram,
                                                       bucket_name=agg_obj.col,
                                                       interval=time_slice,
                                                       size=BucketAttributes.DefaultBucketSize, **agg_obj.kwargs))
            aggs_builder.group_by_cols_chain(bucketing_chain)
        if metric_aggs_lst:
            metric_chain = []
            for metric_obj in metric_aggs_lst:
                if isinstance(metric_obj, QueryParams.MetricAgg):
                    metric_chain.append(MetricAggregation(column_name=metric_obj.col, aggregation_type=metric_obj.agg,
                                                          output_name=metric_obj.col, **metric_obj.kwargs))
            aggs_builder.aggregate_by(metric_chain)

        self.os_query_builder = aggs_builder

    def fetch_search_result(self, final_search_obj, search_params):

        if self.ping_opensearch():
            os_client = self.get_connection()
            final_df = pd.DataFrame()
            batch_data_lst = []
            for block in OpenSearchResultYield.search_result_yield_in_batches(os_client,
                                                                              index_name=search_params.index_name,
                                                                              query=final_search_obj.query().to_dict(),
                                                                              block_size=10000,
                                                                              scroll_context_span_bw_requests="5m",
                                                                              raise_on_error=True,
                                                                              preserve_order=False,
                                                                              request_timeout=30,
                                                                              clear_scroll=True,
                                                                              scroll_kwargs=None
                                                                              ):
                batch_data_lst.append(block)

            if len(batch_data_lst) > 0:
                final_df = pd.concat(batch_data_lst, axis=0)
            return final_df
        else:
            logger.info('Connection Ping returned False. So raising Connection Not Found Exception')

    def get_search_obj1(self, search_params: QueryParams):
        self.os_query_builder = OpenSearchQueryBuilder()
        os_client = self.get_connection()
        self.os_query_builder.set_client(os_client)
        final_search_obj = None
        if search_params is not None:
            self.os_query_builder.set_index(search_params.index_name)
            self.add_query(search_params)
            self.add_aggregations(search_params)
            final_builder = self.os_query_builder.build()
            final_search_obj = final_builder.get_search()
        return final_search_obj

    def search_data(self, search_params: QueryParams):
        if self.ping_opensearch():
            q_builder = OpenSearchQueryBuilder()
            os_client = self.get_connection()
            q_builder.set_client(os_client)
            if search_params is not None:
                q_builder.set_index(search_params.index_name)
                expr_result = None
                if search_params.match_columns:
                    for match in search_params.match_columns:
                        if isinstance(match.val, list):
                            exp = FilterColumn(match.col, **match.kwargs).contains(match.val)
                        else:
                            exp = (FilterColumn(match.col, **match.kwargs) == match.val)
                        if not expr_result:
                            expr_result = exp
                        else:
                            expr_result = expr_result & exp
                    q_builder.add_match_text_filters(expr_result.get_final_exp())
                if search_params.range_filters:
                    range_exp_result = None
                    for curr_range_filter in search_params.range_filters:
                        col, from_value, to_value = curr_range_filter.column, curr_range_filter.from_value, curr_range_filter.to_value
                        if from_value is not None and to_value is not None:
                            exp = FilterColumn(column_name=col, match_type='range').range_in_between(from_value, to_value)
                        elif from_value is not None:

                            exp = (FilterColumn(column_name=col, match_type='range') > from_value)
                        elif to_value is not None:

                            exp = FilterColumn(column_name=col, match_type='range') < to_value
                        else:
                            raise Exception('both from value and to value is specified as None')

                        if not range_exp_result:
                            range_exp_result = exp
                        else:
                            range_exp_result = range_exp_result & exp
                    q_builder.set_range_filter(range_exp_result.get_final_exp())

                if search_params.time_column and search_params.time_range_filter:
                    q_builder.set_time_range_filter(column=search_params.time_column,
                                                    from_time=search_params.time_range_filter.from_time,
                                                    to_time=search_params.time_range_filter.to_time)

                if search_params.num_of_records:
                    q_builder.set_records_size(search_params.num_of_records)

                if search_params.include_cols:
                    q_builder.set_filter_by_columns(search_params.include_cols)

                if search_params.excludes_cols:
                    q_builder.set_excludes(search_params.excludes_cols)

                if search_params.group_by_columns:
                    bucketing_chain = []
                    for agg_obj in search_params.group_by_columns:
                        if agg_obj.is_time_col:
                            bucketing_chain.append(GroupBY(column_name=agg_obj.col,
                                                           bucket_type=BucketTypes.DateHistogram,
                                                           bucket_name=agg_obj.col,
                                                           interval=search_params.time_slice,
                                                           **agg_obj.kwargs))
                        else:
                            bucketing_chain.append(GroupBY(column_name=agg_obj.col,
                                                           bucket_type=BucketTypes.Terms,
                                                           bucket_name=agg_obj.col,
                                                           size=BucketAttributes.DefaultBucketSize, **agg_obj.kwargs))
                    q_builder.set_group_by_cols_chain(bucketing_chain)

                if search_params.metric_aggregations:
                    metric_chain = []
                    for metric_obj in search_params.metric_aggregations:
                        metric_chain.append(MetricAggregation(column_name=metric_obj.col,
                                                              aggregation_type=metric_obj.agg,
                                                              output_name=metric_obj.col, **metric_obj.kwargs))
                    q_builder.set_aggregate_by(metric_chain)
                
                print(f'Querying Index - {search_params.index_name} - {q_builder.query}')
                if search_params.group_by_columns:
                    return self.aggregation_search(q_builder.get_search(), search_params)
                else:
                    return self.search(q_builder.get_search(), search_params)
            else:
                logger.error("Search params is empty.")
        else:
            logger.error("Opensearch is not reachable.")
        return None

    def aggregation_search(self, search, search_params):
        agg_ordered_lst, metrics_ordered_lst, index_col = [], [], ''
        if search_params.group_by_columns:
            agg_ordered_lst = [bucket_grp.col for bucket_grp in search_params.group_by_columns]
            index_col = agg_ordered_lst[0]

        if search_params.metric_aggregations:
            metrics_ordered_lst = [metric_obj.col for metric_obj in search_params.metric_aggregations]
        resp = search.execute()
        result = OpenSearchResultYield.aggregation_result_yield(resp, agg_ordered_lst, metrics_ordered_lst,
                                                                index_col, keys_seperator='__')
        return result

    def search(self, search, search_params):
        print(search.query().to_dict())
        os_client = self.get_connection()
        final_df = pd.DataFrame()
        batch_data_lst = []
        for block in OpenSearchResultYield.search_result_yield_in_batches(os_client,
                                                                          search_params=search_params,
                                                                          query=search.query().to_dict(),
                                                                          block_size=10000,
                                                                          scroll_context_span_bw_requests="5m",
                                                                          raise_on_error=True,
                                                                          preserve_order=False,
                                                                          request_timeout=30,
                                                                          clear_scroll=True,
                                                                          scroll_kwargs=None
                                                                          ):
            batch_data_lst.append(block)

        if len(batch_data_lst) > 0:
            final_df = pd.concat(batch_data_lst, axis=0)
        return final_df


def helper_test_case():
    c = ConnectionParams()
    c.ip_string = '192.168.13.171:9200'
    c.user_id = 'admin'
    c.password = 'admin'
    c.timeout_in_minutes = 15
    c.max_no_persistent_connections = 15
    c.sniff_flag = 'True'
    c.http_compress_on_flag = 'True'
    # ===================================== #
    q = QueryParams()

    q.index_name = 'heal_signals_heal_health_*'
    q.time_column = '@timestamp'
    q.include_cols = ["signalId", "anomalies", "metadata"]
    match_ref = QueryParams.Match
    in_filter_ref = QueryParams.InFilter
    time_filter_ref = QueryParams.TimeFilter
    q.match_columns = match_ref("metadata.account_id", "heal_health")
    # q.match_columns = [match_ref("metadata.account_id", ["heal_health", "heal_health1"])]
    signal_ids = ['E-2-1081-19-1651772940', 'E-2-1081-19-1651756740', 'E-2-1081-19-1651734540']
    # q.contains_columns = [in_filter_ref('signalId', signal_ids)]
    q.match_columns = match_ref("signalId", signal_ids)
    from_date = "2022-05-04T05:27:53.031Z"
    to_date = "2022-05-11T05:27:53.031Z"
    q.time_range_filter = time_filter_ref(from_date, to_date)
    # ============================================== #

    query_helper = QueryHelper(c)
    # df = query_helper.fetch_data(q)
    df = query_helper.search_data(q)
    print(df)


def helper_test_case2():
    c = ConnectionParams()
    c.ip_string = '192.168.13.171:9200'
    c.user_id = 'admin'
    c.password = 'admin'
    c.timeout_in_minutes = 15
    c.max_no_persistent_connections = 15
    c.sniff_flag = 'True'
    c.http_compress_on_flag = 'True'
    # ===================================== #
    q = QueryParams()

    q.index_name = 'heal_signals_heal_health_*'
    q.time_column = '@timestamp'
    # q.include_cols = ["signalId", "anomalies", "metadata"]
    match_ref = QueryParams.Match
    in_filter_ref = QueryParams.InFilter
    time_filter_ref = QueryParams.TimeFilter
    mtric_aggs = QueryParams.MetricAgg
    grp_by_ref = QueryParams.Grp
    q.match_columns = match_ref("metadata.account_id", "heal_health")
    # q.match_columns = [match_ref("metadata.account_id", ["heal_health", "heal_health1"])]
    signal_ids = ['E-2-1081-19-1651772940', 'E-2-1081-19-1651756740', 'E-2-1081-19-1651734540']
    # q.contains_columns = [in_filter_ref('signalId', signal_ids)]
    q.match_columns = match_ref("signalId", signal_ids)
    from_date = "2022-05-04T05:27:53.031Z"
    to_date = "2022-05-11T05:27:53.031Z"
    q.time_range_filter = time_filter_ref(from_date, to_date)
    # ============================================== #
    # q.group_by_columns = [grp_by_ref("signalId"), grp_by_ref("anomalies")]
    q.group_by_columns = [grp_by_ref("signalId")]
    q.metric_aggregations = [QueryParams.MetricAgg('severityId', 'sum')]
    query_helper = QueryHelper(c)
    # df = query_helper.fetch_data(q)
    df = query_helper.search_data(q)
    print(df)


def helper_test_case3():
    c = ConnectionParams()
    c.ip_string = '192.168.13.105:9201'
    c.user_id = 'admin'
    c.password = 'admin'
    c.timeout_in_minutes = 15
    c.max_no_persistent_connections = 15
    c.sniff_flag = 'True'
    c.http_compress_on_flag = 'True'
    # ===================================== #
    q = QueryParams()

    q.index_name = 'atnt-aioneops-normalized-anomalies-v1-*'
    q.time_column = 'createdDateTime'
    from_date = '2022-05-25T12:18:15.768Z'
    to_date = '2022-05-25T12:20:15.768Z'
    # from_date = pd.to_datetime('2022-05-25 12:18:15').strftime('%S')
    # to_date = pd.to_datetime('2022-05-25 12:20:15').strftime('%s')
    time_filter_ref = QueryParams.TimeFilter
    q.time_range_filter = time_filter_ref(from_date, to_date)

    q.match_columns = QueryParams.Match("sourceName", "aioneopsinsights")

    query_helper = QueryHelper(c)
    df = query_helper.search_data(q)
    print(df)
    # from_date = "2022-05-04T05:27:53.031Z"
    # to_date = "2022-05-11T05:27:53.031Z"

# if __name__ == '__main__':
#     from app.data_access.util import get_os_connection_params
#     d = "{\"alertUrl\":\"4rt\"}"
#     q = query_helper = QueryHelper(get_os_connection_params())
#     q.index_doc(d, 'abc')
