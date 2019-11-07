#coding:utf8
import json
import sys
from elasticsearch import Elasticsearch


sys.path.append('/home/xnr1/xnr_0429/xnr')
sys.path.append('/home/xnr1/xnr_0429')
from parameter import MAX_DETECT_COUNT, MAX_FLOW_TEXT_DAYS, MAX_SEARCH_SIZE, FB_TW_TOPIC_ABS_PATH, FB_DOMAIN_ABS_PATH, \
     DAY, WEEK, fb_tw_topic_en2ch_dict as topic_en2ch_dict, SENTIMENT_DICT_NEW, SORT_FIELD, \
     TOP_KEYWORDS_NUM, TOP_WEIBOS_LIMIT, WEEK_TIME, DAY, DAY_HOURS, HOUR, \
     fb_tw_topic_ch2en_dict as topic_ch2en_dict, WORD2VEC_PATH

from global_utils import es_xnr_2, facebook_xnr_relations_index_name, facebook_xnr_relations_index_type,\
     facebook_flow_text_index_name_pre, facebook_flow_text_index_type,\
     twitter_flow_text_index_type, twitter_flow_text_index_name_pre,\
     twitter_xnr_relations_index_name, twitter_xnr_relations_index_type

MAX_SEARCH_SIZE = 99

def load_uid_query():
    query_body = {
        'query': {
            'filtered': {
                'filter': {
                    'bool': {
                        'must': [
                            {'term': {'yewuguanzhu': 1}},
                        ]
                    }
                }
            }
        },
        'size': MAX_SEARCH_SIZE,
    }
    return query_body


def load_search_query(uids, from_ts, to_ts):
    query_body = {
        'query': {
            "filtered": {
                "filter": {
                    "bool": {
                        "must": [
                            {"terms": {"uid": uids}},
                            {"query": {
                                "range": {
                                    "timestamp": {
                                        "gte": from_ts,
                                        "lte": to_ts
                                    }
                                }
                            }
                            }
                        ],
                    }
                }
            }
        },
        'size': MAX_SEARCH_SIZE,
        "sort": {"timestamp": {"order": "desc"}},
    }
    return query_body

#
# def load_fb_uids(xnr_user_no):
#     query_body = load_uid_query()
#     if xnr_user_no != 'ALL':
#         query_body['query']['filtered']['filter']['bool']['must'].append({'term': {'xnr_no': xnr_user_no}})
#     search_results = es_xnr_2.search(index=facebook_xnr_relations_index_name, doc_type=facebook_xnr_relations_index_type, body=query_body)['hits']['hits']
#     return [item['_source']['uid'] for item in search_results]


def load_tw_uids(xnr_user_no):
    query_body = load_uid_query()
    if xnr_user_no != 'ALL':
        query_body['query']['filtered']['filter']['bool']['must'].append({'term': {'xnr_no': xnr_user_no}})
    search_results = es_xnr_2.search(index=twitter_xnr_relations_index_name, doc_type=twitter_xnr_relations_index_type, body=query_body)['hits']['hits']
    return [item['_source']['uid'] for item in search_results]


# def search_fb_posts(uids, from_ts, to_ts):
#     query_body = load_search_query(uids, from_ts, to_ts)
#     print query_body
#     search_results = es_xnr_2.search(index=facebook_flow_text_index_name_pre + '*', doc_type=facebook_flow_text_index_type, body=query_body)['hits']['hits']
#     return [item['_source'] for item in search_results]


def search_tw_posts(uids, from_ts, to_ts):
    query_body = load_search_query(uids, from_ts, to_ts)
    print query_body
    search_results = es_xnr_2.search(index=twitter_flow_text_index_name_pre + '*', doc_type=twitter_flow_text_index_type, body=query_body)['hits']['hits']
    return [item['_source'] for item in search_results]

# def load_fb_posts(xnr_user_no, from_ts, to_ts):
#     uids = load_fb_uids(xnr_user_no)
#     fb_posts = search_fb_posts(uids, from_ts, to_ts)
#     return fb_posts


def load_tw_posts(xnr_user_no, from_ts, to_ts):
    uids = load_tw_uids(xnr_user_no)
    tw_posts = search_tw_posts(uids, from_ts, to_ts)
    return tw_posts


if __name__ == '__main__':
    # print load_fb_posts('FXNR0152', 1550764800, 1550841403)
    print load_tw_posts('TXNR0024', 1566489600, 1566921600)




