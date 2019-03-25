# _*_coding:utf-8_*_
import json
import sys
from elasticsearch import Elasticsearch

sys.path.append('/home/xnr1/xnr_0429/xnr')
sys.path.append('/home/xnr1/xnr_0429')
from parameter import MAX_DETECT_COUNT, MAX_FLOW_TEXT_DAYS, MAX_SEARCH_SIZE, FB_TW_TOPIC_ABS_PATH, FB_DOMAIN_ABS_PATH, \
    DAY, WEEK, fb_tw_topic_en2ch_dict as topic_en2ch_dict, SENTIMENT_DICT_NEW, SORT_FIELD, \
    TOP_KEYWORDS_NUM, TOP_WEIBOS_LIMIT, WEEK_TIME, DAY, DAY_HOURS, HOUR, \
    fb_tw_topic_ch2en_dict as topic_ch2en_dict, WORD2VEC_PATH
from global_utils import es_flow_text,flow_text_index_name_pre, flow_text_index_type,\
    weibo_xnr_relations_index_name, weibo_xnr_relations_index_type, es_xnr

MAX_SEARCH_SIZE = 99

def load_uids(xnr_user_no):
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

    if xnr_user_no != 'ALL':
        query_body['query']['filtered']['filter']['bool']['must'].append({'term': {'xnr_no': xnr_user_no}})
    print query_body
    search_results = es_xnr.search(index=weibo_xnr_relations_index_name, doc_type=weibo_xnr_relations_index_type, body=query_body)['hits']['hits']
    return [item['_source']['uid'] for item in search_results]

def search_posts(uids, from_ts, to_ts):
    query_body = {
        'query':{
            "filtered":{
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
    search_results = es_flow_text.search(index=flow_text_index_name_pre + '*', doc_type=flow_text_index_type, body=query_body)['hits']['hits']
    return [item['_source'] for item in search_results]

def load_posts(xnr_user_no, from_ts, to_ts):
    uids = load_uids(xnr_user_no)
    print uids
    posts = search_posts(uids, from_ts, to_ts)
    return posts

if __name__ == '__main__':
    print load_posts('WXNR0152', 1550764800, 1550841403)



