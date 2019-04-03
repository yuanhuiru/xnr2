# -*- coding: utf-8 -*-
import json
import time
import arrow
import sys
reload(sys)
sys.path.append('../../')
sys.path.append('/home/xnr1/xnr_0429/xnr')
sys.path.append('/home/xnr1/xnr_0429')
from global_utils import es_xnr as es,\
    info_monitor_index_name_pre, info_monitor_index_type,\
    weibo_xnr_index_name, weibo_xnr_index_type,\
    weibo_xnr_relations_index_name,weibo_xnr_relations_index_type
from info_monitor_mappings import info_monitor_mappings
from keyword_info_monitor_utils import search_posts as search_posts_keywords
from user_info_monitor_utils import load_posts as search_posts_users


def datetime2ts(date):
    return int(time.mktime(time.strptime(date, '%Y-%m-%d')))


def load_xnr_user_no():
    query_body = {
        'query': {
            'bool': {
                'must': [
                    {
                        'term': {
                            'create_status': 2
                        }
                    }
                ],
            }
        },
        'size': 1000
    }
    search_results = es.search(weibo_xnr_index_name, weibo_xnr_index_type, query_body)['hits']['hits']
    return [item['_source']['xnr_user_no'] for item in search_results]


def main():
    date = arrow.now().shift(days=-1).format('YYYY-MM-DD')
    from_ts = datetime2ts(date)
    to_ts = from_ts + 24*3600
    index_name = info_monitor_index_name_pre + date
    info_monitor_mappings(date)
    xnr_user_no_list = load_xnr_user_no() 
    xnr_user_no_list.append('ALL')
    
    for xnr_user_no in xnr_user_no_list:
        try:
            for data in search_posts_users(xnr_user_no, from_ts, to_ts):
                es.index(
                    index = index_name,
                    doc_type = info_monitor_index_type,
                    body = {
                        'xnr_no': xnr_user_no,
                        'platform': 'weibo',
                        'type': 'users',
                        'content': json.dumps(data)}
                )
        except Exception,e:
            print e

        try:
            for data in search_posts_keywords(xnr_user_no, from_ts, to_ts):
                es.index(
                    index = index_name,
                    doc_type = info_monitor_index_type,
                    body = {
                        'xnr_no': xnr_user_no,
                        'platform': 'weibo',
                        'type': 'keywords',
                        'content': json.dumps(data)}
                )
        except Exception,e:
            print e

if __name__ == '__main__':
    main()



