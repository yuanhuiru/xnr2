# -*- coding: utf-8 -*-
import arrow
import time
import sys
import json
reload(sys)
sys.path.append('../../')
sys.path.append('/home/xnr1/xnr_0429/xnr')
sys.path.append('/home/xnr1/xnr_0429')
from global_utils import es_xnr_2 ,es_xnr, info_monitor_index_name_pre, info_monitor_index_type,\
     fb_xnr_index_name, fb_xnr_index_type, facebook_xnr_relations_index_name, facebook_xnr_relations_index_type,\
     tw_xnr_index_name, tw_xnr_index_type, facebook_xnr_relations_index_name, facebook_xnr_relations_index_type
from info_monitor_mappings import info_monitor_mappings
from keyword_info_monitor_utils import search_posts as search_posts_keywords
from user_info_monitor_utils import load_posts as search_posts_users
#from overseas_user_info_monitor_utils import #load_fb_posts as search_fb_posts_users,\
#    load_tw_posts as search_tw_posts_users
from overseas_user_info_monitor_utils import load_tw_posts as search_tw_posts_users
from overseas_keyword_info_monitor_utils import search_fb_posts as search_fb_posts_keywords,\
    search_tw_posts as search_tw_posts_keywords


def datetime2ts(date):
    return int(time.mktime(time.strptime(date, '%Y-%m-%d')))


def load_xnr_uid_query():
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
    return query_body


# 查找facebook所有虚拟人
def load_fb_xnr_user_no():
    query_body = load_xnr_uid_query()
    search_results = es_xnr_2.search(fb_xnr_index_name, fb_xnr_index_type, query_body)['hits']['hits']
    return [item['_source']['xnr_user_no'] for item in search_results]


# 查找twitter所有虚拟人
def load_tw_xnr_user_no():
    query_body = load_xnr_uid_query()
    search_results = es_xnr_2.search(tw_xnr_index_name, tw_xnr_index_type, query_body)['hits']['hits']
    return [item['_source']['xnr_user_no'] for item in search_results]


def main():
    # 出来date 2019-04-01
    # info_monitor_index_name_pre = 'info_monitor_'
    # info_monitor_index_type = 'text'

    date = arrow.now().shift(days=-1).format("YYYY-MM-DD")
    print date
    from_ts = datetime2ts(date)
    to_ts = from_ts + 24 * 3600
    index_name = info_monitor_index_name_pre + date
    #print index_name
    info_monitor_mappings(date)
    # print '1111111111111111加载facebook所有虚拟人'
    # fb_xnr_user_no = load_fb_xnr_user_no()
    # fb_xnr_user_no.append('ALL')

    print '1111111111111111加载twitter所有虚拟人'
    tw_xnr_user_no_list = load_tw_xnr_user_no()
    tw_xnr_user_no_list.append('ALL')

    # print '2222222222222222facebook用户监测'
    # # facebook 用户监测
    # for fb_xnr_user_no in load_fb_xnr_user_no():
    #     try:
    #         for data in search_fb_posts_users(fb_xnr_user_no, from_ts, to_ts):
    #             es_xnr.index(
    #                 index=index_name,
    #                 doc_type=info_monitor_index_type,
    #                 body={
    #                     'xnr_no': fb_xnr_user_no,
    #                     'platform': 'facebook',
    #                     'type': 'users',
    #                     'content': json.dumps(data)}
    #                         )
    #     except Exception, e:
    #         print e
    #     print '2222222222222222facebook关键词监测'
    #     try:
    #         for data in search_fb_posts_keywords(fb_xnr_user_no, from_ts, to_ts):
    #             es_xnr.index(
    #                 index=index_name,
    #                 doc_type=info_monitor_index_type,
    #                 body={
    #                     'xnr_no': fb_xnr_user_no,
    #                     'platform': 'facebook',
    #                     'type': 'keywords',
    #                     'content': json.dumps(data)}
    #             )
    #     except Exception, e:
    #         print e

    print '3333333333333333333twitter用户监测'
    # twitter 用户监测
    #from_ts = 1566489600
    #to_ts = 1567492401
    for tw_xnr_user_no in tw_xnr_user_no_list:
        try:
            for data in search_tw_posts_users(tw_xnr_user_no, from_ts, to_ts):
                es_xnr.index(
                    index=index_name,
                    doc_type=info_monitor_index_type,
                    body={
                        'xnr_no': tw_xnr_user_no,
                        'platform': 'twitter',
                        'type': 'users',
                        'content': json.dumps(data)}
                )
        except Exception, e:
            print e

        try:
            for data in search_tw_posts_keywords(tw_xnr_user_no, from_ts, to_ts):
                print data
                es_xnr.index(
                     index = index_name,
                     doc_type = info_monitor_index_type,
                     body = {
                         'xnr_no': tw_xnr_user_no,
                         'platform': 'twitter',
                         'type': 'keywords',
                         'content': json.dumps(data)}
                 )
        except Exception,e:
            print e


if __name__ == '__main__':
    main()
