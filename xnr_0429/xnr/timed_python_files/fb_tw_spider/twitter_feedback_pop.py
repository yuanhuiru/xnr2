# -*- coding:utf-8 -*-
import os
import json
import time
import random
import redis
from elasticsearch import Elasticsearch
import sys

sys.path.append('../../')
from global_utils import es_xnr_2 as es
from global_utils import twitter_feedback_comment_index_name_pre, twitter_feedback_comment_index_type, \
    twitter_feedback_retweet_index_name_pre, twitter_feedback_retweet_index_type, \
    twitter_feedback_private_index_name_pre, twitter_feedback_private_index_type, \
    twitter_feedback_at_index_name_pre, twitter_feedback_at_index_type, \
    twitter_feedback_like_index_name_pre, twitter_feedback_like_index_type, \
    tw_xnr_flow_text_index_name_pre, tw_xnr_flow_text_index_type,\
    twitter_feedback_guanzhuhuifen_index_name, twitter_feedback_guanzhuhuifen_index_type
from global_utils import tw_xnr_index_name, tw_xnr_index_type, \
    tw_xnr_fans_followers_index_name, tw_xnr_fans_followers_index_type

from time_utils import ts2datetime, datetime2ts
from xnr_relations_utils import update_twitter_xnr_relations

sys.path.append('../')
from twitter_feedback_mappings_timer import twitter_feedback_like_mappings, \
    twitter_feedback_retweet_mappings, twitter_feedback_fans_mappings,\
    twitter_feedback_at_mappings, twitter_feedback_comment_mappings,\
    twitter_feedback_private_mappings, twitter_feedback_guanzhuhuifen_mappings,\
    twitter_feedback_follow_mappings
from tw_xnr_manage_mappings import tw_xnr_fans_followers_mappings

sys.path.append('../../twitter/sensitive')
from get_sensitive import get_sensitive_info, get_sensitive_user
from tw_xnr_flow_text_mappings import tw_xnr_flow_text_mappings
from global_utils import twitter_xnr_relations_index_name, twitter_xnr_relations_index_type


r_spider = redis.Redis(host='47.94.133.29', port=9506)
EXCEPTION = ''
redis_keys4tw_spider_data = [
    'twitter_feedback_comment_data',
    'twitter_feedback_like_data',
    'twitter_feedback_retweet_data',
    'twitter_feedback_private_data',
    'twitter_feedback_at_data',
    'twitter_feedback_guanzhuhuifen_data',
    'twitter_feedback_fensi_exist_data',
    'twitter_feedback_guanzhu_exist_data'
]


def load_twitter_relations_base(xnr_user_no, relations_type):
    """
    :param xnr_user_no:
    :param relations_type: pingtaiguanzhu 或者 pingtaifensi
    :return:
    """
    li = []
    query_body = {
        'query': {
            'bool': {
                'must': [
                    {'term': {'xnr_no': xnr_user_no}},
                    {'term': {relations_type: 1}}
                ]
            }
        },
        'size': 99999
    }
    search_res = es.search(twitter_xnr_relations_index_name, twitter_xnr_relations_index_type, query_body)['hits']['hits']
    for item in search_res:
        li.append(item['_source']['uid'])
    return li


def load_twitter_relations(xnr_user_no):
    """
    返回twitter xnr的粉丝列表，关注列表
    :param xnr_user_no:
    :return:
    """
    guanzhu_list = load_twitter_relations_base(xnr_user_no, 'pingtaiguanzhu')
    fensi_list = load_twitter_relations_base(xnr_user_no, 'pingtaifensi')
    return guanzhu_list, fensi_list


def check_relation(xnr_info, uid):
    """
    根据粉丝列表、关注列表、用户uid，判断该用户是不是与xnr存在关注等关系
    :param xnr_info:
    :param uid:
    :return:
    """
    guanzhu_list = xnr_info['guanzhu_list']
    fensi_list = xnr_info['fensi_list']
    if (uid in guanzhu_list) and (uid in fensi_list):
        return u'双向关注'
    elif uid in guanzhu_list:
        return u'关注对象'
    elif uid in fensi_list:
        return u'粉丝'
    else:
        return u'陌生人'


def load_xnr_info():
    res = []
    search_res = es.search(tw_xnr_index_name, tw_xnr_index_type, {'size': 999})['hits']['hits']
    for item in search_res:
        source = item['_source']
        tw_mail_account = source.get('tw_mail_account', '')
        tw_phone_account = source.get('tw_phone_account', '')
        account = ''
        if tw_mail_account:
            account = tw_mail_account
        elif tw_phone_account:
            account = tw_phone_account
        if account:
            xnr_user_no = source.get('xnr_user_no', '')
            guanzhu_list, fensi_list = load_twitter_relations(xnr_user_no)

            info = {
                'root_uid': source.get('uid', ''),
                'root_nick_name': source.get('nick_name', ''),
                'xnr_user_no': xnr_user_no,
                'account': account,
                'password': source.get('password', ''),
                'guanzhu_list': guanzhu_list,
                'fensi_list': fensi_list
            }
            res.append(info)
    return res


def sensitive_func(ts, text, uid):
    sensitive_info = get_sensitive_info(timestamp=ts, text=text)
    sensitive_user = get_sensitive_user(timestamp=ts, uid=uid)
    return sensitive_info, sensitive_user


'''
如果记录已经存在twitter_feedback_comment*中，则update；反之，则index
'''
def savedata2es(date, index_pre, index_type, data):
    config = {
        'twitter_feedback_like_': ['uid', 'root_uid', 'timestamp', 'text', 'root_text', 'root_mid'],
        'twitter_feedback_comment_': ['uid', 'root_uid', 'mid', 'timestamp', 'text', 'root_text', 'root_mid', 'comment_type'],
        'twitter_feedback_retweet_': ['uid', 'root_uid', 'mid', 'timestamp', 'text', 'root_text', 'root_mid'],
        'twitter_feedback_private_': ['uid', 'root_uid', 'timestamp', 'text', 'root_text', 'private_type'],
        'twitter_feedback_guanzhuhuifen': ['uid', 'root_uid'],
        'twitter_feedback_at_': ['uid', 'root_uid', 'mid', 'timestamp', 'text'],
    }
    if index_pre in ['twitter_feedback_at_', 'twitter_feedback_comment_', 'twitter_feedback_retweet_',
                     'twitter_feedback_private_', 'twitter_feedback_like_']:
        index_name = index_pre + date
        search_index_name = index_pre + '*'
    else:
        index_name = index_pre
        search_index_name = index_name
    for d in data:
        query_body = {
            "query": {
                "filtered": {
                    "filter": {
                        "bool": {
                            "must": [
                            ]
                        }
                    }
                }
            }
        }
        try:
            for field in config[index_pre]:
                query_body['query']['filtered']['filter']['bool']['must'].append({'term': {field: d.get(field, '')}})
            query_result = es.search(search_index_name, index_type, query_body)['hits']['hits']
            if query_result:
                print es.update(index=index_name, doc_type=index_type, body={'doc': d}, id=query_result[0]['_id'])
            else:
                print es.index(index_name, index_type, d)
        except Exception, e:
            print e


def load_data(xnr_user_no, redis_key):
    lis = []
    for i in range(r_spider.llen(redis_key)):
        res_str = r_spider.rpop(redis_key)
        res = json.loads(res_str)
        if res.keys()[0] == xnr_user_no:
            lis = res.values()[0]
            break
        else:
            r_spider.lpush(redis_key, res_str)
    return lis

'''
# 评论
def comment(xnr_info, date):
    global EXCEPTION
    ts = datetime2ts(date)
    twitter_feedback_comment_mappings(twitter_feedback_comment_index_name_pre + date)
    redis_key = 'twitter_feedback_comment_data'
    xnr_user_no = xnr_info['xnr_user_no']
    lis = load_data(xnr_user_no, redis_key)

    # {'uid', 'nick_name', 'mid', 'timestamp', 'text', 'update_time', 'root_text', 'root_mid', 'comment_type'}
    data = []
    for item in lis:
        try:
            uid = item['uid']
            text = item['text']
            twitter_type = check_relation(xnr_info, uid)
            sensitive_info, sensitive_user = sensitive_func(ts, text, uid)
            d = {
                'uid': uid,
                'text': text,
                'nick_name': item['nick_name'],
                'mid': item['mid'],
                'timestamp': item['timestamp'],
                'update_time': item['update_time'],
                'root_text': item['root_text'],
                'root_mid': item['root_mid'],
                'comment_type': item['comment_type'],
                'photo_url': '',
                'root_uid': xnr_info['root_uid'],
                'root_nick_name': xnr_info['root_nick_name'],
                'twitter_type': twitter_type,
                'sensitive_info': sensitive_info,
                'sensitive_user': sensitive_user
            }
            data.append(d)
        except Exception, e:
            EXCEPTION += '\n comment Exception: ' + str(e)
    savedata2es(date, twitter_feedback_comment_index_name_pre, twitter_feedback_comment_index_type, data)


# 点赞
def like(xnr_info, date):
    global EXCEPTION
    ts = datetime2ts(date)
    twitter_feedback_like_mappings(twitter_feedback_like_index_name_pre + date)
    redis_key = 'twitter_feedback_like_data'
    xnr_user_no = xnr_info['xnr_user_no']
    lis = load_data(xnr_user_no, redis_key)

    # {'uid', 'photo_url', 'nick_name', 'timestamp', 'root_text', 'update_time', 'root_mid'}
    data = []
    for item in lis:
        try:
            uid = item['uid']
            nick_name = item['nick_name']
            text = nick_name + u" 赞同了您的帖子。"
            twitter_type = check_relation(xnr_info, uid)
            sensitive_info, sensitive_user = sensitive_func(ts, text, uid)
            d = {
                'uid': uid,
                'text': text,
                'nick_name': nick_name,
                'timestamp': item['timestamp'],
                'update_time': item['update_time'],
                'root_text': item['root_text'],
                'root_mid': item['root_mid'],
                'photo_url': item['photo_url'],
                'root_uid': xnr_info['root_uid'],
                'root_nick_name': xnr_info['root_nick_name'],
                'twitter_type': twitter_type,
                'sensitive_info': sensitive_info,
                'sensitive_user': sensitive_user
            }
            data.append(d)
        except Exception, e:
            EXCEPTION += '\n like Exception: ' + str(e)
    savedata2es(date, twitter_feedback_like_index_name_pre, twitter_feedback_like_index_type, data)


# 分享
def retweet(xnr_info, date):
    global EXCEPTION
    ts = datetime2ts(date)
    twitter_feedback_retweet_mappings(twitter_feedback_retweet_index_name_pre + date)
    redis_key = 'twitter_feedback_retweet_data'
    xnr_user_no = xnr_info['xnr_user_no']
    lis = load_data(xnr_user_no, redis_key)

    # {'uid', 'nick_name', 'mid', 'timestamp', 'text', 'update_time', 'root_text', 'root_mid'}
    data = []
    for item in lis:
        try:
            uid = item['uid']
            text = item['text']
            twitter_type = check_relation(xnr_info, uid)
            sensitive_info, sensitive_user = sensitive_func(ts, text, uid)
            d = {
                'uid': uid,
                'text': text,
                'nick_name': item['nick_name'],
                'mid': item['mid'],
                'timestamp': item['timestamp'],
                'update_time': item['update_time'],
                'root_text': item['root_text'],
                'root_mid': item['root_mid'],
                'photo_url': '',
                'root_uid': xnr_info['root_uid'],
                'root_nick_name': xnr_info['root_nick_name'],
                'twitter_type': twitter_type,
                'sensitive_info': sensitive_info,
                'sensitive_user': sensitive_user,
                'retweet': 0,
                'comment': 0,
                'like': 0
            }
            data.append(d)
        except Exception, e:
            EXCEPTION += '\n retweet Exception: ' + str(e)
    savedata2es(date, twitter_feedback_retweet_index_name_pre, twitter_feedback_retweet_index_type, data)


# 私信
def private(xnr_info, date):
    global EXCEPTION
    ts = datetime2ts(date)
    twitter_feedback_private_mappings(twitter_feedback_private_index_name_pre + date)
    redis_key = 'twitter_feedback_private_data'
    xnr_user_no = xnr_info['xnr_user_no']
    lis = load_data(xnr_user_no, redis_key)

    # {'uid', 'photo_url', 'nick_name', 'timestamp', 'update_time', 'text', 'root_text', 'private_type'}
    data = []
    for item in lis:
        try:
            uid = item['uid']
            text = item['text']
            twitter_type = check_relation(xnr_info, uid)
            sensitive_info, sensitive_user = sensitive_func(ts, text, uid)
            d = {
                'uid': uid,
                'text': text,
                'nick_name': item['nick_name'],
                'timestamp': item['timestamp'],
                'update_time': item['update_time'],
                'root_text': item['root_text'],
                'private_type': item['private_type'],
                'photo_url': item['photo_url'],
                'root_uid': xnr_info['root_uid'],
                'root_nick_name': xnr_info['root_nick_name'],
                'twitter_type': twitter_type,
                'sensitive_info': sensitive_info,
                'sensitive_user': sensitive_user
            }
            data.append(d)
        except Exception, e:
            EXCEPTION += '\n private Exception: ' + str(e)
    savedata2es(date, twitter_feedback_private_index_name_pre, twitter_feedback_private_index_type, data)


# 关注回粉
def guanzhuhuifen(xnr_info, date):
    global EXCEPTION
    ts = datetime2ts(date)
    twitter_feedback_guanzhuhuifen_mappings()
    redis_key = 'twitter_feedback_guanzhuhuifen_data'
    xnr_user_no = xnr_info['xnr_user_no']
    lis = load_data(xnr_user_no, redis_key)

    # {'uid', 'nick_name', 'profile_url', 'photo_url', 'fensi_num', 'guanzhu_num', 'update_time'}
    data = []
    for item in lis:
        try:
            uid = item['uid']
            twitter_type = check_relation(xnr_info, uid)
            d = {
                'uid': uid,
                'nick_name': item['nick_name'],
                'update_time': item['update_time'],
                'photo_url': item['photo_url'],
                'fensi_num': item['fensi_num'],
                'guanzhu_num': item['guanzhu_num'],
                'profile_url': item['profile_url'],
                'root_uid': xnr_info['root_uid'],
                'root_nick_name': xnr_info['root_nick_name'],
                'twitter_type': twitter_type,
            }
            data.append(d)
        except Exception, e:
            EXCEPTION += '\n guanzhuhuifen Exception: ' + str(e)
    savedata2es(date, twitter_feedback_guanzhuhuifen_index_name, twitter_feedback_guanzhuhuifen_index_type, data)
'''


# 点赞
def at(xnr_info, date):
    global EXCEPTION
    ts = datetime2ts(date)
    twitter_feedback_at_mappings(twitter_feedback_at_index_name_pre + date)
    redis_key = 'twitter_feedback_at_data'
    xnr_user_no = xnr_info['xnr_user_no']
    lis = load_data(xnr_user_no, redis_key)

    # ['update_time', 'uid', 'text', 'nick_name', 'timestamp', 'user_name', 'mid', 'photo_url']
    data = []
    for item in lis:
        try:
            uid = item['uid']
            text = item['text']
            twitter_type = check_relation(xnr_info, uid)
            sensitive_info, sensitive_user = sensitive_func(ts, text, uid)
            d = {
                'uid': uid,
                'text': text,
                'nick_name': item['nick_name'],
                'mid': item['mid'],
                'timestamp': item['timestamp'],
                'update_time': item['update_time'],
                'photo_url': item['photo_url'],
                'root_uid': xnr_info['root_uid'],
                'twitter_type': twitter_type,
                'sensitive_info': sensitive_info,
                'sensitive_user': sensitive_user
            }
            data.append(d)
        except Exception, e:
            EXCEPTION += '\n at Exception: ' + str(e)
    savedata2es(date, twitter_feedback_at_index_name_pre, twitter_feedback_at_index_type, data)


# 关注列表
def guanzhu_exist(xnr_info, date):
    global EXCEPTION
    ts = datetime2ts(date)
    twitter_feedback_follow_mappings()
    redis_key = 'twitter_feedback_guanzhu_exist_data'
    xnr_user_no = xnr_info['xnr_user_no']
    root_uid = xnr_info['root_uid']
    lis = load_data(xnr_user_no, redis_key)

    for item in lis:
        # ['update_time', 'uid', 'nick_name', 'user_name', 'profile_url', 'photo_url']
        uid = item.get('uid', '')
        user_data = {
            'platform': 'twitter',
            'xnr_no': xnr_user_no,
            'xnr_uid': root_uid,

            'uid': uid,
            'nickname': item.get('nick_name', ''),
            'username': item.get('user_name', ''),
            'photo_url': item.get('photo_url', ''),

            'pingtaiguanzhu': 1,
        }

        if not update_twitter_xnr_relations(root_uid, uid, user_data, update_portrait_info=True):
            EXCEPTION += '\n guanzhu_exist Exception: [root_uid: %s] [xnr_user_no: %s] ' % (root_uid, xnr_user_no)


# 关注列表
def fensi_exist(xnr_info, date):
    global EXCEPTION
    ts = datetime2ts(date)
    twitter_feedback_fans_mappings()
    redis_key = 'twitter_feedback_fensi_exist_data'
    xnr_user_no = xnr_info['xnr_user_no']
    root_uid = xnr_info['root_uid']
    lis = load_data(xnr_user_no, redis_key)

    for item in lis:
        # ['update_time', 'uid', 'nick_name', 'user_name', 'profile_url', 'photo_url']
        uid = item.get('uid', '')
        user_data = {
            'platform': 'twitter',
            'xnr_no': xnr_user_no,
            'xnr_uid': root_uid,

            'uid': uid,
            'nickname': item.get('nick_name', ''),
            'username': item.get('user_name', ''),
            'photo_url': item.get('photo_url', ''),

            'pingtaifensi': 1,
        }

        if not update_twitter_xnr_relations(root_uid, uid, user_data, update_portrait_info=True):
            EXCEPTION += '\n guanzhu_exist Exception: [root_uid: %s] [xnr_user_no: %s] ' % (root_uid, xnr_user_no)


def main():
    global EXCEPTION
    xnr_info_list = load_xnr_info()
    date = ts2datetime(time.time())
    print date
    for xnr_info in xnr_info_list:
        '''
        try:
            like(xnr_info, date)
        except Exception, e:
            print e

        try:
            retweet(xnr_info, date)
        except Exception, e:
            print e

        try:
            comment(xnr_info, date)
        except Exception, e:
            print e

        try:
            private(xnr_info, date)
        except Exception, e:
            print e            
            
        '''
        try:
            at(xnr_info, date)
        except Exception, e:
            print e

        try:
            fensi_exist(xnr_info, date)
        except Exception, e:
            print e

        try:
            guanzhu_exist(xnr_info, date)
        except Exception, e:
            print e
    print 'EXCEPTION: ', EXCEPTION


if __name__ == '__main__':
    main()














