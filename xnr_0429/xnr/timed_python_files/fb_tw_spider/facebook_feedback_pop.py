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
from global_utils import facebook_feedback_comment_index_name_pre,facebook_feedback_comment_index_type,\
                        facebook_feedback_retweet_index_name_pre,facebook_feedback_retweet_index_type,\
                        facebook_feedback_private_index_name_pre,facebook_feedback_private_index_type,\
                        facebook_feedback_at_index_name_pre,facebook_feedback_at_index_type,\
                        facebook_feedback_like_index_name_pre,facebook_feedback_like_index_type,\
                        facebook_feedback_friends_index_name_pre,facebook_feedback_friends_index_type,\
                        facebook_feedback_friends_index_name,fb_xnr_flow_text_index_name_pre,\
                        fb_xnr_flow_text_index_type
from global_utils import fb_xnr_index_name, fb_xnr_index_type, \
                        fb_xnr_fans_followers_index_name, fb_xnr_fans_followers_index_type
                        
from time_utils import ts2datetime, datetime2ts
from xnr_relations_utils import update_facebook_xnr_relations
sys.path.append('../')
from facebook_feedback_mappings_timer import facebook_feedback_like_mappings, facebook_feedback_retweet_mappings,\
                                            facebook_feedback_at_mappings, facebook_feedback_comment_mappings,\
                                            facebook_feedback_private_mappings, facebook_feedback_friends_mappings
from fb_xnr_manage_mappings import fb_xnr_fans_followers_mappings


sys.path.append('../../facebook/sensitive')
from get_sensitive import get_sensitive_info, get_sensitive_user
from fb_xnr_flow_text_mappings import fb_xnr_flow_text_mappings
from global_utils import facebook_xnr_relations_index_name, facebook_xnr_relations_index_type


r_spider = redis.Redis(host='47.94.133.29', port=9506)
EXCEPTION = ''
redis_keys4fb_spider_data = [
    'facebook_feedback_comment_data',
    'facebook_feedback_like_data',
    'facebook_feedback_retweet_data',
    'facebook_feedback_private_data',
    'facebook_feedback_friends_data',
    'facebook_feedback_at_data',
    'facebook_feedback_friends_exist_data']


def load_xnr_info():
    res = []
    search_res = es.search(fb_xnr_index_name, fb_xnr_index_type, {'size':999})['hits']['hits']
    for item in search_res:
        source = item['_source']
        fb_mail_account = source.get('fb_mail_account', '')
        fb_phone_account = source.get('fb_phone_account', '')
        account = ''
        if fb_mail_account:
            account = fb_mail_account
        elif fb_phone_account:
            account = fb_phone_account
        if account:
            xnr_user_no = source.get('xnr_user_no', '')

            '''
            旧的好友列表获取方式，弃用  @hanmc 2019-3-25 15:10:05
            
            try:
                friends_list = es.get(index=fb_xnr_fans_followers_index_name, doc_type=fb_xnr_fans_followers_index_type, id=xnr_user_no)['_source']['fans_list']
            except:
                friends_list = []
            '''
            # 新的好友列表获取方式
            friends_list = []
            query_body = {
                'query': {
                    'bool': {
                        'must': [
                            {'term': {'xnr_no': xnr_user_no}},
                        ]
                    }
                },
                'size': 99999
            }
            friends_search_res = es.search(facebook_xnr_relations_index_name, facebook_xnr_relations_index_type, query_body)['hits']['hits']
            for friends_item in friends_search_res:
                friends_list.append(friends_item['_source']['uid'])

            info = {
                'root_uid': source.get('uid', ''),
                'root_nick_name': source.get('nick_name', ''),
                'xnr_user_no': xnr_user_no,
                'account': account,
                'password': source.get('password', ''),
                'friends_list': friends_list
            }
            res.append(info)
    return res

def sensitive_func(ts, text, uid):
    sensitive_info = get_sensitive_info(timestamp=ts, text=text)
    sensitive_user = get_sensitive_user(timestamp=ts, uid=uid)
    return sensitive_info, sensitive_user
        
'''
如果记录已经存在facebook_feedback_comment*中，则update；反之，则index
'''
def savedata2es(date, index_pre, index_type, data):
    config = {
        'facebook_feedback_like_':      ['uid', 'root_uid', 'timestamp', 'text', 'root_text', 'root_mid'],
        'facebook_feedback_comment_':   ['uid', 'root_uid', 'mid', 'timestamp', 'text', 'root_text', 'root_mid', 'comment_type'],
        'facebook_feedback_retweet_':   ['uid', 'root_uid', 'mid', 'timestamp', 'text', 'root_text', 'root_mid'],
        'facebook_feedback_private_':   ['uid', 'root_uid', 'timestamp', 'text', 'root_text', 'private_type'],
        'facebook_feedback_friends':    ['uid', 'root_uid'],
        'facebook_feedback_at_':        ['uid', 'root_uid', 'mid', 'timestamp', 'text'],
    }
    if index_pre in ['facebook_feedback_at_', 'facebook_feedback_comment_', 'facebook_feedback_retweet_', 'facebook_feedback_private_', 'facebook_feedback_like_']:
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
        except Exception,e:
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

# 评论
def comment(xnr_info, date):
    global EXCEPTION
    ts = datetime2ts(date)
    facebook_feedback_comment_mappings(facebook_feedback_comment_index_name_pre + date)
    redis_key = 'facebook_feedback_comment_data'
    xnr_user_no = xnr_info['xnr_user_no']
    lis = load_data(xnr_user_no, redis_key)

    # {'uid', 'nick_name', 'mid', 'timestamp', 'text', 'update_time', 'root_text', 'root_mid', 'comment_type'}
    data = []
    for item in lis:
        try:
            uid = item['uid']
            text = item['text']
            if uid in xnr_info['friends_list']:
                facebook_type = u"好友"
            else:
                facebook_type = u"陌生人"
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
                'facebook_type': facebook_type,
                'sensitive_info': sensitive_info,
                'sensitive_user': sensitive_user
            }
            data.append(d)
        except Exception,e:
            EXCEPTION += '\n comment Exception: ' + str(e)
    savedata2es(date, facebook_feedback_comment_index_name_pre, facebook_feedback_comment_index_type, data)



# 点赞
def like(xnr_info, date):
    global EXCEPTION
    ts = datetime2ts(date)
    facebook_feedback_like_mappings(facebook_feedback_like_index_name_pre + date)
    redis_key = 'facebook_feedback_like_data'
    xnr_user_no = xnr_info['xnr_user_no']
    lis = load_data(xnr_user_no, redis_key)


    # {'uid', 'photo_url', 'nick_name', 'timestamp', 'root_text', 'update_time', 'root_mid'}
    data = []
    for item in lis:
        try:
            uid = item['uid']
            nick_name = item['nick_name']
            text = nick_name + u" 赞同了您的帖子。"
            if uid in xnr_info['friends_list']:
                facebook_type = u"好友"
            else:
                facebook_type = u"陌生人"
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
                'facebook_type': facebook_type,
                'sensitive_info': sensitive_info,
                'sensitive_user': sensitive_user
            }
            data.append(d)
        except Exception,e:
            EXCEPTION += '\n like Exception: ' + str(e)
    savedata2es(date, facebook_feedback_like_index_name_pre, facebook_feedback_like_index_type, data)

# 分享
def retweet(xnr_info, date):
    global EXCEPTION
    ts = datetime2ts(date)
    facebook_feedback_retweet_mappings(facebook_feedback_retweet_index_name_pre + date)
    redis_key = 'facebook_feedback_retweet_data'
    xnr_user_no = xnr_info['xnr_user_no']
    lis = load_data(xnr_user_no, redis_key)

    # {'uid', 'nick_name', 'mid', 'timestamp', 'text', 'update_time', 'root_text', 'root_mid'}
    data = []
    for item in lis:
        try:
            uid = item['uid']
            text = item['text']
            if uid in xnr_info['friends_list']:
                facebook_type = u"好友"
            else:
                facebook_type = u"陌生人"
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
                'facebook_type': facebook_type,
                'sensitive_info': sensitive_info,
                'sensitive_user': sensitive_user,
                'retweet': 0,
                'comment': 0,
                'like': 0
            }
            data.append(d)
        except Exception,e:
            EXCEPTION += '\n retweet Exception: ' + str(e)
    savedata2es(date, facebook_feedback_retweet_index_name_pre, facebook_feedback_retweet_index_type, data)

# 私信
def private(xnr_info, date):
    global EXCEPTION
    ts = datetime2ts(date)
    facebook_feedback_private_mappings(facebook_feedback_private_index_name_pre + date)
    redis_key = 'facebook_feedback_private_data'
    xnr_user_no = xnr_info['xnr_user_no']
    lis = load_data(xnr_user_no, redis_key)

    # {'uid', 'photo_url', 'nick_name', 'timestamp', 'update_time', 'text', 'root_text', 'private_type'}
    data = []
    for item in lis:
        try:
            uid = item['uid']
            text = item['text']
            if uid in xnr_info['friends_list']:
                facebook_type = u"好友"
            else:
                facebook_type = u"陌生人"
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
                'facebook_type': facebook_type,
                'sensitive_info': sensitive_info,
                'sensitive_user': sensitive_user
            }
            data.append(d)
        except Exception,e:
            EXCEPTION += '\n private Exception: ' + str(e)
    savedata2es(date, facebook_feedback_private_index_name_pre, facebook_feedback_private_index_type, data)
    
# 好友请求
def friends(xnr_info, date):
    global EXCEPTION
    ts = datetime2ts(date)
    facebook_feedback_friends_mappings()
    redis_key = 'facebook_feedback_friends_data'
    xnr_user_no = xnr_info['xnr_user_no']
    lis = load_data(xnr_user_no, redis_key)

    # {'uid', 'nick_name', 'profile_url', 'photo_url', 'friends', 'update_time'}
    data = []
    for item in lis:
        try:
            uid = item['uid']
            if uid in xnr_info['friends_list']:
                facebook_type = u"好友"
            else:
                facebook_type = u"陌生人"
            d = {
                'uid': uid,
                'nick_name': item['nick_name'],
                'update_time': item['update_time'],
                'photo_url': item['photo_url'],
                'friends': item['friends'],
                'profile_url': item['profile_url'],
                'root_uid': xnr_info['root_uid'],
                'root_nick_name': xnr_info['root_nick_name'],
                'facebook_type': facebook_type,
            }
            data.append(d)
        except Exception,e:
            EXCEPTION += '\n friends Exception: ' + str(e)
    savedata2es(date, facebook_feedback_friends_index_name, facebook_feedback_friends_index_type, data)

# 点赞
def at(xnr_info, date):
    global EXCEPTION
    ts = datetime2ts(date)
    facebook_feedback_at_mappings(facebook_feedback_at_index_name_pre + date)
    redis_key = 'facebook_feedback_at_data'
    xnr_user_no = xnr_info['xnr_user_no']
    lis = load_data(xnr_user_no, redis_key)

    # {'uid', 'nick_name', 'mid', 'timestamp', 'text', 'update_time', 'photo_url'}
    data = []
    for item in lis:
        try:
            uid = item['uid']
            text = item['text']
            if uid in xnr_info['friends_list']:
                facebook_type = u"好友"
            else:
                facebook_type = u"陌生人"
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
                'root_nick_name': xnr_info['root_nick_name'],
                'facebook_type': facebook_type,
                'sensitive_info': sensitive_info,
                'sensitive_user': sensitive_user
            }
            data.append(d)
        except Exception,e:
            EXCEPTION += '\n at Exception: ' + str(e)
    savedata2es(date, facebook_feedback_at_index_name_pre, facebook_feedback_at_index_type, data)  

# 好友列表
def friends_exist(xnr_info, date):
    global EXCEPTION
    ts = datetime2ts(date)
    fb_xnr_fans_followers_mappings()
    redis_key = 'facebook_feedback_friends_exist_data'
    xnr_user_no = xnr_info['xnr_user_no']
    root_uid = xnr_info['root_uid']
    lis = load_data(xnr_user_no, redis_key)



    '''
    旧的好友列表存储方式，弃用  @hanmc 2019-3-25 15:10:05
    try:
        new_friends_list = [item['uid'] for item in lis]
    except Exception,e:
        EXCEPTION += '\n friends_exist Exception: ' + str(e)
    try:  
        friends_list = es.get(index=fb_xnr_fans_followers_index_name, doc_type=fb_xnr_fans_followers_index_type, id=xnr_user_no)['_source']['fans_list']
    except:
        print es.index(fb_xnr_fans_followers_index_name, fb_xnr_fans_followers_index_type, body={'fans_list': []}, id=xnr_user_no)
        friends_list = []
        
    if not new_friends_list == friends_list:
        print es.update(index=fb_xnr_fans_followers_index_name, doc_type=fb_xnr_fans_followers_index_type, body={'doc': {'fans_list': new_friends_list}}, id=xnr_user_no)
    '''
    # 新的好友列表存储方式
    for item in lis:
        # {'uid', 'nick_name', 'profile_url'}
        uid = item.get('uid', '')
        user_data = {
            'platform': 'facebook',
            'xnr_no': xnr_user_no,
            'xnr_uid': root_uid,

            'uid': uid,
            'nickname': item.get('nick_name', ''),

            'pingtaihaoyou': 1,
        }

        if not update_facebook_xnr_relations(root_uid, uid, user_data, update_portrait_info=True):
            EXCEPTION += '\n friends_exist Exception: [root_uid: %s] [xnr_user_no: %s] ' % (root_uid, xnr_user_no)


def main():
    global EXCEPTION
    xnr_info_list = load_xnr_info()
    date = ts2datetime(time.time())
    for xnr_info in xnr_info_list:

        try:
            comment(xnr_info, date)
        except Exception,e:
            print e
                    
        try:
            like(xnr_info, date)
        except Exception,e:
            print e

        try:
            at(xnr_info, date)
        except Exception,e:
            print e

        try:    
            retweet(xnr_info, date)
        except Exception,e:
            print e
        
        try:
            private(xnr_info, date)
        except Exception,e:
            print e
        
        try:
            friends(xnr_info, date)
        except Exception,e:
            print e

        try:
            friends_exist(xnr_info, date)
        except Exception,e:
            print e
    print 'EXCEPTION: ', EXCEPTION
        
if __name__ == '__main__':
    main()

    












