#-*-coding:utf-8-*-
import os
import json
import time
import random
import redis
from elasticsearch import Elasticsearch
import sys
sys.path.append('../../')
from global_utils import es_xnr_2 as es
from global_utils import \
        fb_xnr_index_name, fb_xnr_index_type, \
        fb_xnr_fans_followers_index_name, fb_xnr_fans_followers_index_type         
from global_utils import \
        facebook_feedback_comment_index_name_pre,facebook_feedback_comment_index_type,\
        facebook_feedback_retweet_index_name_pre,facebook_feedback_retweet_index_type,\
        facebook_feedback_private_index_name_pre,facebook_feedback_private_index_type,\
        facebook_feedback_at_index_name_pre,facebook_feedback_at_index_type,\
        facebook_feedback_like_index_name_pre,facebook_feedback_like_index_type,\
        facebook_feedback_friends_index_name_pre,facebook_feedback_friends_index_type,\
        facebook_feedback_friends_index_name,fb_xnr_flow_text_index_name_pre,\
        fb_xnr_flow_text_index_type             
from time_utils import ts2datetime, datetime2ts

sys.path.append('../')
from facebook_feedback_mappings_timer import \
        facebook_feedback_like_mappings, facebook_feedback_retweet_mappings,\
        facebook_feedback_at_mappings, facebook_feedback_comment_mappings,\
        facebook_feedback_private_mappings, facebook_feedback_friends_mappings
from fb_xnr_manage_mappings import fb_xnr_fans_followers_mappings


r_spider = redis.Redis(host='47.94.133.29', port=9506)


def push2spider_redis(key, value):
    try:
        if r_spider.rpush(key, value):
            return True
    except Exception,e:
        print e
    return False


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
            try:
                friends_list = es.get(index=fb_xnr_fans_followers_index_name, doc_type=fb_xnr_fans_followers_index_type, id=xnr_user_no)['_source']['fans_list']
            except:
                friends_list = []
                
            info = {
                'root_uid': source.get('uid', ''),
                'root_nick_name': source.get('nick_name', ''),
                'xnr_user_no': xnr_user_no,
                'account': account,
                'password': source.get('password', ''),
                'friends_list': friends_list,
                'retry_times': 0,
                'remark': '',
            }
            res.append(info)
    return res
        

# 评论
def send_task4comment(xnr_info, date):
    facebook_feedback_comment_mappings(facebook_feedback_comment_index_name_pre + date)
    return push2spider_redis('facebook_feedback_comment', xnr_info)


# 点赞
def send_task4like(xnr_info, date):
    facebook_feedback_like_mappings(facebook_feedback_like_index_name_pre + date)
    return push2spider_redis('facebook_feedback_like', xnr_info)

# 分享
def send_task4retweet(xnr_info, date):
    facebook_feedback_retweet_mappings(facebook_feedback_retweet_index_name_pre + date)
    return push2spider_redis('facebook_feedback_retweet', xnr_info)

# 私信
def send_task4private(xnr_info, date):
    facebook_feedback_private_mappings(facebook_feedback_private_index_name_pre + date)
    return push2spider_redis('facebook_feedback_private', xnr_info)
 
# 好友请求
def send_task4friends(xnr_info, date):
    facebook_feedback_friends_mappings()
    return push2spider_redis('facebook_feedback_friends', xnr_info)

# @
def send_task4at(xnr_info, date):
    facebook_feedback_at_mappings(facebook_feedback_at_index_name_pre + date)
    return push2spider_redis('facebook_feedback_at', xnr_info)

# 好友列表
def send_task4friends_exist(xnr_info, date):
    fb_xnr_fans_followers_mappings()
    return push2spider_redis('facebook_feedback_friends_exist', xnr_info)

def main():
    xnr_info_list = load_xnr_info()
    date = ts2datetime(time.time())
    for xnr_info in xnr_info_list:
        send_task4comment(xnr_info, date)
        send_task4like(xnr_info, date)
        send_task4retweet(xnr_info, date)
        send_task4private(xnr_info, date)
        send_task4friends(xnr_info, date)
        send_task4at(xnr_info, date)
        send_task4friends_exist(xnr_info, date)

if __name__ == '__main__':
    main()

    









