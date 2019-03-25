# -*-coding:utf-8-*-
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
    tw_xnr_index_name, tw_xnr_index_type, \
    tw_xnr_fans_followers_index_name, tw_xnr_fans_followers_index_type
from global_utils import \
    twitter_feedback_comment_index_name_pre, twitter_feedback_comment_index_type, \
    twitter_feedback_retweet_index_name_pre, twitter_feedback_retweet_index_type, \
    twitter_feedback_private_index_name_pre, twitter_feedback_private_index_type, \
    twitter_feedback_at_index_name_pre, twitter_feedback_at_index_type, \
    twitter_feedback_like_index_name_pre, twitter_feedback_like_index_type, \
    tw_xnr_flow_text_index_name_pre, tw_xnr_flow_text_index_type,\
    twitter_feedback_guanzhuhuifen_index_name, twitter_feedback_guanzhuhuifen_index_type
from time_utils import ts2datetime, datetime2ts

sys.path.append('../')
from twitter_feedback_mappings_timer import \
    twitter_feedback_like_mappings, twitter_feedback_retweet_mappings, \
    twitter_feedback_at_mappings, twitter_feedback_comment_mappings, \
    twitter_feedback_private_mappings, twitter_feedback_guanzhuhuifen_mappings

from tw_xnr_manage_mappings import tw_xnr_fans_followers_mappings

r_spider = redis.Redis(host='47.94.133.29', port=9506)


def push2spider_redis(key, value):
    try:
        if r_spider.rpush(key, value):
            return True
    except Exception, e:
        print e
    return False


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

            info = {
                'root_uid': source.get('uid', ''),
                'root_nick_name': source.get('nick_name', ''),
                'xnr_user_no': xnr_user_no,
                'account': account,
                'password': source.get('password', ''),
                'retry_times': 0,
                'remark': '',
            }
            res.append(info)
    return res


# 评论
def send_task4comment(xnr_info, date):
    twitter_feedback_comment_mappings(twitter_feedback_comment_index_name_pre + date)
    return push2spider_redis('twitter_feedback_comment', xnr_info)


# 点赞
def send_task4like(xnr_info, date):
    twitter_feedback_like_mappings(twitter_feedback_like_index_name_pre + date)
    return push2spider_redis('twitter_feedback_like', xnr_info)


# 分享
def send_task4retweet(xnr_info, date):
    twitter_feedback_retweet_mappings(twitter_feedback_retweet_index_name_pre + date)
    return push2spider_redis('twitter_feedback_retweet', xnr_info)


# 私信
def send_task4private(xnr_info, date):
    twitter_feedback_private_mappings(twitter_feedback_private_index_name_pre + date)
    return push2spider_redis('twitter_feedback_private', xnr_info)


# @
def send_task4at(xnr_info, date):
    twitter_feedback_at_mappings(twitter_feedback_at_index_name_pre + date)
    return push2spider_redis('twitter_feedback_at', xnr_info)


# 关注回粉
def send_task4guanzhuhuifen(xnr_info, date):
    twitter_feedback_guanzhuhuifen_mappings()
    return push2spider_redis('twitter_feedback_guanzhuhuifen', xnr_info)


# 关注列表
def send_task4guanzhu_exist(xnr_info, date):
    return push2spider_redis('twitter_feedback_guanzhu_exist', xnr_info)


# 关注列表
def send_task4fensi_exist(xnr_info, date):
    return push2spider_redis('twitter_feedback_fensi_exist', xnr_info)


def main():
    xnr_info_list = load_xnr_info()
    date = ts2datetime(time.time())
    for xnr_info in xnr_info_list:
        send_task4comment(xnr_info, date)
        send_task4like(xnr_info, date)
        send_task4retweet(xnr_info, date)
        send_task4private(xnr_info, date)
        send_task4at(xnr_info, date)
        send_task4guanzhu_exist(xnr_info, date)
        send_task4fensi_exist(xnr_info, date)
        send_task4guanzhuhuifen(xnr_info, date)


if __name__ == '__main__':
    main()













