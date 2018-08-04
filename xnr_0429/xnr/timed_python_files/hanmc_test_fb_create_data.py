#-*-coding:utf-8-*-
import os
import json
import time
import random
from elasticsearch import Elasticsearch
import sys
sys.path.append('../')
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
from facebook_feedback_mappings_timer import facebook_feedback_like_mappings, facebook_feedback_retweet_mappings,\
                                            facebook_feedback_at_mappings, facebook_feedback_comment_mappings,\
                                            facebook_feedback_private_mappings, facebook_feedback_friends_mappings
sys.path.append('../facebook/sensitive')
from get_sensitive import get_sensitive_info, get_sensitive_user
from fb_xnr_flow_text_mappings import fb_xnr_flow_text_mappings

sys.path.append('../facebook')
from feedback_comment import Comment


def load_xnr_info():
    res = []
    search_res = es.search(fb_xnr_index_name, fb_xnr_index_type, {})['hits']['hits']
    for item in search_res:
        source = item['_source']
        if source.get('fb_mail_account', ''):
            account = source.get('fb_mail_account')
        else:
            account = source.get('fb_phone_account')
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
            'friends_list': friends_list
        }
        res.append(info)
    return res

# 评论
def comment(xnr_info, date):
    facebook_feedback_comment_index_name = facebook_feedback_comment_index_name_pre + date
    facebook_feedback_comment_mappings(facebook_feedback_comment_index_name)
    comment = Comment(xnr_info['account'], xnr_info['password'])
    lis = comment.get_comment()
    # {'uid', 'nick_name', 'mid', 'timestamp', 'text', 'update_time', 'root_text', 'root_mid'}
    for item in lis:
        uid = item['uid']
        if uid in xnr_info['friends_list']:
            facebook_type = u"好友"
        else:
            facebook_type = u"陌生人"
        data = {
            'uid': uid,
            'nick_name': item['nick_name'],
            'mid': item['mid'],
            'timestamp': item['timestamp'],
            'text': item['text'],
            'update_time': item['update_time'],
            'root_text': item['root_text'],
            'root_mid': item['mid'],
            'comment_type': 'receive',
            'photo_url': '',
            'root_uid': xnr_info['root_uid'],
            'root_nick_name': xnr_info['root_nick_name'],
            'facebook_type': facebook_type,
        }
        print es.index(index=facebook_feedback_comment_index_name, doc_type=facebook_feedback_comment_index_type, body=data)

        
def main():
    xnr_info_list = load_xnr_info()
    date = ts2datetime(time.time())
    for xnr_info in xnr_info_list:
        comment(xnr_info, date)

main()



'''
# 点赞
def like(date):
    facebook_feedback_like_index_name = facebook_feedback_like_index_name_pre + date
    nick_name = random_nick_name()
    data = {
        'uid': random_uid(),
        'photo_url': random_photo_url(),
        'nick_name': nick_name,
        'timestamp': load_timestamp(date),
        'text': nick_name + '赞同了您的帖子',
        'update_time': random_update_time(date),
        'root_text': random_text(),
        'root_mid': random_mid(),
        'root_uid': root_uid,
        'root_nick_name': root_nick_name,
        'facebook_type': random_facebook_type(),
    }
    facebook_feedback_like_mappings(facebook_feedback_like_index_name)
#     print es.index(index=facebook_feedback_like_index_name, doc_type=facebook_feedback_like_index_type, body=data)

# 分享
def retweet(date):
    facebook_feedback_retweet_index_name = facebook_feedback_retweet_index_name_pre + date
    data = {
        'uid': random_uid(),
        'photo_url': random_photo_url(),
        'nick_name': random_nick_name(),
        'mid': random_mid(),
        'timestamp': load_timestamp(date),
        'text': random_text(),
        'update_time': random_update_time(date),
        'root_text': random_text(),
        'root_mid': random_mid(),
        'root_uid': root_uid,
        'root_nick_name': root_nick_name,
        'facebook_type': random_facebook_type(),
        'retweet': random_retweet_num(),
        'comment': random_comment_num(),
        'like': random_like_num(),
    }
    facebook_feedback_retweet_mappings(facebook_feedback_retweet_index_name)
#     print es.index(index=facebook_feedback_retweet_index_name, doc_type=facebook_feedback_retweet_index_type, body=data)

# 标记
def at(date):
    facebook_feedback_at_index_name = facebook_feedback_at_index_name_pre + date
    nick_name = random_nick_name()
    data = {
        'uid': random_uid(),
        'photo_url': random_photo_url(),
        'nick_name': nick_name, 
        'mid': random_mid(),
        'timestamp': load_timestamp(date),
        'text': nick_name + '提到了你',
        'update_time': random_update_time(date),
        'root_uid': root_uid,
        'root_nick_name': root_nick_name,
        'facebook_type': random_facebook_type(),
    }
    facebook_feedback_at_mappings(facebook_feedback_at_index_name)
#     print es.index(index=facebook_feedback_at_index_name, doc_type=facebook_feedback_at_index_type, body=data)


# 私信
def private(date):
    facebook_feedback_private_index_name = facebook_feedback_private_index_name_pre + date
    data = {
        'uid': random_uid(),
        'photo_url': random_photo_url(),
        'nick_name': random_nick_name(),
        'timestamp': load_timestamp(date),
        'text': random_text(),
        'update_time': random_update_time(date),
        'root_text': random_text(),
        'root_uid': root_uid,
        'root_nick_name': root_nick_name,
        'facebook_type': random_facebook_type(),
        'private_type': random_private_type(),
    }
    facebook_feedback_private_mappings(facebook_feedback_private_index_name)
#     print es.index(index=facebook_feedback_private_index_name, doc_type=facebook_feedback_private_index_type, body=data)

# 好友列表
def friends(date):
    uid = random_uid()
    data = {
        'uid': uid,
        'photo_url': random_photo_url(),
        'nick_name': random_nick_name(),
        'friends': random_friends_num(),
        'profile_url': 'https://www.facebook.com/profile.php?id=' + uid,
        'update_time': random_update_time(date),
        'root_uid': root_uid,
        'root_nick_name': root_nick_name,
        'facebook_type': '好友',
    }
    facebook_feedback_friends_mappings()
#     print es.index(index=facebook_feedback_friends_index_name, doc_type=facebook_feedback_friends_index_type, body=data)


def sensitive_func(index_name, ts):
    bulk_action = []
    query_body = {
        'query':{
            'match_all':{}
        },
        'size': 999,
    }
    res = es.search(index=index_name, doc_type='text', body=query_body)['hits']['hits']
    for r in res:
        _id = r['_id']
        uid = r['_source']['uid']
        mid = ''
        if r['_source'].has_key('mid'):
            mid = r['_source']['mid']
        text = ''
        if r['_source'].has_key('text'):
            text = r['_source']['text']
        sensitive_info = get_sensitive_info(ts, mid, text)
        sensitive_user = get_sensitive_user(ts, uid)
        item = {
            'sensitive_info': sensitive_info,
            'sensitive_user': sensitive_user,
        }

        action = {'update':{'_id':_id}}
        bulk_action.extend([action, {'doc': item}])
    if bulk_action:
        print es.bulk(bulk_action,index=index_name,doc_type='text',timeout=600)

def daily_post():
    data = {
        'task_source': 'daily_post',
        'message_type': 1,
    }
    return data

def business_post():
    data = {
        'task_source': 'business_post',
        'message_type': 2,
    }
    return data

def hot_post():
    data = {
        'task_source': 'hot_post',
        'message_type': 2,
    }
    return data

def trace_follow_tweet():
    data = {
        'task_source': 'trace_follow_tweet',
        'message_type': 3,
    }
    return data
    
def xnr_flow_text(date):
    if date < '2017-10-18':
        user_friendsnum = 3
    else:
        user_friendsnum = 5
    index_name = fb_xnr_flow_text_index_name_pre + date
    fb_xnr_flow_text_mappings(index_name)
    for post in [daily_post, business_post, hot_post, trace_follow_tweet]:
        for i in range(random.randint(2,5)):
            data = post()
            _id = xnr_user_no + '_' + str(load_timestamp(date))
            data['uid'] = root_uid
            data['xnr_user_no'] = xnr_user_no
            data['fid'] = ''
            data['text'] = random_text()
            data['user_friendsnum'] = user_friendsnum
            print es.index(index=index_name, doc_type=fb_xnr_flow_text_index_type, body=data, id=_id)



if __name__ == '__main__':
    #create
    #2017-10-15     2017-10-30
    for i in range(15, 31, 1):
        date = '2018-07-' + str(i)
        print 'date', date
        for i in range(random.randint(2,5)):
            like(date)
            retweet(date)
            at(date)
            comment(date)
            private(date)
            friends(date)
    


#update
#2017-10-15     2017-10-30
bulk_action = []
for i in range(15, 31, 1):
    date = '2017-10-' + str(i)
    ts = datetime2ts(date)
    for index_name_pre in ['facebook_feedback_at_', 'facebook_feedback_comment_', 'facebook_feedback_retweet_', 'facebook_feedback_private_', 'facebook_feedback_like_']:
        index_name = index_name_pre + date
        sensitive_func(index_name, ts)
    sensitive_func('facebook_feedback_friends', ts)
'''

'''
#xnr_flow_text_
#2017-10-15     2017-10-30
for i in range(15, 31, 1):
    date = '2017-10-' + str(i)
    xnr_flow_text(date)
'''
    



