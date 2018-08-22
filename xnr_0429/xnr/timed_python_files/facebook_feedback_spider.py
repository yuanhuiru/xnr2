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
from fb_xnr_manage_mappings import fb_xnr_fans_followers_mappings
from send_mail import send_mail

sys.path.append('../facebook/sensitive')
from get_sensitive import get_sensitive_info, get_sensitive_user
from fb_xnr_flow_text_mappings import fb_xnr_flow_text_mappings

sys.path.append('../facebook')
from feedback_comment import Comment
from feedback_like import Like
from feedback_share import Share
from feedback_message import Message
from feedback_friends import Friend
from feedback_mention import Mention
from feedback_friends_exist import FriendExist

EXCEPTION = ''

def sendfile2mail(filepath='', text=''):
    content = {
        'subject': 'Facebook_feedback_exception',
        'text': text,
        'files_path': filepath,    #支持多个，以逗号隔开
        }
    from_user = {
        'name': '虚拟人项目',
        'addr': '929673096@qq.com',
        'password': 'czlasoaiehchbega',
        'smtp_server': 'smtp.qq.com'   
    }
    to_user = {
        'name': '管理员',
        'addr': '929673096@qq.com'  #支持多个，以逗号隔开
    }
    send_mail(from_user=from_user, to_user=to_user, content=content)
    
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
                'friends_list': friends_list
            }
            res.append(info)
    return res

def sensitive_func(ts, text, uid):
    sensitive_info = get_sensitive_info(ts, text)
    sensitive_user = get_sensitive_user(ts, uid)
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
            EXCEPTION += '\n savedata2es Exception: ' + str(e)

# 评论
def comment(xnr_info, date):
    ts = datetime2ts(date)
    facebook_feedback_comment_mappings(facebook_feedback_comment_index_name_pre + date)
    comment = Comment(xnr_info['account'], xnr_info['password'])
    lis = comment.get_comment()
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
    ts = datetime2ts(date)
    facebook_feedback_like_mappings(facebook_feedback_like_index_name_pre + date)
    like = Like(xnr_info['account'], xnr_info['password'])
    lis = like.get_like()
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
    ts = datetime2ts(date)
    facebook_feedback_retweet_mappings(facebook_feedback_retweet_index_name_pre + date)
    share = Share(xnr_info['account'], xnr_info['password'])
    lis = share.get_share()
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
    ts = datetime2ts(date)
    facebook_feedback_private_mappings(facebook_feedback_private_index_name_pre + date)
    message = Message(xnr_info['account'], xnr_info['password'])
    lis = message.get_message()
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
    ts = datetime2ts(date)
    facebook_feedback_friends_mappings()
    friend = Friend(xnr_info['account'], xnr_info['password'])
    lis = friend.get_friend()
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
    ts = datetime2ts(date)
    facebook_feedback_at_mappings(facebook_feedback_at_index_name_pre + date)
    mention = Mention(xnr_info['account'], xnr_info['password'])
    lis = mention.get_mention()
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
    ts = datetime2ts(date)
    fb_xnr_fans_followers_mappings()
    friend = FriendExist(xnr_info['account'], xnr_info['password'])
    lis = friend.get_friend_exist()
    # {'uid', 'nick_name', 'photo_url'}
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
       
def main():
    xnr_info_list = load_xnr_info()
    date = ts2datetime(time.time())
    for xnr_info in xnr_info_list:
        try:
            comment(xnr_info, date)
        except Exception,e:
            pass
            
        try:
            like(xnr_info, date)
        except Exception,e:
            pass
        
        try:    
            retweet(xnr_info, date)
        except Exception,e:
            pass
        
        try:
            private(xnr_info, date)
        except Exception,e:
            pass
        
        try:
            friends(xnr_info, date)
        except Exception,e:
            pass

        try:
            friends_exist(xnr_info, date)
        except Exception,e:
            pass
        
    if EXCEPTION:
        try:
            sendfile2mail(text=EXCEPTION)
        except:
            pass
        
if __name__ == '__main__':
    main()

    








