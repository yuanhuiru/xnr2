#-*- coding:utf-8 -*-
import os
import time
import json
import sys
import random
import base64
import re

#reload(sys)
sys.path.append('../')
from global_config import S_DATE,S_TYPE,S_DATE_BCI,SYSTEM_START_DATE
from global_utils import es_xnr as es
from global_utils import weibo_hot_keyword_task_index_name,weibo_hot_keyword_task_index_type,\
                            weibo_xnr_timing_list_index_name,weibo_xnr_timing_list_index_type,\
                            weibo_xnr_index_name,weibo_xnr_index_type,es_flow_text,flow_text_index_name_pre,\
                            flow_text_index_type,es_user_profile,profile_index_name,profile_index_type,\
                            social_sensing_index_name,social_sensing_index_type,\
                            weibo_hot_content_recommend_results_index_name,\
                            weibo_hot_content_recommend_results_index_type,\
                            weibo_hot_subopinion_results_index_name,weibo_hot_subopinion_results_index_type,\
                            weibo_bci_index_name_pre,weibo_bci_index_type,portrait_index_name,portrait_index_type,\
                            es_user_portrait,es_user_profile,active_social_index_name_pre,active_social_index_type,\
                            weibo_business_tweets_index_name_pre,weibo_business_tweets_index_type
from global_utils import weibo_feedback_comment_index_name,weibo_feedback_comment_index_type,\
                            weibo_feedback_retweet_index_name,weibo_feedback_retweet_index_type,\
                            weibo_feedback_private_index_name,weibo_feedback_private_index_type,\
                            weibo_feedback_at_index_name,weibo_feedback_at_index_type,\
                            weibo_feedback_like_index_name,weibo_feedback_like_index_type,\
                            weibo_feedback_fans_index_name,weibo_feedback_fans_index_type,\
                            weibo_feedback_follow_index_name,weibo_feedback_follow_index_type,\
                            weibo_feedback_group_index_name,weibo_feedback_group_index_type,\
                            weibo_xnr_fans_followers_index_name,weibo_xnr_fans_followers_index_type,\
                            index_sensing,type_sensing,weibo_xnr_retweet_timing_list_index_name,\
                            weibo_domain_index_name,weibo_domain_index_type,weibo_xnr_retweet_timing_list_index_type,weibo_private_white_uid_index_name,\
                            weibo_private_white_uid_index_type,daily_interest_index_name_pre,\
                            daily_interest_index_type, be_retweet_index_name_pre, be_retweet_index_type, es_retweet

from global_utils import weibo_xnr_save_like_index_name,weibo_xnr_save_like_index_type

from time_utils import ts2datetime,datetime2ts,get_flow_text_index_list,\
                            get_timeset_indexset_list, get_db_num
from weibo_publish_func import publish_tweet_func,retweet_tweet_func,comment_tweet_func,private_tweet_func,\
                                like_tweet_func,follow_tweet_func,unfollow_tweet_func,\
                                reply_tweet_func #,at_tweet_func create_group_func,
from parameter import DAILY_INTEREST_TOP_USER,DAILY_AT_RECOMMEND_USER_TOP,TOP_WEIBOS_LIMIT,\
                        HOT_AT_RECOMMEND_USER_TOP,HOT_EVENT_TOP_USER,BCI_USER_NUMBER,USER_POETRAIT_NUMBER,\
                        MAX_SEARCH_SIZE,domain_ch2en_dict,topic_en2ch_dict,topic_ch2en_dict,FRIEND_LIST,\
                        FOLLOWERS_LIST,IMAGE_PATH,WHITE_UID_PATH,WHITE_UID_FILE_NAME,TOP_WEIBOS_LIMIT_DAILY,\
                        daily_ch2en,TOP_ACTIVE_SOCIAL,task_source_ch2en,MAX_VALUE
from utils import uid2nick_name_photo,xnr_user_no2uid,judge_follow_type,judge_sensing_sensor,\
                        get_influence_relative



def get_tweets_from_flow(monitor_keywords_list,sort_item_new):

    nest_query_list = []
    for monitor_keyword in monitor_keywords_list:
        nest_query_list.append({'wildcard':{'keywords_string':'*'+monitor_keyword+'*'}})

    query_body = {
        'query':{
            'bool':{
                'should':nest_query_list
            }  
        },
        'sort':[{sort_item_new:{'order':'desc'}},{'timestamp':{'order':'desc'}}],
        'size':TOP_WEIBOS_LIMIT
    }

    if S_TYPE == 'test':
        now_ts = datetime2ts(S_DATE)    
    else:
        now_ts = int(time.time())
    datetime = ts2datetime(now_ts-24*3600)

    index_name = flow_text_index_name_pre + datetime

    es_results = es_flow_text.search(index=index_name,doc_type=flow_text_index_type,body=query_body)['hits']['hits']

    if not es_results:
        es_results = es_flow_text.search(index=index_name,doc_type=flow_text_index_type,\
                                body={'query':{'match_all':{}},'size':TOP_WEIBOS_LIMIT,\
                                'sort':{sort_item_new:{'order':'desc'}}})['hits']['hits']
    results_all = []
    for result in es_results:
        result = result['_source']
        uid = result['uid']
        nick_name,photo_url = uid2nick_name_photo(uid)
        result['nick_name'] = nick_name
        result['photo_url'] = photo_url
        results_all.append(result)
    return results_all

def uid_lists2weibo_from_flow_text(monitor_keywords_list,uid_list):

    nest_query_list = []
    for monitor_keyword in monitor_keywords_list:
        nest_query_list.append({'wildcard':{'keywords_string':'*'+monitor_keyword+'*'}})

    query_body = {
        'query':{
            'bool':{
                'should':nest_query_list,
                'must':[
                    {'terms':{'uid':uid_list}}
                ]
            }  
            
        },
        'size':TOP_WEIBOS_LIMIT,
        'sort':{'timestamp':{'order':'desc'}}
    }

    if S_TYPE == 'test':
        now_ts = datetime2ts(S_DATE)    
    else:
        now_ts = int(time.time())
    datetime = ts2datetime(now_ts-24*3600)

    index_name_flow = flow_text_index_name_pre + datetime

    es_results = es_flow_text.search(index=index_name_flow,doc_type=flow_text_index_type,body=query_body)['hits']['hits']

    results_all = []
    for result in es_results:
        result = result['_source']
        uid = result['uid']
        #nick_name,photo_url = uid2nick_name_photo(uid)
        result['nick_name'] = uid#nick_name
        result['photo_url'] = ''#photo_url
        results_all.append(result)
    return results_all

def get_tweets_from_bci(monitor_keywords_list,sort_item_new):

    if S_TYPE == 'test':
        now_ts = datetime2ts(S_DATE_BCI)    
    else:
        now_ts = int(time.time())

    datetime = ts2datetime(now_ts-24*3600)
    datetime_new = datetime[0:4]+datetime[5:7]+datetime[8:10]

    index_name = weibo_bci_index_name_pre + datetime_new

    query_body = {
        'query':{
            'match_all':{}
        },
        'sort':{sort_item_new:{'order':'desc'}},
        'size':BCI_USER_NUMBER
    }

    es_results_bci = es_user_portrait.search(index=index_name,doc_type=weibo_bci_index_type,body=query_body)['hits']['hits']
    #print 'es_results_bci::',es_results_bci
    #print 'index_name::',index_name
    #print ''
    uid_set = set()

    if es_results_bci:
        for result in es_results_bci:
            uid = result['_id']
            uid_set.add(uid)
    uid_list = list(uid_set)

    es_results = uid_lists2weibo_from_flow_text(monitor_keywords_list,uid_list)

    return es_results

def get_tweets_from_user_portrait(monitor_keywords_list,sort_item_new):

    query_body = {
        'query':{
            'match_all':{}
        },
        'sort':{sort_item_new:{'order':'desc'}},
        'size':USER_POETRAIT_NUMBER
    }
    #print 'query_body:::',query_body
    es_results_portrait = es_user_portrait.search(index=portrait_index_name,doc_type=portrait_index_type,body=query_body)['hits']['hits']

    uid_set = set()

    if es_results_portrait:
        for result in es_results_portrait:
            result = result['_source']
            uid = result['uid']
            uid_set.add(uid)
    uid_list = list(uid_set)

    es_results = uid_lists2weibo_from_flow_text(monitor_keywords_list,uid_list)
    
    return es_results

def get_bussiness_recomment_tweets(xnr_user_no,sort_item):
    
    get_results = es.get(index=weibo_xnr_index_name,doc_type=weibo_xnr_index_type,id=xnr_user_no)['_source']
    
    monitor_keywords = get_results['monitor_keywords']
    monitor_keywords_list = monitor_keywords.split(',')
    
    if sort_item == 'timestamp':
        sort_item_new = 'timestamp'
        es_results = get_tweets_from_flow(monitor_keywords_list,sort_item_new)
    elif sort_item == 'sensitive_info':
        sort_item_new = 'sensitive'
        es_results = get_tweets_from_flow(monitor_keywords_list,sort_item_new)
    elif sort_item == 'sensitive_user':
        sort_item_new = 'sensitive'
        es_results = get_tweets_from_user_portrait(monitor_keywords_list,sort_item_new)  
    elif sort_item == 'influence_info':
        sort_item_new = 'retweeted'
        es_results = get_tweets_from_flow(monitor_keywords_list,sort_item_new)
    elif sort_item == 'influence_user':
        sort_item_new = 'user_index'
        es_results = get_tweets_from_bci(monitor_keywords_list,sort_item_new)
        
    return es_results

def get_all_xnrs():

    all_xnr_list = set()

    query_body = {
        'query':{
            'term':{'create_status':2}
        },
        'size':MAX_VALUE
    }


    search_results_new = es.search(index=weibo_xnr_index_name,doc_type=weibo_xnr_index_type,\
                            body=query_body)['hits']['hits']

    print 'len--search..',len(search_results_new)
    #if search_results_new:
    for result_item in search_results_new:
        result = result_item['_source']
        print result['xnr_user_no']
        all_xnr_list.add(result['xnr_user_no'])

    return list(all_xnr_list)

def save_results_to_es(xnr_user_no,current_date,sort_item,result):

    item_body = {}
    item_body['xnr_user_no'] = xnr_user_no
    item_body['sort_item'] = sort_item
    item_body['result'] = json.dumps(result)
    item_body['timestamp'] = datetime2ts(current_date)

    _id = xnr_user_no +'_'+ sort_item

    index_name = weibo_business_tweets_index_name_pre + current_date

    es.index(index=index_name,doc_type=weibo_business_tweets_index_type,body=item_body,id=_id)

def weibo_business_tweets_recommend_daily(current_date):

    # 1. 获得所有已完成虚拟人

    all_xnrs = get_all_xnrs()
    print 'all_xnrs',all_xnrs
    # 2. 对于每个虚拟人，计算 按 时间戳，按信息敏感度，按人物敏感度，按信息影响力、按人物影响力五个结果 并保存
    for xnr_user_no in all_xnrs:
        for sort_item in ['timestamp','sensitive_info','sensitive_user','influence_info','influence_user']:
            task_detail = {}
                        
            # 计算
            result = get_bussiness_recomment_tweets(xnr_user_no,sort_item)
            #print 'result',len(result)
            # 保存
            save_results_to_es(xnr_user_no,current_date,sort_item,result)


if __name__ == '__main__':

    current_time = time.time()
    current_date = ts2datetime(current_time)
    start_ts = time.time()
    weibo_business_tweets_recommend_daily(current_date)
    end_ts = time.time()
    print 'cost..',end_ts - start_ts

