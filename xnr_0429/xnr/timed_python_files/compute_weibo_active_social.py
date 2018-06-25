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
                            weibo_bci_index_name_pre,weibo_bci_index_type,portrait_index_name,portrait_index_type,\
                            es_user_portrait,es_user_profile,es_retweet,be_retweet_index_name_pre,be_retweet_index_type

from global_utils import active_social_index_name_pre, active_social_index_type

from time_utils import ts2datetime,datetime2ts,get_flow_text_index_list,\
                            get_timeset_indexset_list, get_db_num
from weibo_publish_func import publish_tweet_func,retweet_tweet_func,comment_tweet_func,private_tweet_func,\
                                like_tweet_func,follow_tweet_func,unfollow_tweet_func,create_group_func,\
                                reply_tweet_func #,at_tweet_func
from parameter import DAILY_INTEREST_TOP_USER,DAILY_AT_RECOMMEND_USER_TOP,TOP_WEIBOS_LIMIT,\
                        HOT_AT_RECOMMEND_USER_TOP,HOT_EVENT_TOP_USER,BCI_USER_NUMBER,USER_POETRAIT_NUMBER,\
                        MAX_SEARCH_SIZE,domain_ch2en_dict,topic_en2ch_dict,topic_ch2en_dict,FRIEND_LIST,\
                        FOLLOWERS_LIST,IMAGE_PATH,WHITE_UID_PATH,WHITE_UID_FILE_NAME,TOP_WEIBOS_LIMIT_DAILY,\
                        daily_ch2en,TOP_ACTIVE_SOCIAL,task_source_ch2en,MAX_VALUE
from utils import uid2nick_name_photo,xnr_user_no2uid,judge_follow_type,judge_sensing_sensor,\
                        get_influence_relative



def get_friends_list(recommend_set_list):

    friend_list = []
    if len(recommend_set_list) == 0:
		return friend_list
    now_ts = time.time()
    now_date_ts = datetime2ts(ts2datetime(now_ts))
    #get redis db number
    db_number = get_db_num(now_date_ts)
    #print 'db_number...',db_number
    search_result = es_retweet.mget(index=be_retweet_index_name_pre+str(db_number), doc_type=be_retweet_index_type, body={"ids": recommend_set_list})["docs"]
    for item in search_result:
        uid = item['_id']
        if not item['found']:
            continue
        else:
            data = item['_source']['uid_be_retweet']
            data = eval(data)
            friend_list.extend(data.keys())

    return friend_list[:500]

## 主动社交- 相关推荐
def get_related_recommendation(task_detail):
    
    avg_sort_uid_dict = {}

    xnr_user_no = task_detail['xnr_user_no']
    sort_item = task_detail['sort_item']
    es_result = es.get(index=weibo_xnr_index_name,doc_type=weibo_xnr_index_type,id=xnr_user_no)['_source']
    uid = es_result['uid']

    monitor_keywords = es_result['monitor_keywords']
    
    monitor_keywords_list = monitor_keywords.split(',')

    nest_query_list = []
    #print 'monitor_keywords_list::',monitor_keywords_list
    for monitor_keyword in monitor_keywords_list:
        #print 'monitor_keyword::::',monitor_keyword
        nest_query_list.append({'wildcard':{'keywords_string':'*'+monitor_keyword+'*'}})
    
    # else:
    try:
        recommend_list = es.get(index=weibo_xnr_fans_followers_index_name,doc_type=weibo_xnr_fans_followers_index_type,id=xnr_user_no)['_source']['followers_list']
    except:
        recommend_list = []

    recommend_set_list = list(set(recommend_list))

    if S_TYPE == 'test':
        current_date = S_DATE
    else:
        current_date = ts2datetime(int(time.time()-24*3600))
    
    flow_text_index_name = flow_text_index_name_pre + current_date

    if sort_item != 'friend':

        uid_list = []
        #uid_list = recommend_set_list
        if sort_item == 'influence':
            sort_item = 'user_fansnum'
        query_body_rec = {
            'query':{
                
                'bool':{
                    'should':nest_query_list
                }
            },
            'aggs':{
                'uid_list':{
                    'terms':{'field':'uid','size':TOP_ACTIVE_SOCIAL,'order':{'avg_sort':'desc'} },
                    'aggs':{'avg_sort':{'avg':{'field':sort_item}}}

                }
            }
        }

        es_rec_result = es_flow_text.search(index=flow_text_index_name,doc_type='text',body=query_body_rec)['aggregations']['uid_list']['buckets']
        #print 'es_rec_result///',es_rec_result
        for item in es_rec_result:
            uid = item['key']
            uid_list.append(uid)
            
            avg_sort_uid_dict[uid] = {}

            if sort_item == 'user_fansnum':
                avg_sort_uid_dict[uid]['sort_item_value'] = int(item['avg_sort']['value'])
            else:
                avg_sort_uid_dict[uid]['sort_item_value'] = round(item['avg_sort']['value'],2)

    else:
        if S_TYPE == 'test':
            uid_list = FRIEND_LIST
            #sort_item = 'sensitive'
        else:
            uid_list = []
            '''
            friends_list_results = es_user_profile.mget(index=profile_index_name,doc_type=profile_index_type,body={'ids':recommend_set_list})['docs']
            for result in friends_list_results:
                friends_list = friends_list + result['friend_list']
            '''
            friends_list = get_friends_list(recommend_set_list)

            friends_set_list = list(set(friends_list))

            #uid_list = friends_set_list

            sort_item_new = 'fansnum'

            query_body_rec = {
                'query':{
                    'bool':{
                        'must':[
                            {'terms':{'uid':friends_set_list}},
                            {'bool':{
                                'should':nest_query_list
                            }}
                        ]
                    }
                },
                'aggs':{
                    'uid_list':{
                        'terms':{'field':'uid','size':TOP_ACTIVE_SOCIAL,'order':{'avg_sort':'desc'} },
                        'aggs':{'avg_sort':{'avg':{'field':sort_item_new}}}

                    }
                }
            }
            es_friend_result = es_flow_text.search(index=flow_text_index_name,doc_type='text',body=query_body_rec)['aggregations']['uid_list']['buckets']
            
            for item in es_friend_result:
                uid = item['key']
                uid_list.append(uid)
                
                avg_sort_uid_dict[uid] = {}
                
                if not item['avg_sort']['value']:
                    avg_sort_uid_dict[uid]['sort_item_value'] = 0
                else:
                    avg_sort_uid_dict[uid]['sort_item_value'] = int(item['avg_sort']['value'])
                
    results_all = []

    for uid in uid_list:
        #if sort_item == 'friend':
        query_body = {
            'query':{
                'filtered':{
                    'filter':{
                        'term':{'uid':uid}
                    }
                }
            }
        }

        es_results = es_user_portrait.search(index=portrait_index_name,doc_type=portrait_index_type,body=query_body)['hits']['hits']

    
       
        if es_results:
            #print 'portrait--',es_results[0]['_source'].keys()
            for item in es_results:
                uid = item['_source']['uid']
                #nick_name,photo_url = uid2nick_name_photo(uid)
                item['_source']['nick_name'] = uid #nick_name
                item['_source']['photo_url'] = ''#photo_url
                weibo_type = judge_follow_type(xnr_user_no,uid)
                sensor_mark = judge_sensing_sensor(xnr_user_no,uid)

                item['_source']['weibo_type'] = weibo_type
                item['_source']['sensor_mark'] = sensor_mark
                try:
				    del item['_source']['group']
				    del item['_source']['activity_geo_dict']
                except:
					pass


                if sort_item == 'friend':
                    if S_TYPE == 'test':
                        item['_source']['fansnum'] = item['_source']['fansnum']
                    else:
                        item['_source']['fansnum'] = avg_sort_uid_dict[uid]['sort_item_value']
                elif sort_item == 'sensitive':
                    item['_source']['sensitive'] = avg_sort_uid_dict[uid]['sort_item_value']
                    item['_source']['fansnum'] = item['_source']['fansnum']
                else:
                    item['_source']['fansnum'] = avg_sort_uid_dict[uid]['sort_item_value']

                if S_TYPE == 'test':
                    current_time = datetime2ts(S_DATE)
                else:
                    current_time = int(time.time())

                index_name = get_flow_text_index_list(current_time)

                query_body = {
                    'query':{
                        'bool':{
                            'must':[
                                {'term':{'uid':uid}},
                                {'terms':{'message_type':[1,3]}}
                            ]
                        }
                    },
                    'sort':{'retweeted':{'order':'desc'}},
                    'size':5
                }

                es_weibo_results = es_flow_text.search(index=index_name,doc_type=flow_text_index_type,body=query_body)['hits']['hits']

                weibo_list = []
                for weibo in es_weibo_results:
                    weibo = weibo['_source']
                    weibo_list.append(weibo)
                item['_source']['weibo_list'] = weibo_list
                item['_source']['portrait_status'] = True
                results_all.append(item['_source'])
        else:
            item_else = dict()
            item_else['uid'] = uid
            #nick_name,photo_url = uid2nick_name_photo(uid)
            item_else['nick_name'] = uid#nick_name
            item_else['photo_url'] = ''#photo_url
            weibo_type = judge_follow_type(xnr_user_no,uid)
            sensor_mark = judge_sensing_sensor(xnr_user_no,uid)
            item_else['weibo_type'] = weibo_type
            item_else['sensor_mark'] = sensor_mark
            item_else['portrait_status'] = False
            #if sort_item != 'friend':
            #item_else['sort_item_value'] = avg_sort_uid_dict[uid]['sort_item_value']
            # else:
            #     item_else['sort_item_value'] = ''
            

            if S_TYPE == 'test':
                current_time = datetime2ts(S_DATE)
            else:
                current_time = int(time.time())

            index_name = get_flow_text_index_list(current_time)

            query_body = {
                'query':{
                    'term':{'uid':uid}
                },
                'sort':{'retweeted':{'order':'desc'}}
            }

            es_weibo_results = es_flow_text.search(index=index_name,doc_type=flow_text_index_type,body=query_body)['hits']['hits']

            weibo_list = []
            for weibo in es_weibo_results:
                item_else['fansnum'] = weibo['_source']['user_fansnum']
                weibo = weibo['_source']
                weibo_list.append(weibo)
            item_else['weibo_list'] = weibo_list
            item_else['friendsnum'] = 0
            item_else['statusnum'] = 0
            if sort_item == 'sensitive':
                item_else['sensitive'] = avg_sort_uid_dict[uid]['sort_item_value']
            else:
                item_else['fansnum'] = avg_sort_uid_dict[uid]['sort_item_value']

            results_all.append(item_else)
            
    
    return results_all



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
	
	index_name = active_social_index_name_pre + current_date

	es.index(index=index_name,doc_type=active_social_index_type,body=item_body,id=_id)



def active_social_recommend_daily(current_date):

	# 1. 获得所有已完成虚拟人

	all_xnrs = get_all_xnrs()
	print 'all_xnrs',all_xnrs
	# 2. 对于每个虚拟人，计算 按粉丝数、按敏感度、按朋友圈 三个结果 并保存
	for xnr_user_no in all_xnrs:
		for sort_item in ['influence','sensitive','friend']:
			task_detail = {}
			print 'sort_item..',sort_item
			task_detail['xnr_user_no'] = xnr_user_no
			task_detail['sort_item'] = sort_item

			# 计算
			result = get_related_recommendation(task_detail)	
			print 'result',len(result)
			# 保存
			save_results_to_es(xnr_user_no,current_date,sort_item,result)	

     
if __name__ == '__main__':
	
	current_time = time.time()
	current_date = ts2datetime(current_time)
	start_ts = time.time()
	active_social_recommend_daily(current_date)
	end_ts = time.time()
	print 'cost..',end_ts - start_ts
