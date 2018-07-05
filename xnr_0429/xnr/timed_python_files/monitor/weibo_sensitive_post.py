#-*- coding: utf-8 -*-
'''
weibo monitor function
'''
import os
import json
import time
import sys
reload(sys)
# import sqlite
import sqlite3

sys.path.append('../../')
# from timed_python_files.system_log_create import get_user_account_list
from parameter import DAY,MAX_VALUE,WARMING_DAY,UID_TXT_PATH,MAX_SEARCH_SIZE,MAX_WARMING_SIZE,USER_XNR_NUM,\
                      FOLLOWER_INFLUENCE_MAX_JUDGE,NOFOLLOWER_INFLUENCE_MIN_JUDGE,EVENT_OFFLINE_COUNT,FLASK_DB_PATH,\
                      MAX_HOT_POST_SIZE,WEIBO_SENSITIVE_POST_TIME 

from time_utils import ts2datetime,datetime2ts
from global_utils import es_flow_text,flow_text_index_name_pre,flow_text_index_type,\
                         es_xnr,weibo_xnr_fans_followers_index_name,weibo_xnr_fans_followers_index_type,\
                         es_user_profile,profile_index_name,profile_index_type,\
                         weibo_xnr_index_name,weibo_xnr_index_type,\
                         weibo_sensitive_post_index_name_pre,weibo_sensitive_post_index_type

from global_utils import R_WEIBO_SENSITIVE  as r_sensitive
from global_utils import weibo_sensitive_post_task_queue_name 
import jieba.posseg as pseg

#from textrank4zh import TextRank4Keyword, TextRank4Sentence
sys.path.append('../../timed_python_files/monitor/')
from weibo_sensitive_post_mappings import weibo_sensitive_post_mappings

#连接数据库,获取账户列表
def get_user_account_list():     
    cx = sqlite3.connect(FLASK_DB_PATH)
    #cx = sqlite3.connect("/home/xnr1/xnr_0313/xnr/flask-admin.db") 
    cu=cx.cursor()
    cu.execute("select email from user") 
    user_info = cu.fetchall()
    cx.close()
    return user_info


def get_user_xnr_list(user_account):
    query_body={
        'query':{
        	'filtered':{
        		'filter':{
        			'bool':{
        				'must':[
        				{'term':{'submitter':user_account}},
        				{'term':{'create_status':2}}
        				]
        			}
        		}
        	}
        },
        'size':USER_XNR_NUM
    }
    try:
        user_result=es_xnr.search(index=weibo_xnr_index_name,doc_type=weibo_xnr_index_type,body=query_body)['hits']['hits']
        xnr_user_no_list=[]
        for item in user_result:
            xnr_user_no_list.append(item['_source']['xnr_user_no'])
    except:
        xnr_user_no_list=[]
    return xnr_user_no_list


def lookup_weiboxnr_concernedusers(weiboxnr_id):
    try:
        result=es_xnr.get(index=weibo_xnr_fans_followers_index_name,doc_type=weibo_xnr_fans_followers_index_type,id=weiboxnr_id)
        followers_list=result['_source']['followers_list']
    except:
    	followers_list=[]
    return followers_list


#交集判断
def set_intersection(str_A,list_B):
    list_A=[]
    list_A.append(str_A)
    set_A = set(list_A)
    set_B = set(list_B)
    result = set_A & set_B
    number = len(result)
    return number


#帖子去重处理
def remove_repeat(post_result,warning_type):
    origin_list = []
    filter_ids = []
    post_result_2 = post_result
    #print 'len post', len(post_result)
    for i in range(0,len(post_result)):
        set_mark = set_intersection(post_result[i]['_source']['mid'],filter_ids)
        #if not post_result[i]['mid'] in filter_ids:
        if set_mark == 0:
            for j in range(i+1,len(post_result_2)):
                re = filterDouble(post_result[i]['_source'], post_result_2[j]['_source'])
                #print 're:::',re
                if not re == 'not':
                    filter_ids.append(re)
    #print 'filter', len(set(filter_ids))
    new_post_results = []
    # followers_list =  lookup_weiboxnr_concernedusers(xnr_user_no)
    for item in post_result:
        set_flag = set_intersection(item['_source']['mid'],filter_ids)
        if set_flag == 0 :
            if warning_type == 'user':
            	item['_source']['xnr_user_no'] = ''
                item['_source']['nick_name'] = get_user_nickname(item['_source']['uid'])
                #好友判断
                item['_source']['search_type'] = 0
                # followers_mark = set_intersection(item['_source']['uid'],followers_list)
                # if followers_mark == 0:
                # 	item['_source']['search_type'] = -1
                # else:
                # 	item['_source']['search_type'] = 1

            new_post_results.append(item['_source'])

    #print '^^^^^', len(new_post_results)
    return new_post_results


def filterDouble(doc1, doc2):
    #print doc1['text'],doc1['mid']
    #print doc2['text'],doc2['mid']
    doc1_text = [w for w in pseg.cut(doc1['text'])]
    doc2_text = [w for w in pseg.cut(doc2['text'])]
    #rint len(doc1_text), len(doc2_text)
    #print doc1_text
    #print doc2_text
    if len(set(doc1_text)) == 0 or len(set(doc2_text))==0:
        simi_A = 0
        simi_B = 0
    else:
        simi_A = float(len(list(set(doc1_text)&set(doc2_text))))/len(set(doc1_text))
        simi_B = float(len(list(set(doc1_text)&set(doc2_text))))/len(set(doc2_text))
    #print 'simi::',simi_A,simi_B
    if (simi_A > 0.75) or (simi_B > 0.75):
        if simi_A > simi_B:
            return doc1['mid']
        else:
            return doc2['mid']
    else:
        return 'not'


#查询用户昵称
def get_user_nickname(uid):
    try:
        result=es_user_profile.get(index=profile_index_name,doc_type=profile_index_type,id=uid)
        user_name=result['_source']['nick_name']
    except:
        user_name=''
    return user_name


#查询敏感帖子
def lookup_sensitive_posts(start_time,end_time):
    start_date = ts2datetime(start_time)
    end_date = ts2datetime(end_time)
     
    flow_text_index_name_list = []
    if start_date == end_date:
        print '11'
        index_name = flow_text_index_name_pre + end_date
        flow_text_index_name_list.append(index_name)
        sensitive_index_name = weibo_sensitive_post_index_name_pre + end_date
        if es_xnr.indices.exists(index=sensitive_index_name):
            pass
        else:
            weibo_sensitive_post_mappings(sensitive_index_name)
            print '111'
    else:
        start_index_name = flow_text_index_name_pre + start_date
        end_index_name = flow_text_index_name_pre + end_date
        flow_text_index_name_list.append(start_index_name)
        flow_text_index_name_list.append(end_index_name)

        sensitive_start_index = weibo_sensitive_post_index_name_pre + start_date
        sensitive_end_index = weibo_sensitive_post_index_name_pre + end_date
        if not es_xnr.indices.exists(index=sensitive_start_index):
        	weibo_sensitive_post_mappings(sensitive_start_index)
        if not es_xnr.indices.exists(index=sensitive_end_index):
        	weibo_sensitive_post_mappings(sensitive_end_index)

      
    query_body = {
        'query':{
            'filtered':{
                'filter':{
                    'bool':{
                        'must':[
                             {'range':{'timestamp':{'gte':start_time,'lte':end_time}}},
                             {'range':{'sensitive':{'gte':1}}}
                        ]
                    }
                }
            }
        },
        'sort':{'sensitive':{'order':'desc'}},
        'size':100
    }
    print 'start search!!!'
    print flow_text_index_name_list
    try:
        es_result=es_flow_text.search(index=flow_text_index_name_list,doc_type=flow_text_index_type,\
            body=query_body)['hits']['hits']

        warning_type = 'user'
        print 'repeat!!!'
        hot_result = remove_repeat(es_result,warning_type)
        print 'save!!!'
        for item in hot_result:
            task_id = item['mid']
            # item['order_type']='sensitive'
            post_index_name = weibo_sensitive_post_index_name_pre + ts2datetime(item['timestamp'])
            es_xnr.index(index=post_index_name,doc_type=weibo_sensitive_post_index_type,body=item,id=task_id)
        # hot_result=[]
        # for item in es_result:
        #     item['_source']['nick_name']=get_user_nickname(item['_source']['uid'])
        #     hot_result.append(item['_source'])
        mark_result = True
        print 'finish!'
    except:
        mark_result = False
    
    return mark_result



#查询高影响力帖子
def lookup_influence_posts(start_time,end_time):
    start_date = ts2datetime(start_time)
    end_date = ts2datetime(end_time)
     
    flow_text_index_name_list = []
    if start_date == end_date:
        print '11'
        index_name = flow_text_index_name_pre + end_date
        flow_text_index_name_list.append(index_name)
        sensitive_index_name = weibo_sensitive_post_index_name_pre + end_date
        if es_xnr.indices.exists(index=sensitive_index_name):
            pass
        else:
            weibo_sensitive_post_mappings(sensitive_index_name)
            print '111'
    else:
        start_index_name = flow_text_index_name_pre + start_date
        end_index_name = flow_text_index_name_pre + end_date
        flow_text_index_name_list.append(start_index_name)
        flow_text_index_name_list.append(end_index_name)

        sensitive_start_index = weibo_sensitive_post_index_name_pre + start_date
        sensitive_end_index = weibo_sensitive_post_index_name_pre + end_date
        if not es_xnr.indices.exists(index=sensitive_start_index):
            weibo_sensitive_post_mappings(sensitive_start_index)
        if not es_xnr.indices.exists(index=sensitive_end_index):
            weibo_sensitive_post_mappings(sensitive_end_index)

      
    query_body = {
        'query':{
            'filtered':{
                'filter':{
                    'bool':{
                        'must':[
                             {'range':{'timestamp':{'gte':start_time,'lte':end_time}}}
                        ]
                    }
                }
            }
        },
        'sort':{'retweeted':{'order':'desc'}},
        'size':50
    }
    print 'start search!!!'
    print flow_text_index_name_list
    try:
        es_result=es_flow_text.search(index=flow_text_index_name_list,doc_type=flow_text_index_type,\
            body=query_body)['hits']['hits']

        warning_type = 'user'
        print 'repeat!!!'
        hot_result = remove_repeat(es_result,warning_type)
        print 'save!!!'
        for item in hot_result:
            task_id = item['mid']
            # item['order_type']='influence'
            post_index_name = weibo_sensitive_post_index_name_pre + ts2datetime(item['timestamp'])
            es_xnr.index(index=post_index_name,doc_type=weibo_sensitive_post_index_type,body=item,id=task_id)
        # hot_result=[]
        # for item in es_result:
        #     item['_source']['nick_name']=get_user_nickname(item['_source']['uid'])
        #     hot_result.append(item['_source'])
        mark_result = True
        print 'finish!'
    except:
        mark_result = False
    
    return mark_result


#查询最新帖子
def lookup_timestamp_posts(start_time,end_time):
    start_date = ts2datetime(start_time)
    end_date = ts2datetime(end_time)
     
    flow_text_index_name_list = []
    if start_date == end_date:
        print '11'
        index_name = flow_text_index_name_pre + end_date
        flow_text_index_name_list.append(index_name)
        sensitive_index_name = weibo_sensitive_post_index_name_pre + end_date
        if es_xnr.indices.exists(index=sensitive_index_name):
            pass
        else:
            weibo_sensitive_post_mappings(sensitive_index_name)
            print '111'
    else:
        start_index_name = flow_text_index_name_pre + start_date
        end_index_name = flow_text_index_name_pre + end_date
        flow_text_index_name_list.append(start_index_name)
        flow_text_index_name_list.append(end_index_name)

        sensitive_start_index = weibo_sensitive_post_index_name_pre + start_date
        sensitive_end_index = weibo_sensitive_post_index_name_pre + end_date
        if not es_xnr.indices.exists(index=sensitive_start_index):
            weibo_sensitive_post_mappings(sensitive_start_index)
        if not es_xnr.indices.exists(index=sensitive_end_index):
            weibo_sensitive_post_mappings(sensitive_end_index)

      
    query_body = {
        'query':{
            'filtered':{
                'filter':{
                    'bool':{
                        'must':[
                             {'range':{'timestamp':{'gte':start_time,'lte':end_time}}}
                        ]
                    }
                }
            }
        },
        'sort':{'timestamp':{'order':'desc'}},
        'size':50
    }
    print 'start search!!!'
    print flow_text_index_name_list
    try:
        es_result=es_flow_text.search(index=flow_text_index_name_list,doc_type=flow_text_index_type,\
            body=query_body)['hits']['hits']

        warning_type = 'user'
        print 'repeat!!!'
        hot_result = remove_repeat(es_result,warning_type)
        print 'save!!!'
        for item in hot_result:
            task_id = item['mid']
            # item['order_type']='timestamp'
            post_index_name = weibo_sensitive_post_index_name_pre + ts2datetime(item['timestamp'])
            es_xnr.index(index=post_index_name,doc_type=weibo_sensitive_post_index_type,body=item,id=task_id)
        # hot_result=[]
        # for item in es_result:
        #     item['_source']['nick_name']=get_user_nickname(item['_source']['uid'])
        #     hot_result.append(item['_source'])
        mark_result = True
        print 'finish!'
    except:
        mark_result = False
    
    return mark_result



def create_post_task():

    now_time = int(time.time())
    start_time = now_time - WEIBO_SENSITIVE_POST_TIME 
    end_time = now_time

    # account_list=get_user_account_list()
    #print account_list
    # for account in account_list:
    #     xnr_list=get_user_xnr_list(account)
    #     if xnr_list:
    #         print xnr_list
    #         for xnr_user_no in xnr_list:
    task_dict = dict()
    # task_dict['xnr_user_no'] = xnr_user_no
    task_dict['start_time'] = start_time
    task_dict['end_time'] = end_time
    #将计算任务加入队列
    r_sensitive.lpush(weibo_sensitive_post_task_queue_name ,json.dumps(task_dict))



if __name__ == '__main__':
    create_post_task()
