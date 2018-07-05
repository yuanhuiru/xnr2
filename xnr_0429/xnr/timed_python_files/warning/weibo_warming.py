#-*- coding: utf-8 -*-
'''
weibo warming function
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
                      MAX_HOT_POST_SIZE
from global_config import S_TYPE,S_DATE_BCI,S_DATE_WARMING,S_DATE
from time_utils import ts2datetime,datetime2ts,get_day_flow_text_index_list,ts2yeartime
from global_utils import R_CLUSTER_FLOW2 as r_cluster
from global_utils import es_xnr,weibo_xnr_fans_followers_index_name,weibo_xnr_fans_followers_index_type,\
                         es_flow_text,flow_text_index_name_pre,flow_text_index_type,\
                         weibo_feedback_follow_index_name,weibo_feedback_follow_index_type,\
                         weibo_user_warning_index_name_pre,weibo_user_warning_index_type,\
                         weibo_xnr_index_name,weibo_xnr_index_type,\
                         weibo_speech_warning_index_name_pre,weibo_speech_warning_index_type,\
                         weibo_timing_warning_index_name_pre,weibo_timing_warning_index_type,\
                         weibo_date_remind_index_name,weibo_date_remind_index_type,\
                         weibo_event_warning_index_name_pre,weibo_event_warning_index_type,\
                         es_user_profile,profile_index_name,profile_index_type
import jieba.posseg as pseg
sys.path.append('../../timed_python_files/warning/')
from weibo_warning_mappings import weibo_user_warning_mappings,weibo_event_warning_mappings,\
                                   weibo_speech_warning_mappings,weibo_timing_warning_mappings

#连接数据库,获取账户列表
def get_user_account_list():     
    cx = sqlite3.connect(FLASK_DB_PATH)
    #cx = sqlite3.connect("/home/xnr1/xnr_0313/xnr/flask-admin.db") 
    cu=cx.cursor()
    cu.execute("select email from user") 
    user_info = cu.fetchall()
    cx.close()
    return user_info


#查询用户昵称
def get_user_nickname(uid):
    try:
        user_result=es_user_profile.get(index=profile_index_name,doc_type=profile_index_type,id=uid)['_source']
        user_name=user_result['nick_name']
    except:
        user_name=''
    return user_name

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

#查询关注列表或者粉丝列表
#lookup_type='followers_list'或者'fans_list'
def lookup_xnr_fans_followers(user_id,lookup_type):
    try:
        xnr_result=es_xnr.get(index=weibo_xnr_fans_followers_index_name,doc_type=weibo_xnr_fans_followers_index_type,id=user_id)['_source']
        lookup_list=xnr_result[lookup_type]
    except:
        lookup_list=[]
    return lookup_list

#查询虚拟人uid
def lookup_xnr_uid(xnr_user_no):
    try:
        xnr_result=es_xnr.get(index=weibo_xnr_index_name,doc_type=weibo_xnr_index_type,id=xnr_user_no)['_source']
        xnr_uid=xnr_result['uid']
    except:
        xnr_uid=''
    return xnr_uid


def get_xnr_warming_index_listname(index_name_pre,date_range_start_ts,date_range_end_ts):
    index_name_list=[]
    if ts2datetime(date_range_start_ts) != ts2datetime(date_range_end_ts):
        iter_date_ts=date_range_end_ts
        while iter_date_ts >= date_range_start_ts:
            date_range_start_date=ts2datetime(iter_date_ts)
            index_name=index_name_pre+date_range_start_date
            if es_xnr.indices.exists(index=index_name):
                index_name_list.append(index_name)
            else:
                pass
            iter_date_ts=iter_date_ts-DAY
    else:
        date_range_start_date=ts2datetime(date_range_start_ts)
        index_name=index_name_pre+date_range_start_date
        if es_xnr.indices.exists(index=index_name):
            index_name_list.append(index_name)
        else:
            pass
    return index_name_list


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
    for item in post_result:
        set_flag = set_intersection(item['_source']['mid'],filter_ids)
        if set_flag == 0 :
            if warning_type == 'user':
                item['_source']['nick_name'] = get_user_nickname(item['_source']['uid'])
            elif warning_type =='speech':
                pass

            new_post_results.append(item['_source'])

    #print '^^^^^', len(new_post_results)
    return new_post_results

def remove_repeat_v2(post_result):
    origin_list = []
    filter_ids = []
    post_result_2 = post_result
    #print 'len post', len(post_result)
    for i in range(0,len(post_result)):
        set_mark = set_intersection(post_result[i]['mid'],filter_ids)
        #if not post_result[i]['mid'] in filter_ids:
        if set_mark == 0:
            for j in range(i+1,len(post_result_2)):
                re = filterDouble(post_result[i], post_result_2[j])
                #print 're:::',re
                if not re == 'not':
                    filter_ids.append(re)
    #print 'filter', len(set(filter_ids))
    new_post_results = []

    for item in post_result:
        set_flag = set_intersection(item['mid'],filter_ids)
        if set_flag == 0 :
            new_post_results.append(item)

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

###查询历史人物预警信息
def lookup_history_user_warming(xnr_user_no,start_time,end_time):
    query_body={
       'query':{
            'filtered':{
                'filter':{
                    'bool':{
                        'must':[
                            {'term':{'xnr_user_no':xnr_user_no}},
                            {'range':{
                                'timestamp':{
                                    'gte':start_time,
                                    'lte':end_time
                                }
                            }}
                        ]
                    }
                }
            }
        },
        'sort':{'user_sensitive':{'order':'asc'}} ,
        'size':MAX_WARMING_SIZE
    }

    user_warming_list=get_xnr_warming_index_listname(weibo_user_warning_index_name_pre,start_time,end_time)
    uid_list = []
    try:
        temp_results=es_xnr.search(index=user_warming_list,doc_type=weibo_user_warning_index_type,body=query_body)['hits']['hits']
        results=[]
        for item in temp_results:
            #item['_source']['_id']=item['_id']
            uid_list.append(item['_source']['uid'])
            results.append(item['_source'])
        #results.sort(key=lambda k:(k.get('user_sensitive',0)),reverse=True)
    except:
        results=[]
    #print results
    return results,uid_list


#人物行为预警
def create_personal_warning(xnr_user_no,start_time,end_time):
    #查询关注列表
    lookup_type='followers_list'
    followers_list=lookup_xnr_fans_followers(xnr_user_no,lookup_type)

    #查询虚拟人uid
    xnr_uid=lookup_xnr_uid(xnr_user_no)

    #计算敏感度排名靠前的用户
    '''
    query_body={
        # 'query':{
        #     'filtered':{
        #         'filter':{
        #             'terms':{'uid':followers_list}
        #         }
        #     }
        # },
        'aggs':{
            'followers_sensitive_num':{
                'terms':{'field':'uid'},
                'range':{
                        'timestamp':{
                            'gte':start_time,
                            'lte':end_time
                        }
                        },
                'aggs':{
                    'sensitive_num':{
                        'sum':{'field':'sensitive'}
                    }
                }                  
            }
            },
        'size':MAX_SEARCH_SIZE
    }
    '''
    query_body={
        # 'query':{
        #     'filtered':{
        #         'filter':{
        #             'terms':{'uid':followers_list}
        #         }
        #     }
        # },
        'aggs':{
            'followers_sensitive_num':{
                'terms':{'field':'uid'},
                'aggs':{
                    'sensitive_num':{
                        'sum':{'field':'sensitive'}
                    }
                }                        
            }
            },
        'size':MAX_SEARCH_SIZE
    }
    flow_text_index_name=get_day_flow_text_index_list(end_time)
    
   # try:   
    first_sum_result=es_flow_text.search(index=flow_text_index_name,doc_type=flow_text_index_type,\
        body=query_body)['aggregations']['followers_sensitive_num']['buckets']
    #except:
     #`   first_sum_result=[]

    #print first_sum_result
    top_userlist=[]
    for i in xrange(0,len(first_sum_result)):
        user_sensitive=first_sum_result[i]['sensitive_num']['value']
        #ser_influence=first_sum_result[i]['influence_num']['value']
        #if (user_sensitive > 0) or (user_influence > 0) :
        if user_sensitive > 0:
            user_dict=dict()
            user_dict['uid']=first_sum_result[i]['key']
            followers_mark=judge_user_type(user_dict['uid'],followers_list)
            user_dict['sensitive']=user_sensitive*followers_mark
           # user_dict['influence']=user_influence*followers_mark
            top_userlist.append(user_dict)
        else:
            pass



    ####################################
    #如果是关注者则敏感度提升
    ####################################
    #查询敏感用户的敏感微博内容
    results=[]
    for user in top_userlist:
        #print user
        user_detail=dict()
        user_detail['uid']=user['uid']
        user_detail['user_sensitive']=user['sensitive']
        #user_detail['user_influence']=user['influence']
        # user_lookup_id=xnr_uid+'_'+user['uid']
        # print user_lookup_id
        # try:
        #     #user_result=es_xnr.get(index=weibo_feedback_follow_index_name,doc_type=weibo_feedback_follow_index_type,id=user_lookup_id)['_source']
        #     user_result=es_user_profile.get(index=profile_index_name,doc_type=profile_index_type,id=user['uid'])['_source']
        #     user_detail['user_name']=user_result['nick_name']
        # except:
        user_detail['user_name']=get_user_nickname(user['uid'])

        query_body={
            'query':{
                'filtered':{
                    'filter':{
                        'bool':{
                            'must':[
                                {'term':{'uid':user['uid']}},
                            #    {'range':{'timestamp':{'gte':start_time,'lte':end_time}}}
                                {'range':{'sensitive':{'gte':1}}},
                            #    {'range':{'retweeted':{'gte':1}}}
                            ]
                        }
                    }
                }
            },
            'size':MAX_WARMING_SIZE,
            'sort':{'sensitive':{'order':'desc'}}
        }

        try:
            second_result=es_flow_text.search(index=flow_text_index_name,doc_type=flow_text_index_type,body=query_body)['hits']['hits']
        except:
            second_result=[]
        warning_type = 'user'
        s_result=remove_repeat(second_result,warning_type)
        #tem_word_one = '静坐'
        #tem_word_two = '集合'
        # for item in second_result:
        #     #sensitive_words=item['_source']['sensitive_words_string']
        #     #if ((sensitive_words==tem_word_one) or (sensitive_words==tem_word_two)):
        #     #    pass
        #     #else:
        #     #查询用户昵称

        #     item['_source']['nick_name']=get_user_nickname(item['_source']['uid'])
        #     s_result.append(item['_source'])

        

        s_result.sort(key=lambda k:(k.get('sensitive',0)),reverse=True)
        user_detail['content']=json.dumps(s_result)

        user_detail['xnr_user_no']=xnr_user_no
        user_detail['validity']=0
        user_detail['timestamp']=end_time

        results.append(user_detail)
    #print 'person_wa:::',results
    return results


def save_user_warning(xnr_user_no,start_time,end_time):

    #判断数据库是否存在：
    today_date=ts2datetime(end_time)
    today_datetime = datetime2ts(today_date)
    weibo_user_warning_index_name=weibo_user_warning_index_name_pre+today_date
    if not es_xnr.indices.exists(index=weibo_user_warning_index_name):
        weibo_user_warning_mappings(weibo_user_warning_index_name)

    
    new_user_warning = create_personal_warning(xnr_user_no,start_time,end_time)

    today_history_user_warning,old_uid_list = lookup_history_user_warming(xnr_user_no,today_datetime,end_time)

    results = []
    if new_user_warning:
        for item in new_user_warning:
            id_mark = set_intersection(item['uid'],old_uid_list)
            if id_mark == 1:
                #组合,更新数据库
                task_id = xnr_user_no+'_'+item['uid']
                old_user = es_xnr.get(index=weibo_user_warning_index_name,doc_type=weibo_user_warning_index_type,id=task_id)['_source']
                old_user['content'] = json.loads(old_user['content'])
                old_user['content'].extend(item['content'])
                old_user['user_sensitive'] = old_user['user_sensitive'] + item['user_sensitive']
                #old_user['user_influence'] = old_user['user_influence'] + item['user_influence']
                try:
                    es_xnr.index(index=weibo_user_warning_index_name,doc_type=weibo_user_warning_index_type,body=old_user,id=task_id)
                    mark=True
                except:
                    mark=False

            else:
                #直接存储
                task_id=xnr_user_no+'_'+item['uid']
                try:
                    es_xnr.index(index=weibo_user_warning_index_name,doc_type=weibo_user_warning_index_type,body=item,id=task_id)
                    mark=True
                except:
                    mark=False

            results.append(mark)
    else:
        pass
    print 'person_mark::',results
    return results

def lookup_history_speech_warming(xnr_user_no,start_time,end_time):  
    query_body={
        'query':{
            'filtered':{
                'filter':{
                    'bool':{'must':[{'term':{'xnr_user_no':xnr_user_no}},{'range':{'timestamp':{'gte':start_time,'lte':end_time}}}]}
                }
            }
        },
        'size':MAX_WARMING_SIZE,
        'sort':{'timestamp':{'order':'desc'}}
    }

    speech_warming_list=get_xnr_warming_index_listname(weibo_speech_warning_index_name_pre,start_time,end_time)
    # print 'speech_warming_list:',speech_warming_list
    # print show_condition_list[0]
    try:
        temp_results=es_xnr.search(index=speech_warming_list,doc_type=weibo_speech_warning_index_type,body=query_body)['hits']['hits']
        # print 'temp_results',temp_results
        results=[]
        for item in temp_results:
            results.append(item['_source'])
        #results.sort(key=lambda k:(k.get('sensitive',0)),reverse=True)
    except:
        results=[]
    return results   
 
#言论内容预警
def create_speech_warning(xnr_user_no,start_time,end_time):
    #查询关注列表
    lookup_type='followers_list'
    followers_list=lookup_xnr_fans_followers(xnr_user_no,lookup_type)
    result = [] 
    query_body={
        'query':{
            'filtered':{
                'filter':{
                    'bool':{
                        'must':[
                            {'range':{'sensitive':{'gte':1}}},
                            {'range':{'timestamp':{'gte':start_time,'lte':end_time}}}
                         ]}
                }
            }
        },
        'size':MAX_HOT_POST_SIZE,
        'sort':{'sensitive':{'order':'desc'}}
    }

    flow_text_index_name=get_day_flow_text_index_list(end_time)

    results=es_flow_text.search(index=flow_text_index_name,doc_type=flow_text_index_type,body=query_body)['hits']['hits']
    
    warning_type = 'speech'
    r_result = remove_repeat(results,warning_type)
    for item in r_result:
        item['nick_name']=get_user_nickname(item['uid'])
        followers_mark = set_intersection(item['uid'],followers_list)
        if followers_mark != 0:
            item['content_type']='follow'
        else:
            item['content_type']='unfollow'

        item['validity']=0
        item['xnr_user_no']=xnr_user_no

        task_id=xnr_user_no+'_'+item['mid']

        #写入数据库
        today_date=ts2datetime(end_time)
        weibo_speech_warning_index_name=weibo_speech_warning_index_name_pre+today_date
        if not es_xnr.indices.exists(index=weibo_speech_warning_index_name):
            weibo_speech_warning_mappings(weibo_speech_warning_index_name)
        try:
            es_xnr.index(index=weibo_speech_warning_index_name,doc_type=weibo_speech_warning_index_type,body=item,id=task_id)
            mark=True
        except:
            mark=False

        result.append(mark)
    print 'speech_result::',result
    return result


#事件预警
#事件涌现思路：
#（1）根据get_hashtag获取事件名称
#（2）在流数据中查询与事件名相关的微博数据，
#（3）根据虚拟人编号查找粉丝和关注人的uid，统计事件名称相关的微博数据中粉丝、关注人出现的频次，如果既是关注人又是粉丝则频次相加。取频次前三用户
#（4）计算微博数据的转发数、评论数、敏感等级，得到微博影响力的初始值,
#计算微博影响力的值=初始影响力值X（粉丝值（是1.2，否0.8）+关注值（是1.2，否0.8）
# def get_hashtag(now_time):

#     uid_list = []
#     hashtag_list = {}

#     with open(UID_TXT_PATH+'/uid_sensitive.txt','rb') as f:
#         for line in f:
#             uid = line.strip()
#             uid_list.append(uid)

#     for uid in uid_list:
#         if S_TYPE == 'test':
#             hashtag = r_cluster.hget('hashtag_' + str(datetime2ts(S_DATE_WARMING)-DAY),uid)
#             #hashtag = r_cluster.hget('hashtag_'+str(datetime2ts(S_DATE)+7*DAY),uid)
#         else:
#             hashtag = r_cluster.hget('hashtag_' + str(now_time),uid)
#             #hashtag = r_cluster.hget('hashtag_'+str((time.time()-DAY)),uid)

#         if hashtag != None:
#             hashtag = hashtag.encode('utf8')
#             hashtag = json.loads(hashtag)

#             for k,v in hashtag.iteritems():
#                 try:
#                     hashtag_list[k] += v
#                 except:
#                     hashtag_list[k] = v
#         #r_cluster.hget('hashtag_'+str(a))

#     hashtag_list = sorted(hashtag_list.items(),key=lambda x:x[1],reverse=True)[:80]

#     return hashtag_list


#事件预警
def get_hashtag(today_datetime):
    
    weibo_flow_text_index_name=get_day_flow_text_index_list(today_datetime)
    query_body={
        'query':{
            'filtered':{
                'filter':{
                'bool':{
                    'must':[
                        {'range':{'sensitive':{'gte':1}}}
                    ]
                }}
            }
        },
        'aggs':{
            'all_hashtag':{
                'terms':{'field':'hashtag'},
                'aggs':{'sum_sensitive':{
                    'sum':{'field':'sensitive'}
                }
                }
            }
        },
        'size':EVENT_OFFLINE_COUNT
    }
    weibo_text_exist=es_flow_text.search(index=weibo_flow_text_index_name,doc_type=flow_text_index_type,\
                body=query_body)['aggregations']['all_hashtag']['buckets']
    
    hashtag_list = []
    for item in weibo_text_exist:
        event_dict=dict()
        if item['key']:
            # print item['key']
            event_dict['event_name'] = item['key']
            event_dict['event_count'] = item['doc_count']
            event_dict['event_sensitive'] = item['sum_sensitive']['value']
            hashtag_list.append(event_dict)
        else:
            pass

    hashtag_list.sort(key=lambda k:(k.get('event_sensitive',0),k.get('event_count',0)),reverse=True)
    # print hashtag_list
    return hashtag_list


def create_event_warning(xnr_user_no,start_time,end_time):
    #获取事件名称
    today_datetime = start_time
    hashtag_list = get_hashtag(today_datetime)
    #print 'hashtag_list::',hashtag_list

    flow_text_index_name = get_day_flow_text_index_list(today_datetime)

    #虚拟人的粉丝列表和关注列表
    try:
        es_xnr_result=es_xnr.get(index=weibo_xnr_fans_followers_index_name,doc_type=weibo_xnr_fans_followers_index_type,id=xnr_user_no)['_source']
        followers_list=es_xnr_result['followers_list']
        fans_list=es_xnr_result['fans_list']
    except:
        followers_list=[]
        fans_list=[]

    event_warming_list=[]
    event_num=0
    for event_item in hashtag_list:
        event_sensitive_count=0
        event_warming_content=dict()     #事件名称、主要参与用户、典型微博、事件影响力、事件平均时间
        event_warming_content['event_name']=event_item['event_name']
        print 'event_name:',event_item
        event_num=event_num+1
        print 'event_num:::',event_num
        print 'first_time:::',int(time.time())
        event_influence_sum=0
        event_time_sum=0       
        query_body={
            'query':{
                # 'bool':{
                #     'must':[{'wildcard':{'text':'*'+event_item[0]+'*'}},
                #     {'range':{'sensitive':{'gte':1}}}]
                # }
                'filtered':{
                    'filter':{
                        'bool':{
                            'must':[
                                {'term':{'hashtag':event_item['event_name']}},
                                {'range':{'sensitive':{'gte':1}}},
                                {'range':{'timestamp':{'gte':start_time,'lte':end_time}}}
                            ]
                        }
                    }
                }
            },
            'size':MAX_WARMING_SIZE,
            'sort':{'sensitive':{'order':'desc'}}
        }
        #try:         
        event_results=es_flow_text.search(index=flow_text_index_name,doc_type=flow_text_index_type,body=query_body)['hits']['hits']
        print 'event:::',len(event_results),start_time,end_time
        if event_results:
            weibo_result=[]
            fans_num_dict=dict()
            followers_num_dict=dict()
            alluser_num_dict=dict()
            print 'sencond_time:::',int(time.time())
            for item in event_results:
                #print 'event_content:',item['_source']['text']          
                
                #统计用户信息
                if alluser_num_dict.has_key(str(item['_source']['uid'])):
                    followers_mark=set_intersection(item['_source']['uid'],followers_list)
                    if followers_mark > 0:
                        alluser_num_dict[str(item['_source']['uid'])]=alluser_num_dict[str(item['_source']['uid'])]+1*2
                    else:
                        alluser_num_dict[str(item['_source']['uid'])]=alluser_num_dict[str(item['_source']['uid'])]+1
                else:
                    alluser_num_dict[str(item['_source']['uid'])]=1                

                #计算影响力
                origin_influence_value=(1+item['_source']['comment']+item['_source']['retweeted'])*(1+item['_source']['sensitive'])
                # fans_value=judge_user_type(item['_source']['uid'],fans_list)
                followers_value=judge_user_type(item['_source']['uid'],followers_list)
                item['_source']['weibo_influence_value']=origin_influence_value*(followers_value)
                
                item['_source']['nick_name']=get_user_nickname(item['_source']['uid'])

                weibo_result.append(item['_source'])

                #统计影响力、时间
                event_influence_sum=event_influence_sum+item['_source']['weibo_influence_value']
                event_time_sum=event_time_sum+item['_source']['timestamp']            
        
            print 'third_time:::',int(time.time())
            #典型微博信息
            the_weibo_result=remove_repeat_v2(weibo_result)
            the_weibo_result.sort(key=lambda k:(k.get('weibo_influence_value',0)),reverse=True)
            event_warming_content['main_weibo_info']=json.dumps(the_weibo_result)

            #事件影响力和事件时间
            number=len(event_results)
            event_warming_content['event_influence']=event_influence_sum/number
            event_warming_content['event_time']=event_time_sum/number

        # except:
        #     event_warming_content['main_weibo_info']=[]
        #     event_warming_content['event_influence']=0
        #     event_warming_content['event_time']=0
        
        # try:
            #对用户进行排序
            alluser_num_dict=sorted(alluser_num_dict.items(),key=lambda d:d[1],reverse=True)
            main_userid_list=[]
            for i in xrange(0,len(alluser_num_dict)):
                main_userid_list.append(alluser_num_dict[i][0])

        #主要参与用户信息
            main_user_info=[]
            user_es_result=es_user_profile.mget(index=profile_index_name,doc_type=profile_index_type,body={'ids':main_userid_list})['docs']
            for item in user_es_result:

                user_dict=dict()
                if item['found']:
                    user_dict['photo_url']=item['_source']['photo_url']
                    user_dict['uid']=item['_id']
                    user_dict['nick_name']=item['_source']['nick_name']
                    user_dict['favoritesnum']=item['_source']['favoritesnum']
                    user_dict['fansnum']=item['_source']['fansnum']
                else:
                    user_dict['photo_url']=''
                    user_dict['uid']=item['_id']
                    user_dict['nick_name']=''
                    user_dict['favoritesnum']=0
                    user_dict['fansnum']=0
                main_user_info.append(user_dict)
            event_warming_content['main_user_info']=json.dumps(main_user_info)


        # except:
            # event_warming_content['main_user_info']=[]
            print 'fourth_time:::',int(time.time())
            event_warming_content['xnr_user_no']=xnr_user_no
            event_warming_content['validity']=0
            event_warming_content['timestamp']=today_datetime

            event_warming_list.append(event_warming_content)
        else:
        	pass
        print 'fifth_time:::',int(time.time())
    return event_warming_list


# def write_envent_warming(today_datetime,event_warming_content,task_id):
#     weibo_event_warning_index_name=weibo_event_warning_index_name_pre+ts2datetime(today_datetime)
#     print 'weibo_event_warning_index_name:',weibo_event_warning_index_name
#     #try:
#     es_xnr.index(index=weibo_event_warning_index_name,doc_type=weibo_event_warning_index_type,body=event_warming_content,id=task_id)
#     mark=True
#     #except:
#     #    mark=False
#     return mark

def lookup_history_event_warming(xnr_user_no,start_time,end_time):
    query_body={
       'query':{
            'filtered':{
                'filter':{
                    'bool':{
                        'must':[
                            {'term':{'xnr_user_no':xnr_user_no}},
                            {'range':{
                                'timestamp':{
                                    'gte':start_time,
                                    'lte':end_time
                                }
                            }}
                        ]
                    }
                }
            }
        },
        'sort':{'event_influence':{'order':'asc'}} ,
        'size':MAX_WARMING_SIZE
    }

    event_warming_list=get_xnr_warming_index_listname(weibo_event_warning_index_name_pre,start_time,end_time)
    name_list = []
    #try:
    temp_results=es_xnr.search(index=event_warming_list,doc_type=weibo_event_warning_index_type,body=query_body)['hits']['hits']
    results=[]
    for item in temp_results:
        # item['_source']['_id']=item['_id']
        name_list.append(item['_source']['event_name'])
        results.append(item['_source'])
    # results.sort(key=lambda k:(k.get('event_influence',0)),reverse=True)
    #except:
    #    results=[]
    #print results
    return results,name_list

def save_event_warning(xnr_user_no,start_time,end_time):
    #判断数据库是否存在：
    today_date=ts2datetime(end_time)
    today_datetime = datetime2ts(today_date)
    weibo_event_warning_index_name = weibo_event_warning_index_name_pre+today_date
    if not es_xnr.indices.exists(index=weibo_event_warning_index_name):
        weibo_event_warning_mappings(weibo_event_warning_index_name)

    new_event_warning = create_event_warning(xnr_user_no,start_time,end_time)    

    today_history_event_warning,old_name_list = lookup_history_event_warming(xnr_user_no,today_datetime,end_time)
    print 'warning!!!',len(new_event_warning)
    results = [] 
    if new_event_warning:
        for item in new_event_warning:
            event_mark = set_intersection(item['event_name'],old_name_list)
            if event_mark == 1:
                task_id = xnr_user_no+'_'+ item['event_name']
                old_event = es_xnr.get(index=weibo_event_warning_index_name,doc_type=weibo_event_warning_index_type,id=task_id)['_source']

                #用户合并
                old_event_main_info = json.loads(old_event['main_user_info'])
                old_event_uid_list = [user['uid'] for user in old_event_main_info]

                new_event_main_info = json.loads(item['main_user_info'])
                new_event_uid_list = [user['uid'] for user in new_event_main_info]

                add_uid_list = list(set(new_event_uid_list) - set(old_event_uid_list)&set(new_event_uid_list))

                new_main_user_info = []
                item_main_user_info = json.loads(item['main_user_info'])
                for uid in add_uid_list:
                    
                    uid_info = [u for u in item_main_user_info if u['uid'] == uid]
                    if uid_info:
                        new_main_user_info.append(uid_info[0])
                    else:
                        pass
                old_event['main_user_info'] = json.loads(old_event['main_user_info'])
                old_event['main_user_info'].extend(new_main_user_info)


                old_event_weibo_info = json.loads(old_event['main_weibo_info'])
                old_event_mid_list = [content['mid'] for content in old_event_weibo_info]

                new_event_weibo_info = json.loads(item['main_weibo_info'])
                new_event_mid_list = [content['mid'] for content in new_event_weibo_info]

                add_weibo_list = list(set(new_event_mid_list) - set(new_event_mid_list)&set(old_event_mid_list))     

                new_main_weibo_info = []
                for mid in add_weibo_list:
                    mid_info = [t for t in item['main_weibo_info'] if t['mid'] == mid]
                    if mid_info:
                        new_main_weibo_info.append(mid_info[0])
                    else:
                        pass
                old_event['main_weibo_info'] = json.loads(old_event['main_weibo_info'])
                old_event['main_weibo_info'].extend(new_main_weibo_info)

                old_event['event_influence']=old_event['event_influence']+item['event_influence']
               
                try:
                    es_xnr.update(index=weibo_event_warning_index_name,doc_type=weibo_event_warning_index_type,id=task_id)
                    mark=True
                except:
                    mark=False

            else:
                #直接存储
                task_id=xnr_user_no+'_'+ item['event_name']
                try:
                    es_xnr.index(index=weibo_event_warning_index_name,doc_type=weibo_event_warning_index_type,body=item,id=task_id)
                    mark=True
                except:
                    mark=False
            results.append(mark)
    else:
        pass
    print 'event_waring::',results
    return results




#粉丝或关注用户判断
def judge_user_type(uid,user_list):
    number=set_intersection(uid,user_list)
    if number > 0:
        mark=FOLLOWER_INFLUENCE_MAX_JUDGE
    else:
        mark=NOFOLLOWER_INFLUENCE_MIN_JUDGE
    return mark

def union_dict(*objs):
    #print 'objs:', objs[0]
    _keys=set(sum([obj.keys() for obj in objs],[]))
    _total={}

    for _key in _keys:
        _total[_key]=sum([int(obj.get(_key,0)) for obj in objs])

    return _total

#交集判断
def set_intersection(str_A,list_B):
    list_A=[]
    list_A.append(str_A)
    set_A = set(list_A)
    set_B = set(list_B)
    result = set_A & set_B
    number = len(result)
    return number


#时间预警
def create_date_warning(start_time,end_time):
    today_datetime = end_time
    query_body={
        'query':{
        	'match_all':{}
        },
        'size':MAX_VALUE,
        'sort':{'date_time':{'order':'asc'}}
    }
    #try:
    result=es_xnr.search(index=weibo_date_remind_index_name,doc_type=weibo_date_remind_index_type,body=query_body)['hits']['hits']
    date_result=[]
    for item in result:
        #计算距离日期
        date_time=item['_source']['date_time']
        year=ts2yeartime(today_datetime)
        warming_date=year+'-'+date_time
        today_date=ts2datetime(today_datetime)
        countdown_num=(datetime2ts(warming_date)-datetime2ts(today_date))/DAY
    
        if abs(countdown_num) < WARMING_DAY:
            #根据给定的关键词查询预警微博
            keywords=item['_source']['keywords']
            date_warming=lookup_weibo_date_warming(keywords,start_time,end_time)
            
            item['_source']['weibo_date_warming_content']=json.dumps(date_warming)
            item['_source']['validity']=0
            item['_source']['timestamp']=datetime2ts(ts2datetime(today_datetime))
            now_time=int(time.time())
            task_id=str(item['_source']['date_time'])+'_'+ ts2datetime(today_datetime)    
            #print 'task_id',task_id
            #写入数据库
            
            weibo_timing_warning_index_name=weibo_timing_warning_index_name_pre+warming_date
            #print weibo_timing_warning_index_name
            mark = False
            if date_warming:
                if not es_xnr.indices.exists(index=weibo_timing_warning_index_name):
                    weibo_timing_warning_mappings(weibo_timing_warning_index_name)
                #try:
                es_xnr.index(index=weibo_timing_warning_index_name,doc_type=weibo_timing_warning_index_type,body=item['_source'],id=task_id)
                mark=True
                #except:
                 #   mark=False
                  #  print 'write error!'
            else:
                pass
            date_result.append(mark)
    else:
        pass

    #except:
    #    date_result=[]
    return date_result


def lookup_weibo_date_warming(keywords,start_time,end_time):
    keyword_query_list=[]
    for keyword in keywords:
        # keyword = keyword.encode('utf-8')
        print 'keyword:',keyword, type(keyword)
        keyword_query_list.append({'wildcard':{'text': '*'+keyword+'*'}})
        # keyword_query_list.append({'wildcard':{'text':{'wildcard':'*'+keyword.encode('utf-8')+'*'}}})

    flow_text_index_name=get_day_flow_text_index_list(end_time)
    
    # keyword_query_list.append({'range':{'sensitive':{'gte':1}}})

    query_body={
        'query':{
            'bool':
            { 
                'must':[{'range':{'timestamp':{'gte':start_time,'lte':end_time}}}],
                'should': keyword_query_list
            }
        },
        'size':MAX_WARMING_SIZE,
        'sort':{'sensitive':{'order':'desc'}}
    }
    if es_flow_text.indices.exists(index=flow_text_index_name):
        try:
            temp_result=es_flow_text.search(index=flow_text_index_name,doc_type=flow_text_index_type,body=query_body)['hits']['hits']
            warning_type = 'user'
            date_result=remove_repeat(temp_result,warning_type)
        # print keyword_query_list
        # for item in temp_result:
        #     # print 'item-text:', item['_source']['text'], type(item['_source']['text'])
        #     item['_source']['nick_name']=get_user_nickname(item['_source']['uid'])
        #     date_result.append(item['_source'])
        except:
               date_result=[]
    else:
        pass
    return date_result


#微博预警内容组织
def create_weibo_warning():
    #时间设置
    if S_TYPE == 'test':
        test_day_date=S_DATE_WARMING
        today_datetime=datetime2ts(test_day_date) - DAY
        start_time=today_datetime
        end_time=today_datetime
        operate_date=ts2datetime(start_time)
        print 'operate_date:',operate_date 
    else:
        now_time=int(time.time())
        today_datetime=datetime2ts(ts2datetime(now_time)) - DAY 
        start_time=today_datetime    #前一天0点
        end_time=today_datetime + DAY         #定时文件启动的0点
        operate_date=ts2datetime(start_time)
    
    print 'today_datetime',today_datetime,'operate_date',operate_date
    account_list=get_user_account_list()
    for account in account_list:
        xnr_list=get_user_xnr_list(account)

        for xnr_user_no in xnr_list:
            #人物行为预警
            personal_mark=save_user_warning(xnr_user_no,start_time,end_time)
            #言论内容预警
            speech_mark=create_speech_warning(xnr_user_no,start_time,end_time)
            speech_mark=True
            #事件涌现预警
            save_event_warning(xnr_user_no,start_time,end_time)

    #时间预警
    date_mark=create_date_warning(start_time,end_time)
    return True


if __name__ == '__main__':
    first_time = int(time.time())
    create_weibo_warning()
    end_time = int(time.time())
    print 'cost_time::',end_time - first_time

    
    # now_time = int(time.time())
    # #生成当前时间周期内的
    # start_time = now_time - WEIBO_WARNING_PAUSE_TIME 
    # end_time = now_time
    # save_user_warning(xnr_user_no,start_time,end_time)
    # create_speech_warning(xnr_user_no,start_time,end_time)
    # save_event_warning(xnr_user_no,start_time,end_time)


    # create_date_warning(start_time,end_time)
