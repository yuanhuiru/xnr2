# -*- coding:utf-8 -*-
import os
import json
import time
from elasticsearch import Elasticsearch
import sys
sys.path.append('../../')
from global_utils import es_xnr,weibo_active_user_index_name_pre,weibo_active_user_index_type,\
                         weibo_xnr_fans_followers_index_name,weibo_xnr_fans_followers_index_type,\
                         es_user_portrait,weibo_bci_index_name_pre,weibo_bci_index_type,\
                         es_user_profile,profile_index_name,profile_index_type
from time_utils import ts2datetime
from parameter import DAY

sys.path.append('../../timed_python_files/monitor/')
from weibo_active_user_mappings import weibo_active_user_mappings 

#lookup weibo_xnr concerned users
def lookup_weiboxnr_concernedusers(weiboxnr_id):
    try:
        result=es_xnr.get(index=weibo_xnr_fans_followers_index_name,doc_type=weibo_xnr_fans_followers_index_type,id=weiboxnr_id)
        followers_list=result['_source']['followers_list']
    except:
    	followers_list=[]
    return followers_list

#计算影响力
def count_maxweibouser_influence(index_name):

    query_body={
        'query':{
            'match_all':{}
        },
        'size':1,
        'sort':{'user_index':{'order':'desc'}}
    }
    try:
        max_result=es_user_profile.search(index=index_name,doc_type=weibo_bci_index_type,body=query_body)['hits']['hits']
        for item in max_result:
           max_user_index=item['_source']['user_index']
    except:
        max_user_index=100
    return max_user_index


#计算影响力
def count_minweibouser_influence(index_name):

    query_body={
        'query':{
            'match_all':{}
        },
        'size':1,
        'sort':{'user_index':{'order':'asc'}}
    }
    try:
        max_result=es_user_profile.search(index=index_name,doc_type=weibo_bci_index_type,body=query_body)['hits']['hits']
        for item in max_result:
           max_user_index=item['_source']['user_index']
    except:
        max_user_index=0
    return max_user_index


def lookup_active_weibouser(today_date_time):
    weibo_active_user_index_name = weibo_active_user_index_name_pre + ts2datetime(today_date_time)
    weibo_active_user_mappings(weibo_active_user_index_name)

    bci_index_name = weibo_bci_index_name_pre + ''.join(ts2datetime(today_date_time).split('-'))

    # userlist = lookup_weiboxnr_concernedusers(weiboxnr_id)

    user_max_index=count_maxweibouser_influence(bci_index_name)
    user_min_index=count_minweibouser_influence(bci_index_name)

    results = []

    query_body={

        'query':{
            'match_all':{}
        },
        'size':100,       #查询影响力排名前50的用户即可
        'sort':{'user_index':{'order':'desc'}}
    }
    try:
        flow_text_exist=es_user_portrait.search(index=bci_index_name,\
                doc_type=weibo_bci_index_type,body=query_body)['hits']['hits']
        search_uid_list = [item['_source']['user'] for item in flow_text_exist]
        print len(search_uid_list)
        weibo_user_exist = es_user_profile.search(index=profile_index_name,\
                doc_type=profile_index_type,body={'query':{'terms':{'uid':search_uid_list}}})['hits']['hits']

        weibo_user_dict = dict()
        #user_dict = dict()
        for item_i in weibo_user_exist:
            uid = item_i['_source']['uid']
            weibo_user_dict[uid] = item_i['_source']
        for item in flow_text_exist:
            user_dict = dict()
            #print 'item:', item['_source']
            user_dict['influence'] =(item['_source']['user_index']-user_min_index)/(user_max_index - user_min_index)
            user_dict['fans_num'] = item['_source']['user_fansnum']
            user_dict['friends_num'] = item['_source']['user_friendsnum']
            user_dict['total_number'] = item['_source']['total_number']
            user_dict['uid'] = item['_source']['user']
            try:
                uid = user_dict['uid']
                weibo_user_info = weibo_user_dict[uid]
                user_dict['uname'] = weibo_user_info['nick_name']
                user_dict['location'] = weibo_user_info['user_location']
                user_dict['url'] = weibo_user_info['photo_url']
            except:
                user_dict['uname'] = ''
                user_dict['location'] = ''
                user_dict['url'] = ''

            #es_xnr.index(index_name= weibo_active_user_index_name,doc_type= weibo_active_user_index_type,body=user_dict,id=user_dict['uid'])
            results.append(user_dict)

    except:
        results=[]
    print len(results)
    return results

def create_active_user():
    now_time = int(time.time()) - DAY
    result_list = lookup_active_weibouser(now_time)
    weibo_active_user_index_name = weibo_active_user_index_name_pre + ts2datetime(now_time)
    count = 0
    bulk_create_action = []
    if result_list:
        for item in result_list:
            create_action = {'index':{'_id': item['uid']}}
            bulk_create_action.extend([create_action,item])
            count += 1
            if count % 99 == 0:
                es_xnr.bulk(bulk_create_action, index=weibo_active_user_index_name, doc_type=weibo_active_user_index_type)
                bulk_create_action = []
            if bulk_create_action:
                result = es_xnr.bulk(bulk_create_action, index=weibo_active_user_index_name, doc_type=weibo_active_user_index_type)
                if result['errors'] :
                    print result
                    return False

    return True

if __name__ == '__main__':
    create_active_user()

