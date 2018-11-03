# -*- coding: utf-8 -*-
import time
import sys
import os,datetime
from elasticsearch import helpers
# from datetime import datetime,date,timedelta

sys.path.append('../../')
from time_utils import ts2datetime,datetime2ts
from parameter import DAY
from global_utils import es_xnr,es_xnr_2

#waring
from global_utils import weibo_user_warning_index_name_pre,weibo_user_warning_index_type,\
                         facebook_user_warning_index_name_pre,facebook_user_warning_index_type,\
                         twitter_user_warning_index_name_pre,twitter_user_warning_index_type,\
                         weibo_event_warning_index_name_pre,weibo_event_warning_index_type,\
                         facebook_event_warning_index_name_pre,facebook_event_warning_index_type,\
                         twitter_event_warning_index_name_pre,twitter_event_warning_index_type,\
                         weibo_speech_warning_index_name_pre,weibo_speech_warning_index_type,\
                         facebook_speech_warning_index_name_pre,facebook_speech_warning_index_type,\
                         twitter_speech_warning_index_name_pre,twitter_speech_warning_index_type

from global_utils import weibo_user_history_warning_index_name,weibo_user_history_warning_index_type,\
                         facebook_user_history_warning_index_name,facebook_user_history_warning_index_type,\
                         twitter_user_history_warning_index_name,twitter_user_history_warning_index_type

from global_utils import weibo_event_history_warning_index_name,weibo_event_history_warning_index_type,\
                         facebook_event_history_warning_index_name,facebook_event_history_warning_index_type,\
                         twitter_event_history_warning_index_name,twitter_event_history_warning_index_type                         

from global_utils import weibo_speech_history_warning_index_name,weibo_speech_history_warning_index_type,\
                         facebook_speech_history_warning_index_name,facebook_speech_history_warning_index_type,\
                         twitter_speech_history_warning_index_name,twitter_speech_history_warning_index_type                         


#获取30天前的index_list
def get_history_date_from_today_list(es,index_pre,days):

    index_list = []
    today=datetime.datetime.now() 
    date_30 = today + datetime.timedelta(days=-30)
    for d in range(days):
        yes_day=date_30 + datetime.timedelta(days=-d)
        yes_day_str = yes_day.strftime('%Y-%m-%d')
        index_name = index_pre + yes_day_str
        print 'text_index:',index_name
        index_exist = es.indices.exists(index=index_name)
        if index_exist:
            print index_name
            index_list.append(index_name)
        else:
            pass
    return index_list


#数据批量处理

def get_search_result(es_search_options, es, index_name, doc_type, timeout="1m",scroll='5m'):
    es_result = helpers.scan(
        client=es,
        query=es_search_options,
        scroll=scroll,
        index=index_name,
        doc_type=doc_type,
        timeout=timeout
    )
    return es_result


def set_search_optional():
    # 检索选项
    es_search_options = {
        "query": {
            "match_all": {}
        }
    }
    return es_search_options


def data_bulk_process(es,pre_index,pre_index_type,save_index,save_index_type):
    es_search_options = set_search_optional()
    es_result = get_search_result(es_search_options,es,pre_index,pre_index_type) 
    #print 'es_len',len(es_result)
    index = 0
    bulk_action = []
    for cdr in es_result:
        index += 1
        action = {"index": {"_id": cdr['_id']}}
        bulk_action.extend([action, cdr['_source']])
        if index % 1000 == 0:        
            es.bulk(bulk_action, index=save_index, doc_type = save_index_type)
            bulk_action = []  
    if bulk_action:
        es.bulk(bulk_action, index=save_index, doc_type = save_index_type)
    if (bulk_action) or (index==0):
        es.indices.delete(index=pre_index)
    print 'finish insert'   

def main_data_clean(day_num):
    #初始化，创建存储表    

    #微博部分
    weibo_user_warning_index_name_list = get_history_date_from_today_list(es_xnr,weibo_user_warning_index_name_pre,day_num)
    for index_name in weibo_user_warning_index_name_list:
        print 'index_clean::',index_name
        r_result = data_bulk_process(es_xnr,index_name,weibo_user_warning_index_type,weibo_user_history_warning_index_name,weibo_speech_history_warning_index_type)


    weibo_event_warning_index_name_list = get_history_date_from_today_list(es_xnr,weibo_event_warning_index_name_pre,day_num)
    for index_name in weibo_event_warning_index_name_list:
        e_result = data_bulk_process(es_xnr,index_name,weibo_event_warning_index_type,weibo_event_history_warning_index_name,weibo_event_history_warning_index_type)

    weibo_speech_warning_index_name_list = get_history_date_from_today_list(es_xnr,weibo_speech_warning_index_name_pre,day_num)
    for index_name in weibo_speech_warning_index_name_list:
        s_result = data_bulk_process(es_xnr,index_name,weibo_speech_warning_index_type,weibo_speech_history_warning_index_name,weibo_speech_history_warning_index_type)


    #fb
    facebook_user_warning_index_name_list = get_history_date_from_today_list(es_xnr_2,facebook_user_warning_index_name_pre,day_num)
    for index_name in facebook_user_warning_index_name_list:
        fbr_result = data_bulk_process(es_xnr_2,index_name,facebook_user_warning_index_type,facebook_user_history_warning_index_name,facebook_user_history_warning_index_type)

    facebook_event_warning_index_name_list = get_history_date_from_today_list(es_xnr_2,facebook_event_warning_index_name_pre,day_num)
    for index_name in facebook_event_warning_index_name_list:
        fbe_result = data_bulk_process(es_xnr_2,index_name,facebook_event_warning_index_type,facebook_event_history_warning_index_name,facebook_event_history_warning_index_type)

    facebook_speech_warning_index_name_list = get_history_date_from_today_list(es_xnr_2,facebook_speech_warning_index_name_pre,day_num)
    for index_name in facebook_speech_warning_index_name_list:
        fbs_result = data_bulk_process(es_xnr_2,index_name,facebook_speech_warning_index_type,facebook_speech_history_warning_index_name,facebook_speech_history_warning_index_type)


   #tw
    twitter_user_warning_index_name_list = get_history_date_from_today_list(es_xnr_2,twitter_user_warning_index_name_pre,day_num)
    for index_name in twitter_user_warning_index_name_list:
        twr_result = data_bulk_process(es_xnr_2,index_name,twitter_user_warning_index_type,twitter_user_history_warning_index_name,twitter_user_history_warning_index_type)

    twitter_event_warning_index_name_list = get_history_date_from_today_list(es_xnr_2,twitter_event_warning_index_name_pre,day_num)
    for index_name in twitter_event_warning_index_name_list:
        twe_result = data_bulk_process(es_xnr_2,index_name,twitter_event_warning_index_type,twitter_event_history_warning_index_name,twitter_event_history_warning_index_type)

    twitter_speech_warning_index_name_list = get_history_date_from_today_list(es_xnr_2,twitter_speech_warning_index_name_pre,day_num)
    for index_name in twitter_speech_warning_index_name_list:
        tws_result = data_bulk_process(es_xnr_2,index_name,twitter_speech_warning_index_type,twitter_speech_history_warning_index_name,twitter_speech_history_warning_index_type)



if __name__ == '__main__':
	main_data_clean(365) #表示处理的天数



