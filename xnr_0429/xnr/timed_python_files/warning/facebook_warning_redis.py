# -*- coding: utf-8 -*-
import os
import json
import time
import sys

from datetime import datetime,date

sys.path.append('../../')
from parameter import DAY 
from time_utils import ts2datetime,datetime2ts
from global_utils import R_FACEBOOK_WARNING as r_warning
from global_utils import fb_user_warning_task_queue_name,fb_speech_warning_task_queue_name,\
                         fb_event_warning_task_queue_name,fb_time_warning_task_queue_name

sys.path.append('../../timed_python_files/warning/')
from weibo_warming import get_user_account_list
from facebook_warming import get_user_xnr_list
from facebook_warming import create_personal_warning,create_speech_warning, create_event_warning,create_date_warning
from facebook_warning_mappings import facebook_user_warning_mappings,facebook_event_warning_mappings,facebook_speech_warning_mappings,\
                                      lookup_date_info,facebook_timing_warning_mappings


def create_fb_warning():
    #时间设置
    now_time = int(time.time())
    #生成当前时间周期内的
    start_time = datetime2ts(ts2datetime(now_time))

    #生成表
    for i in range(0,3,1):
        datetime = start_time - i*DAY
        datename = ts2datetime(datetime)
        facebook_user_warning_mappings(datename)
        facebook_event_warning_mappings(datename)
        facebook_speech_warning_mappings(datename)

        date_result=lookup_date_info(datetime)
        facebook_timing_warning_mappings(date_result)

    account_list=get_user_account_list()
    for account in account_list:
        xnr_list=get_user_xnr_list(account)

        for xnr_user_no in xnr_list:
                for i in range(0,3,1):
                    task_dict = dict()
                    task_dict['xnr_user_no'] = xnr_user_no
                    task_dict['today_datetime'] = start_time - i*DAY
                    #将计算任务加入队列
                    r_warning.lpush(fb_user_warning_task_queue_name,json.dumps(task_dict))

                    r_warning.lpush(fb_speech_warning_task_queue_name,json.dumps(task_dict))

                    r_warning.lpush(fb_event_warning_task_queue_name,json.dumps(task_dict))

   #时间预警
    time_task = dict()
    for i in range(0,3,1):
        time_task['today_datetime'] = start_time - i*DAY
        r_warning.lpush(fb_time_warning_task_queue_name,json.dumps(time_task))
    return True



def read_user_warning_task():

    while True:
        temp = r_warning.rpop(fb_user_warning_task_queue_name)

        # print 'temp:::::',temp
        if not temp:
            print '当前没有生成fb_xnr人物预警任务'         
            break
        task_detail = json.loads(temp)
        xnr_user_no = task_detail['xnr_user_no']
        today_datetime = task_detail['today_datetime']

        
        print 'task_detail::',task_detail

        print '把任务从队列中pop出来......'

        user_mark = create_personal_warning(xnr_user_no,today_datetime)

        print 'user_warning::',user_mark


def read_speech_warning_task():

    while True:
        temp = r_warning.rpop(fb_speech_warning_task_queue_name)

        # print 'temp:::::',temp
        if not temp:
            print '当前没有生成fb_xnr言论预警任务'         
            break
        task_detail = json.loads(temp)
        xnr_user_no = task_detail['xnr_user_no']
        today_datetime = task_detail['today_datetime']
        
        print 'task_detail::',task_detail

        print '把任务从队列中pop出来......'

        speech_mark = create_speech_warning(xnr_user_no,today_datetime)

        print 'speech_warning::',speech_mark



def read_event_warning_task():

    while True:
        temp = r_warning.rpop(fb_event_warning_task_queue_name)

        # print 'temp:::::',temp
        if not temp:
            print '当前没有生成fb_xnr事件预警任务'         
            break
        task_detail = json.loads(temp)
        xnr_user_no = task_detail['xnr_user_no']
        today_datetime = task_detail['today_datetime']
        
        print 'task_detail::',task_detail

        print '把任务从队列中pop出来......'
        write_mark = True
        event_mark = create_event_warning(xnr_user_no,today_datetime,write_mark)

        print 'event_warning::',event_mark



def read_time_warning_task():

    while True:
        temp = r_warning.rpop(fb_time_warning_task_queue_name)

        # print 'temp:::::',temp
        if not temp:
            print '当前没有生成fb_xnr时间预警任务'         
            break
        task_detail = json.loads(temp)
        today_datetime = task_detail['today_datetime']
        
        print 'task_detail::',task_detail

        print '把任务从队列中pop出来......'

        date_mark = create_date_warning(today_datetime)

        print 'date_warning::',date_mark





if __name__ == '__main__':
    mark = create_fb_warning()
    if mark:
        read_user_warning_task()

        read_speech_warning_task()

        read_event_warning_task()

        read_time_warning_task()
    
