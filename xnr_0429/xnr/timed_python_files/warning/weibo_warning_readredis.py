# -*- coding: utf-8 -*-
import os
import json
import time
import sys

from datetime import datetime,date

sys.path.append('../../')
from parameter import DAY,WEIBO_WARNING_PAUSE_TIME 
from time_utils import ts2datetime,datetime2ts
from global_utils import R_WEIBO_WARNING as r_warning
from global_utils import weibo_user_warning_task_queue_name,weibo_speech_warning_task_queue_name,\
                         weibo_event_warning_task_queue_name,weibo_time_warning_task_queue_name


sys.path.append('../../timed_python_files/warning/')
from weibo_warming import save_user_warning,create_speech_warning,save_event_warning,create_date_warning


def read_user_warning_task():

    while True:
        temp = r_warning.rpop(weibo_user_warning_task_queue_name)

        # print 'temp:::::',temp
        if not temp:
            print '当前没有生成xnr人物预警任务'         
            break
        task_detail = json.loads(temp)
        xnr_user_no = task_detail['xnr_user_no']
        start_time = task_detail['start_time']
        end_time = task_detail['end_time']
        
        print 'task_detail::',task_detail

        print '把任务从队列中pop出来......'

        user_mark = save_user_warning(xnr_user_no,start_time,end_time)

        print 'user_warning::',user_mark


def read_speech_warning_task():

    while True:
        temp = r_warning.rpop(weibo_speech_warning_task_queue_name)

        # print 'temp:::::',temp
        if not temp:
            print '当前没有生成xnr言论预警任务'         
            break
        task_detail = json.loads(temp)
        xnr_user_no = task_detail['xnr_user_no']
        start_time = task_detail['start_time']
        end_time = task_detail['end_time']
        
        print 'task_detail::',task_detail

        print '把任务从队列中pop出来......'

        speech_mark = create_speech_warning(xnr_user_no,start_time,end_time)

        print 'speech_warning::',speech_mark



def read_event_warning_task():

    while True:
        temp = r_warning.rpop(weibo_event_warning_task_queue_name)

        # print 'temp:::::',temp
        if not temp:
            print '当前没有生成xnr事件预警任务'         
            break
        task_detail = json.loads(temp)
        xnr_user_no = task_detail['xnr_user_no']
        start_time = task_detail['start_time']
        end_time = task_detail['end_time']
        
        print 'task_detail::',task_detail

        print '把任务从队列中pop出来......'

        event_mark = save_event_warning(xnr_user_no,start_time,end_time)

        print 'event_warning::',event_mark



def read_time_warning_task():

    while True:
        temp = r_warning.rpop(weibo_time_warning_task_queue_name)

        # print 'temp:::::',temp
        if not temp:
            print '当前没有生成xnr时间预警任务'         
            break
        task_detail = json.loads(temp)
        start_time = task_detail['start_time']
        end_time = task_detail['end_time']
        
        print 'task_detail::',task_detail

        print '把任务从队列中pop出来......'

        date_mark = create_date_warning(start_time,end_time)

        print 'date_warning::',date_mark




if __name__ == '__main__':
   read_user_warning_task()

   read_speech_warning_task()

   read_event_warning_task()

   read_time_warning_task()
