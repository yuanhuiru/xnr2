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
from weibo_warming import get_user_account_list,get_user_xnr_list


def create_weibo_warning():
    #时间设置
    now_time = int(time.time())
    #生成当前时间周期内的
    start_time = now_time - WEIBO_WARNING_PAUSE_TIME 
    end_time = now_time

    account_list=get_user_account_list()
    for account in account_list:
        xnr_list=get_user_xnr_list(account)

        for xnr_user_no in xnr_list:

                task_dict = dict()
                task_dict['xnr_user_no'] = xnr_user_no
                task_dict['start_time'] = start_time
                task_dict['end_time'] = end_time
                #将计算任务加入队列
                r_warning.lpush(weibo_user_warning_task_queue_name,json.dumps(task_dict))

                r_warning.lpush(weibo_speech_warning_task_queue_name,json.dumps(task_dict))

                r_warning.lpush(weibo_event_warning_task_queue_name,json.dumps(task_dict))

   #时间预警
    time_task = dict()
    time_task['start_time'] = start_time
    time_task['end_time'] = end_time
    r_warning.lpush(weibo_time_warning_task_queue_name,json.dumps(time_task))
    return True


if __name__ == '__main__':
    mark = create_weibo_warning()
    print mark
    

