# -*- coding: utf-8 -*-
import os
import json
import time
import sys

from datetime import datetime,date

sys.path.append('../../')
from global_utils import R_WEIBO_SENSITIVE  as r_sensitive
from global_utils import weibo_sensitive_post_task_queue_name 

sys.path.append('../../timed_python_files/monitor/')
from weibo_sensitive_post import lookup_sensitive_posts

def pop_sensitive_post_task():

    while True:
        temp = r_sensitive.rpop(weibo_sensitive_post_task_queue_name)

        # print 'temp:::::',temp
        if not temp:
            print '当前没有生成敏感帖子任务'         
            break
        task_detail = json.loads(temp)
        xnr_user_no = task_detail['xnr_user_no']
        start_time = task_detail['start_time']
        end_time = task_detail['end_time']
        
        print 'task_detail::',task_detail

        print '把任务从队列中pop出来......'

        sensitive_mark = lookup_sensitive_posts(xnr_user_no,start_time,end_time)

        print 'sensitive::',sensitive_mark


if __name__ == '__main__':
    pop_sensitive_post_task()
