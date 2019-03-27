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
from weibo_sensitive_post import lookup_sensitive_posts,lookup_influence_posts,lookup_timestamp_posts

def pop_post_task():

    while True:
        temp = r_sensitive.rpop(weibo_sensitive_post_task_queue_name)
        
        # print 'temp:::::',temp
        if not temp:
            print '当前没有生成信息监测帖子任务'         
            break
        task_detail = json.loads(temp)
        # xnr_user_no = task_detail['xnr_user_no']
        start_time = task_detail['start_time']
        end_time = task_detail['end_time']
        
        print 'task_detail::',task_detail

        print '把任务从队列中pop出来......'
        print '敏感帖子：：：'        
        sensitive_mark = lookup_sensitive_posts(start_time,end_time)
        print '热门帖子：：：' 
        influence_mark = lookup_influence_posts(start_time,end_time)
        print '最新帖子：：：'
        timestamp_mark = lookup_timestamp_posts(start_time,end_time)

        print 'sensitive::',sensitive_mark,influence_mark,timestamp_mark


if __name__ == '__main__':
    pop_post_task()
