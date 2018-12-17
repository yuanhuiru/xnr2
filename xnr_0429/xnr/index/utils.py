# -*- coding: utf-8 -*-
import time
import os
import sys
from xnr.parameter import TRANS_PATH
#trans_path = os.path.join(os.path.abspath(os.getcwd()), 'xnr/cron/trans/')
#trans_path = '/home/ubuntu8/yuanhuiru/xnr/xnr1/xnr/cron/trans/'
sys.path.append(TRANS_PATH)
from trans import voice2text
from load_weibo_url import load_weibo_url_func
#from trans import trans as text_trans
'''
def utils_text_trans(q):
    q_list = [q]
    result_list = text_trans(q_list)
    try:
        return result_list[0]
    except:
        return ''
'''

from baidu_trans import translate
def utils_text_trans(q):
    result = translate(q)
    try:
        return result
    except:
        return ''


def utils_voice_trans(voice_path):
    if os.path.isfile(voice_path):
        r = voice2text(voice_path)
        if r:
            return r
    return False

def utils_url_trans(uid, mid):
    url = load_weibo_url_func(uid, mid)
    if url:
        return url
    return False
