# -*- coding: utf-8 -*-
import json
import sys
sys.path.append('/home/xnr1/xnr_0429/xnr/')
import re
import time
import random
import traceback
from elasticsearch import Elasticsearch

from weibo_operate import SinaOperateAPI
from utils import uid2xnr_user_no
from tools.Launcher import SinaLauncher

reload(sys)
sys.setdefaultencoding('utf-8')


es = Elasticsearch(hosts=[{'host': '192.168.169.45', 'port': 9205}, {'host': '192.168.169.47', 'port': 9205}, {'host': '192.168.169.47', 'port': 9206}])


def follow_single(xnr_uid, uid, flag):
    result = es.get(index='weibo_xnr', doc_type='user' ,id=xnr_uid)['_source']
    weibo_phone_account = result['weibo_phone_account']
    weibo_mail_account = result['weibo_mail_account']
    password = result['password']
    print weibo_phone_account, password
    sina_operate_api = SinaOperateAPI(weibo_phone_account, password)
    if flag:
        print sina_operate_api.followed(uid=uid)
    else:
        print sina_operate_api.unfollowed(uid=uid)


if __name__ == '__main__':
    follow_single(xnr_uid='WXNR0152', uid='5239676768', flag=True)
