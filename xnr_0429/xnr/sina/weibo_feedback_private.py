# -*- coding: utf-8 -*-
import json
import sys

import re
import time
import traceback

from tools.Launcher import SinaLauncher
from tools.ElasticsearchJson import executeES

reload(sys)
sys.setdefaultencoding('utf-8')

class FeedbackPrivate:
    def __init__(self, username, password):
        self.launcher = SinaLauncher(username, password)
        self.launcher.login()
        self.uid = self.launcher.uid
        self.session = self.launcher.session

    @staticmethod
    def switch_datetime_to_timestamp(raw_datatime):
        raw_datatime = raw_datatime.decode('utf-8')
        print raw_datatime
        timestamp = '0'
        if '秒前'.decode('utf-8') in raw_datatime:
            second = re.search('\d+', raw_datatime).group(1)
            timestamp = str(int(time.time()) + int(second))
        elif '分钟前'.decode('utf-8') in raw_datatime:
            minute = re.search('(\d+)', raw_datatime).group(1)
            timestamp = str(int(time.time()) + int(minute) * 60)
        elif '小时前'.decode('utf-8') in raw_datatime:
            hour = re.search('(\d+)', raw_datatime).group(1)
            timestamp = str(int(time.time()) + int(hour) * 60 * 60)
        elif raw_datatime.count('-') == 1:
            year = time.strftime('%Y', time.localtime(time.time()))
            raw_datatime = year+'-'+raw_datatime
            timestamp = str(int(time.mktime(time.strptime(raw_datatime, '%Y-%m-%d'))))
        elif raw_datatime.count('-') == 2:
            timestamp = str(int(time.mktime(time.strptime(raw_datatime, '%Y-%m-%d'))))
        return timestamp

    def messages(self):
        json_list = []
        page = 1
        while True:
            messages_url = 'https://m.weibo.cn/message/msglist?page={}'.format(page)
            resp = self.session.get(messages_url)
            if not resp.json()['data']:
                break
            else:
                pass
            data_list = resp.json()['data']
            for data in data_list:
                photo_url = data['user']['avatar_large']
                uid = data['user']['id']
                nick_name = data['user']['screen_name']
                timestamp = self.switch_datetime_to_timestamp(data['created_at'])
                text = data['text']
                root_uid = uid
                #_type = 'stranger'
                #type1 = 'followed' if data['user']['following'] is True else ''
                #type2 = 'follow' if data['user']['follow_me'] is True else ''
                #weibo_type = 'friend' if type1 and type2 else _type
                if uid == self.uid:
                    private_type = 'make'
                else:
                    private_type = 'receive'
                update_time = int(time.time())
                item = {
                    'photo_url': photo_url,
                    'uid': str(uid),
                    'nick_name': nick_name,
                    'mid': '',
                    'timestamp': timestamp,
                    'text': text,
                    'root_uid': str(self.uid),
                    'weibo_type': '',
                    'root_uid': str(root_uid),
                    'private_type': private_type,
                    'w_new_count': '0',
                    'update_time': update_time
                }
                json_list.append(json.dumps(item, ensure_ascii=False))
            page += 1
        return json_list

    def execute(self):
        mess = self.messages()
        executeES('weibo_feedback_private', 'text', mess)


if __name__ == '__main__':
    #weibo_feedback_private = FeedbackPrivate('13269704912', 'murcielagolp640')
    weibo_feedback_private = FeedbackPrivate('18737028295', 'xuanhui99999')
    print weibo_feedback_private.messages()

