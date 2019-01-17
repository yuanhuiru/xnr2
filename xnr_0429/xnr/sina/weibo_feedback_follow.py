# -*- coding: utf-8 -*-
import json
import sys
sys.path.append('/home/xnr1/xnr_0429/xnr/')
import re
import time
import traceback
from utils import uid2xnr_user_no
from tools.Launcher import SinaLauncher
from tools.ElasticsearchJson import executeES

reload(sys)
sys.setdefaultencoding('utf-8')

class FeedbackFollow:
    def __init__(self, username, password):
        self.launcher = SinaLauncher(username, password)
        self.launcher.login()
        self.uid = self.launcher.uid
        self.session = self.launcher.session

    @staticmethod
    def read_datetime(raw_datatime):
        datetime_splited_list = raw_datatime.split(' ')
        month = str(datetime_splited_list[1])
        month = [i.values()[0] for i in
                 [{'Jan': '01'}, {'Feb': '02'}, {'Mar': '03'}, {'Apr': '04'}, {'May': '05'}, {'Jun': '06'},
                  {'Jul': '07'}, {'Aug': '08'}, {'Sep': '09'}, {'Oct': '10'}, {'Nov': '11'}, {'Dec': '12'}] if
                month == i.keys()[0]][0]
        day = datetime_splited_list[2]
        time = datetime_splited_list[3]
        year = datetime_splited_list[5]
        datetime = year + '-' + month + '-' + day + ' ' + time
        return datetime

    def follow(self):
        json_list = []
        page = 1
        while True:
            follow_url = 'https://m.weibo.cn/api/container/getIndex?containerid=231093_-_selffollowed&page={}'.format(page)
            resp = self.session.get(follow_url)
            if resp.json().has_key('msg'):
                break
            if page == 1:
                cards = resp.json()['data']['cards']
                for card in cards:
                    try:
                        if card['title'].decode('utf-8') == '全部关注':
                            data_list = card['card_group']
                    except:
                        continue
            else:
                cards = resp.json()['data']['cards'][0]
                data_list = cards['card_group']
            for data in data_list:
                photo_url = data['user']['profile_image_url']
                uid = data['user']['id']
                nickname = data['user']['screen_name']
                if not nickname:
                    nickname = ''
                #print nickname
                mid = uid
                sex = data['user']['gender']
                if not sex:
                    sex = 'unknown'
                elif sex == 0:
                    sex = 'female'
                elif sex == 1:
                    sex = 'male'
                description = data['user']['description']
                if not description:
                    description = ''
                root_uid = uid
                update_time = int(time.time())
                item = {
                    'photo_url': photo_url,
                    'uid': str(uid),
                    'mid': str(uid),
                    'nick_name': nickname,
                    'timestamp': 0,
                    'sex': sex,
                    'description': description,
                    'follow_source': '',
                    'gid': '0',
                    'gname': '',
                    'root_uid': self.uid,
                    'weibo_type': 'follow',
                    'update_time': update_time
                }
                json_list.append(json.dumps(item, ensure_ascii=False))
            page += 1
        return json_list

    def fans(self):
        json_list = []
        page = 1
        while True:
            fans_url = 'https://m.weibo.cn/api/container/getIndex?containerid=231016_-_selffans&page={}'.format(page)
            resp = self.session.get(fans_url)
            #print resp.text
            if resp.json().has_key('msg'):
                break
            if page == 1:
                cards = resp.json()['data']['cards']
                for card in cards:
                    try:
                        if card['title'].decode('utf-8') == '全部粉丝':
                            data_list = card['card_group']
                    except:
                        continue
            else:
                cards = resp.json()['data']['cards'][0]
                data_list = cards['card_group']
            
            for data in data_list:
                photo_url = data['user']['profile_image_url']
                uid = data['user']['id']
                nickname = data['user']['screen_name']
                if not nickname:
                    nickname = ''
                #print nickname
                mid = uid
                sex = data['user']['gender']
                if not sex:
                    sex = 'unknown'
                elif sex == 0:
                    sex = 'female'
                elif sex == 1:
                    sex = 'male'
                else:
                    sex = 'unknown'
                follower = data['user']['follow_count']
                if not follower:
                    follower = 0
                fans = data['user']['followers_count']
                if not fans:
                    fans = 0
                description = data['user']['description']
                if not description:
                    description = ''
                root_uid = uid
                update_time = int(time.time())
                item = {
                    'photo_url': photo_url,
                    'uid': str(uid),
                    'mid': str(uid),
                    'nick_name': nickname,
                    'timestamp': 0,
                    'sex': sex,
                    'follower': str(follower),
                    'fan_source': '',
                    'fans': str(fans),
                    'geo': '',
                    'description': description,
                    'weibos': '',
                    'root_uid': str(self.uid),
                    'weibo_type': 'followed',
                    'update_time': update_time
                }
                json_list.append(json.dumps(item, ensure_ascii=False))
            page += 1
        return json_list

    def execute(self):

        follow = self.follow()
        fans = self.fans()
        groups = ''
        print 'follow', follow, len(follow)
        print 'fans', fans, len(fans)
        print '+++++++++++++++++++++++++++++++++++++'
        executeES('weibo_feedback_follow', 'text', follow)
        print '-------------------------------------'
        executeES('weibo_feedback_fans', 'text', fans)
        print '+++++++++++++++++++++++++++++++++++++'
        #return fans, follow, groups
        

if __name__ == '__main__':
    #weibo_feedback_follow = FeedbackFollow('sosisuki@163.com', '2012hlwxxc')
    weibo_feedback_follow = FeedbackFollow('18737028295', 'xuanhui99999')
    # print weibo_feedback_follow.follow()
    #print weibo_feedback_follow.fans()
    weibo_feedback_follow.execute()
