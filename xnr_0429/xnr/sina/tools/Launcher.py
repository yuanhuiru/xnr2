# -*- coding: utf-8 -*-
import requests
import random
import json
import traceback
import sys
from utils_error_es import save_error_es
reload(sys)
sys.setdefaultencoding('utf-8')

class SinaLauncher():
    def __init__(self, username, password, account_type='mail'):
        self.password = password
        self.username = username
        self.account_type = account_type
        self.uid = ''

        self.user_agents = [
            'Mozilla/5.0 (iPhone; CPU iPhone OS 9_1 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 '
            'Mobile/13B143 Safari/601.1]',
            'Mozilla/5.0 (Linux; Android 5.0; SM-G900P Build/LRX21T) AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/48.0.2564.23 Mobile Safari/537.36',
            'Mozilla/5.0 (Linux; Android 5.1.1; Nexus 6 Build/LYZ28E) AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/48.0.2564.23 Mobile Safari/537.36']

        self.headers = {
            'User_Agent': random.choice(self.user_agents),
            'Referer': 'https://passport.weibo.cn/signin/login?entry=mweibo&res=wel&wm=3349&r=http%3A%2F%2Fm.weibo.cn%2F',
            'Origin': 'https://passport.weibo.cn',
            'Host': 'passport.weibo.cn'
        }
        self.post_data = {
            'username': self.username,
            'password': self.password,
            'savestate': '1',
            'r': '',
            'ec': '0',
            'pagerefer': 'https://passport.weibo.cn/signin/welcome?entry=mweibo&r=http%3A%2F%2Fm.weibo.cn%2F&wm=3349&vt=4',
            'entry': 'mweibo',
            'wentry': '',
            'loginfrom': '',
            'client_id': '',
            'code': '',
            'qq': '',
            'mainpageflag': '1',
            'hff': '',
            'hfp': ''
        }
    def login(self):
        try:
            self.session = requests.session()

            login_url = 'https://passport.weibo.cn/sso/login'

            r = self.session.post(login_url, data=self.post_data, headers=self.headers)
            data = json.loads(r.text)
            url1 = data['data']['crossdomainlist']['weibo.com']
            url2 = data['data']['crossdomainlist']['sina.com.cn']
            url3 = data['data']['crossdomainlist']['weibo.cn']
            self.uid = data['data']['uid']
            self.session.get(url1)
            self.session.get(url2)
            self.session.get(url3)
            url = 'https://weibo.com/u/{}/home'.format(self.uid)
            header = {
                "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36",
                "Referer": "https://weibo.com/",
            }
            self.session.get(url,headers=header)
            if r.status_code != 200:
                print "Login error!"
                # 在此如果出错的话，则写入错误帐号到weibo_xnr表中的verify_password字段。  
                save_error_es(self.username, self.account_type)
                print "hhhhhhhhhhhhhhhhhhhhhhhhhhhhahahahahhhhhhhhhhhhhhhhhha"
                return False
            else:
                print "Login success!"
                return True
        except Exception, e:
            traceback.print_exc(e)
            save_error_es(self.username, self.account_type)
            print "hhhhhhhhhhhhhhhhhhhhhhhhhhhhahahahahhhhhhhhhhhhhhhhhha"
            print "Login error!"
            return False

if __name__ == '__main__':
    test = SinaLauncher('13269704912','murcielagolp640')
    test.login()
    print test.uid

