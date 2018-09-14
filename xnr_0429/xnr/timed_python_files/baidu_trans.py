# -*- coding: utf-8 -*-
import httplib
import md5
import urllib
import random
import langid
import re
import json
import time

appinfo = [
    ('20170921000084362', 'uKPwOwSfMDG4Byrq1ey7')]

class TransRes():
    def __init__(self, src, text):
        self.src = src
        self.text = text

class Translator():   
    def load_appinfo(self):
        return random.choice(appinfo)
    
    def trans(self, q):
        appKey, secretKey = self.load_appinfo()
        salt = str(random.randint(32768, 65536))
        s1 = md5.new()
        s1.update(appKey + q + salt + secretKey)
        sign = s1.hexdigest()
        myurl = '/api/trans/vip/translate?appid='+appKey+'&q='+urllib.quote(q)+'&from=auto&to='+self.toLang+'&salt='+salt+'&sign='+sign
        try:
            httpClient = httplib.HTTPConnection('api.fanyi.baidu.com')
            httpClient.request('GET', myurl)
            response = httpClient.getresponse()
            result = json.loads(response.read())
            return result['trans_result'][0]['dst']
        except Exception, e:
            print 'Exception: ', str(e)
            return False
        finally:
            if httpClient:
                httpClient.close()
    
    def langdetect(self, q):
        return langid.classify(q)[0]
            
    def translate(self, q, target_language='zh-cn', test_num=2):
        if target_language == 'zh-cn':
            self.toLang = 'zh'
        elif target_language == 'en':
            self.toLang = 'en'
        
        src = self.langdetect(q)
        if src == 'zh':
            text = q
        else:
            while test_num:
                text = self.trans(q)
                if text:
                    break
                test_num = test_num - 1
            if not text:
                text = q
        
        return TransRes(src, text)
        
if __name__ == '__main__':
    q = "‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏طالبة جامعية ‏‏من مواليد [١٩٩١م] طموحي الحصول على الدكتوراه في تخصصي من هواياتي: كتابه الخط العربي وعشقي مايسمى [بالتصوير"
    q = '你好啊'
    q = u'\u30b9\u30de\u30dbRPG\u306f\u4eca\u3053\u308c\u3092\u3084\u3063\u3066\u308b\u3088\u3002\u4eca\u306f\u3053\u306e\u30a4\u30d9\u30f3\u30c8\u304c\u958b\u50ac\u4e2d\uff01\u3000\u2192\u3000https://t.co/Fhdz3QQ3OI https://t.co/Jo2iC4w5OB'
    q = q.encode('utf8')
    r = Translator().translate(q)
    print r.text, r.src
    


