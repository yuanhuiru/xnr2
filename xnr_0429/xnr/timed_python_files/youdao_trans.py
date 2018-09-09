# -*- coding: utf-8 -*-
import httplib
import md5
import urllib
import random
import langid

appinfo = [
    ('3026da95302fe406', 'D6Sueb74kdW3nj6qjSaMTGGQTngoHMjW')]

class TransRes():
    def __init__(self, src, text):
        self.src = src
        self.text = text

class Translator():   
    def load_appinfo(self):
        return random.choice(appinfo)
    
    def trans(self):
        appKey, secretKey = self.load_appinfo()
        salt = str(random.randint(1, 65536))
        s1 = md5.new()
        s1.update(appKey + q + salt + secretKey)
        sign = s1.hexdigest()
        myurl = '/api?appKey='+appKey+'&q='+urllib.quote(q)+'&from=auto&to='+self.toLang+'&salt='+salt+'&sign='+sign
        try:
            httpClient = httplib.HTTPConnection('openapi.youdao.com')
            httpClient.request('GET', myurl)
            response = httpClient.getresponse()
            result = eval(response.read())
            return result['translation'][0].decode('utf8')
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
            self.toLang = 'zh-CHS'
        elif target_language == 'en':
            self.toLang = 'EN'

        while test_num:
            text = self.trans()
            if text:
                break
            test_num = test_num - 1
        if not text:
            text = q
        src = self.langdetect(q)
        return TransRes(src, text)
        
if __name__ == '__main__':
    q = "‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏طالبة جامعية ‏‏من مواليد [١٩٩١م] طموحي الحصول على الدكتوراه في تخصصي من هواياتي: كتابه الخط العربي وعشقي مايسمى [بالتصوير"
    q = '你好啊'
    t = Translator()
    r = t.translate(q)
    print r.text, r.src
    
