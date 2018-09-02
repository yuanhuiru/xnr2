#-*- coding: utf-8 -*-
import httplib
import md5
import urllib
import random
import json

class Resutls():
    def __init__(self, text, src):
        self.text = text
        self.src = src
    
class Translator():
    def __init__(self):
        self.appid = '20170921000084362'
        self.secretKey = 'uKPwOwSfMDG4Byrq1ey7'
    
    def translate(self, q, target_language='zh-cn'):
        if target_language == 'zh-cn':
            target_lang = 'zh'
        elif target_language == 'en':
            target_lang = 'en'
        q = [item.encode('utf8') for item in q]
        q = '\n'.join(q)
        fromLang = 'auto'
        toLang = target_lang
        
        httpClient = None
        myurl = '/api/trans/vip/translate'


        salt = random.randint(32768, 65536)
        sign = self.appid+q+str(salt)+self.secretKey
        m1 = md5.new()
        m1.update(sign)
        sign = m1.hexdigest()
        myurl = myurl+'?appid='+self.appid+'&q='+urllib.quote(q)+'&from='+fromLang+'&to='+toLang+'&salt='+str(salt)+'&sign='+sign
        
        try:
            httpClient = httplib.HTTPConnection('api.fanyi.baidu.com')
            httpClient.request('GET', myurl)
            #response是HTTPResponse对象
            response = httpClient.getresponse()
            trans_res = json.loads(response.read())
        except Exception, e:
            print e
            trans_res = []
        finally:
            if httpClient:
                httpClient.close()
        
        res = []
        src = 'en'
        if trans_res:
            try:
                src = trans_res.get('from', 'en')
                for r in  trans_res['trans_result']:
                    res.append(Resutls(r['dst'], src))
            except Exception,e:
                print e
        if len(res) == 1:
            return res[0]
        return res
if __name__ == '__main__':                
	res = Translator().translate(q=['apple'])
	print res.text, res.src


