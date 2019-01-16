#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2017-11-27 12:20:11
# @Author  : guangqiang_xu (981886190@qq.com)
# @Link    : http://www.treenewbee.com/
# @Version : $Id$
import random
import time
import requests
from scrapy.selector import Selector


# 爬取西刺代理的免费代理
def crawl_ips():
	headers = {
        "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'}
	ip_list = []
	for i in range(1, 11):
		time.sleep(3)
		print("正在爬取{0}页.".format(i))
		time.sleep(2)
		res = requests.get("http://www.xicidaili.com/nn/{0}".format(i), headers=headers, timeout=30)
		selector = Selector(text=res.text)
		all_trs = selector.css("#ip_list tr")

		for tr in all_trs[1:]:
			ip_item = {}
			speed_str = tr.css(".bar::attr(title)").extract()[0]
			if speed_str:
				speed = float(speed_str.split(u"秒")[0])
			else:
				speed = 0
			all_texts = tr.css("td::text").extract()
			ip = all_texts[0]
			port = all_texts[1]
			xici_type = all_texts[5]
			ip_item['ip'] = ip
			ip_item['port'] = port
			ip_item['speed'] = speed
			ip_item['xici_type'] = xici_type
			ip_list.append(ip_item)
	return ip_list


class GetIP(object):

	def __init__(self, ip_list):
		self.ip_list = ip_list

	# 判断ip是否可用
	def judeg_ip(self, ip, port, xici_type, speed):
		http_url = 'http://www.baidu.com'
		proxy_type = xici_type.lower()
		proxy_url = '{0}://{1}:{2}'.format(proxy_type, ip, port)

		try:
			proxy_dict = {
				proxy_type: proxy_url
			}
			print proxy_dict
			response = requests.get(http_url, proxies=proxy_dict, timeout=5)
		except Exception as e:
			print("invaild ip and port")
			return False
		else:
			code = response.status_code
			if code >= 200 and code < 300:
				print("effctive ip")
				return True
			else:
				print("invaild ip and port")
				return False

	# 从数据库中随机获取一个ip
	def get_random_ip(self):
		index = random.randrange(0, len(self.ip_list)-1)
		ip_item = self.ip_list[index]

		ip = ip_item['ip']
		port = ip_item['port']
		speed = ip_item['speed']
		xici_type = ip_item['xici_type']
		result = self.judeg_ip(ip, port, xici_type, speed)
		if result:
			proxy_type = xici_type.lower()
			return {"{}".format(proxy_type): "{0}:{1}".format(ip, port)}
		else:
			del self.ip_list[index]
			return self.get_random_ip()


if __name__ == '__main__':
    ip_list = crawl_ips()
    print ip_list
    get_ip = GetIP(ip_list)
    print get_ip.get_random_ip()

