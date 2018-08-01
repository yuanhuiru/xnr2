#!/usr/bin/env python
#encoding: utf-8

from launcher import Launcher
import time
from Elasticsearch_fb import Es_fb
import re

class Like():
	def __init__(self, username, password):
		self.launcher = Launcher(username, password)
		self.like_urls_list,self.driver = self.launcher.get_like_list()
		self.es = Es_fb()
		self.like_list = []
		self.update_time = int(time.time())

	def date2timestamp(self, date):
		date = date.replace(u'月', '-').replace(u'日', '').replace(' ', '')
		if u'上午' in date:
			date = date.split(u'上午')[0]
		if u'下午' in date:
			date = date.split(u'下午')[0] 
		if u'分钟' in date: 
			timestamp = int(time.time()) - int(re.search(r'(\d+)', date).group(1)) * 60 
			return timestamp 
		if u'小时' in date: 
			timestamp = int(time.time()) - int(re.search(r'(\d+)', date).group(1)) * 60 * 60 
			return timestamp 
		if u'年' not in date and u'分钟' not in date and u'小时' not in date: 
			date = str(time.strftime('%Y-%m-%d', time.localtime(time.time())).split('-')[0]) + '-' + date 
		if u'年' in date and u'分钟' not in date and u'小时' not in date: 
			date = date.replace(u'年', '-') 
		timestamp = int(time.mktime(time.strptime(date, '%Y-%m-%d'))) 
		return timestamp

	def get_like(self):
		for url in self.like_urls_list:
			self.driver.get(url)
			time.sleep(1)

			try:
				root_text = self.driver.find_element_by_xpath('//div[@id="m_story_permalink_view"]/div[1]/div/div[1]/div[2]').text
			except:
				root_text = 'None'
			print root_text


			try:
				timestamp = self.date2timestamp(self.driver.find_element_by_xpath('//div[@id="m_story_permalink_view"]/div[1]/div/div[2]/div[1]').text)
			except:
				timestamp = 0
			print timestamp

			try:
				root_mid = ''.join(re.search(re.compile('fbid%3D(\d+)%'), url).group(1))
			except:
				root_mid = 0
			print root_mid

			# 进入点赞列表页
			self.driver.get(self.driver.find_element_by_xpath('//div[@id="m_story_permalink_view"]/div[2]/div/div[3]/a').get_attribute('href'))
			time.sleep(5)

			for each in self.driver.find_elements_by_xpath('//div[@id="root"]/table/tbody/tr/td/div/ul/li'):
				try:
					author_name = each.find_element_by_xpath('./table/tbody/tr/td/table/tbody/tr/td[3]/div/h3[1]/a').text
				except:
					author_name = 'None'
				print author_name

				try:
					author_id = ''.join(re.findall(re.compile('id=(\d+)'),each.find_element_by_xpath('./table/tbody/tr/td/table/tbody/tr/td[3]/div/h3[1]/a').get_attribute('href')))
				except:
					author_id = 0
				try:
					pic_url = each.find_element_by_xpath('./table/tbody/tr/td/table/tbody/tr/td[1]/img').get_attribute('src')
				except:
					pic_url = 'None'

				item = {'uid':author_id, 'photo_url':pic_url, 'nick_name':author_name, 'timestamp':timestamp, 'root_text':root_text, 'update_time':self.update_time, 'root_mid':root_mid}
				self.like_list.append(item)

		self.driver.quit()
		return self.like_list

	def save(self, indexName, typeName, list):
		self.es.executeES(indexName, typeName, list)

if __name__ == '__main__':
	like = Like('8613520874771','13018119931126731x')
	list = like.get_like()
	print list

