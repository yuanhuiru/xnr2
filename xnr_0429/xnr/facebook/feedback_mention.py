#!/usr/bin/env python
#encoding: utf-8

from launcher import Launcher
import time
from Elasticsearch_fb import Es_fb
import re

class Mention():
	def __init__(self, username, password):
		self.launcher = Launcher(username, password)
		self.mention_list,self.driver = self.launcher.get_mention_list()
		self.es = Es_fb()
		self.list = []
		self.update_time = int(time.time())

	def date2timestamp(self, date):
		date = date.replace(u'月', '-').replace(u'日', '').replace(' ', '')
		if date == '刚刚':
			timestamp = int(time.time())
			return timestamp
		if u'上午' in date:
			date = date.replace(u'上午', ' ')
		if u'下午' in date:
			if date.split(u'下午')[1].split(':')[0] == '12':
				date = date.replace(u'下午', ' ')
			elif eval(date.split(u'下午')[1].split(':')[0]) < 12:
				date = date.split(u'下午')[0] + ' ' + str(eval(date.split(u'下午')[1].split(':')[0])+12) + ':' + date.split(u'下午')[1].split(':')[1]
		if u'年' not in date and u'分钟' not in date and u'小时' not in date: 
			date = str(time.strftime('%Y-%m-%d', time.localtime(time.time())).split('-')[0]) + '-' + date 
		if u'年' in date and u'分钟' not in date and u'小时' not in date: 
			date = date.replace(u'年', '-') 
		if u'分钟' in date: 
			timestamp = int(time.time()) - int(re.search(r'(\d+)', date).group(1)) * 60 
			return timestamp 
		if u'小时' in date: 
			timestamp = int(time.time()) - int(re.search(r'(\d+)', date).group(1)) * 60 * 60 
			return timestamp
		try: 
			timestamp = int(time.mktime(time.strptime(date, '%Y-%m-%d'))) 
		except: 
			timestamp = int(time.mktime(time.strptime(date, '%Y-%m-%d %H:%M'))) 
		return timestamp	


	def get_mention(self):

		for url in self.mention_list:
			self.driver.get(url)
			time.sleep(1)

			try:
				nick_name = self.driver.find_element_by_xpath('//div[@id="root"]/div[1]/div[1]/div/div[1]/div[1]/table/tbody/tr/td[2]/div/h3/strong/a').text
			except:
				nick_name = ''
			print nick_name

			try:
				uid = re.findall(r'id=(\d+)', self.driver.find_element_by_xpath('//div[@id="root"]/div[1]/div[1]/div/div[1]/div[1]/table/tbody/tr/td[2]/div/h3/strong/a').get_attribute('href'))[0]
			except:
				uid = ''
			print uid

			try:
				timestamp = self.date2timestamp(self.driver.find_element_by_xpath('//div[@id="root"]/div[1]/div[1]/div/div[2]/div/abbr').text)
			except:
				timestamp = 0
			print timestamp

			try:
				text = self.driver.find_element_by_xpath('//div[@id="root"]/div[1]/div[1]/div/div[1]/div[2]').text
			except:
				text = ''
			print text

			try:
				mid = ''.join(re.findall(re.compile('fbid%3D(\d+)'),url))
			except:
				mid = ''
			print mid

			item = {'uid':uid, 'nick_name':nick_name, 'mid':mid, 'timestamp':timestamp, 'text':text, 'update_time':self.update_time}
			self.list.append(item)

		for i in self.list:
			self.driver.get('https://m.facebook.com/profile.php?id=' + str(i['uid']))
			try:
				photo_url = self.driver.find_element_by_xpath('//div[@id="m-timeline-cover-section"]/div[1]/div[2]/div[1]/div/a/img').get_attribute('src')
			except:
				try:
					photo_url = self.driver.find_element_by_xpath('//div[@id="m-timeline-cover-section"]/div[2]/div/div[1]/div[1]/a/img').get_attribute('src')
				except:
					photo_url = self.driver.find_element_by_xpath('//div[@id="m-timeline-cover-section"]/div[2]/div/div[1]/a/img').get_attribute('src')
			i['photo_url'] = photo_url

		self.driver.quit()
		return self.list

	def save(self, indexName, typeName, mention_list):
		self.es.executeES(indexName, typeName, mention_list)

if __name__ == '__main__':
	mention = Mention('8613520874771','13018119931126731x')
	mention_list = mention.get_mention()
	print(mention_list)

