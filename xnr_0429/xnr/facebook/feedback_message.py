#!/usr/bin/env python
#encoding: utf-8

from launcher import Launcher
import time
from Elasticsearch_fb import Es_fb
import re

class Message():
	def __init__(self,username, password):
		self.launcher = Launcher(username, password)
		self.driver = self.launcher.login_mobile()
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

	def get_list(self):
		self.driver.get('https://m.facebook.com/messages/t/')

		sx_list = []
		for each in self.driver.find_elements_by_xpath('//div[@id="root"]/div[1]/div[2]/div/table'):
			try:
				author_name = each.find_element_by_xpath('./tbody/tr/td/div/h3[1]').text
			except:
				author_name = 'None'
			print author_name

			try:
				author_id = ''.join(re.findall(re.compile('%3A(\d+)#'),each.find_element_by_xpath('./tbody/tr/td/div/h3[1]/a').get_attribute('href')))
			except:
				author_id = 'None'
			print author_id

			try:
				message_url = each.find_element_by_xpath('./tbody/tr/td/div/h3[1]/a').get_attribute('href')
			except:
				message_url = False
			print message_url

			if message_url:
				sx_list.append({'author_name':author_name, 'message_url':message_url, 'author_id':author_id})
		return sx_list

	def get_message(self):
		sx_list = self.get_list()
		for sx in sx_list:
			self.driver.get('https://m.facebook.com/profile.php?id=' + str(sx['author_id']))
			try:
				photo_url = self.driver.find_element_by_xpath('//div[@id="m-timeline-cover-section"]/div[1]/div[2]/div[1]/div/a/img').get_attribute('src')
			except:
				try:
					photo_url = self.driver.find_element_by_xpath('//div[@id="m-timeline-cover-section"]/div[2]/div/div[1]/div[1]/a/img').get_attribute('src')
				except:
					photo_url = self.driver.find_element_by_xpath('//div[@id="m-timeline-cover-section"]/div[2]/div/div[1]/a/img')
			sx['photo_url'] = photo_url


		for sx in sx_list:
			self.driver.get(sx['message_url'])
			time.sleep(1)

			for message in self.driver.find_elements_by_xpath('//div[@id="messageGroup"]/div/div'):
				try:
					date = message.find_element_by_xpath('./div[2]/abbr').text
				except:
					break
				print date

				try:
					messageTime = self.date2timestamp(date)					
				except:
					messageTime = 0
				print messageTime

				try:
					#messageId = re.findall(re.compile('"fbid:(\d+)"'),message.find_element_by_xpath('./div/div').get_attribute('participants'))[-1]
					#if messageId == sx['author_id']:
					#	private_type = 'receive'
					#	text = message.text
					#	root_text = 'None'
					#else:
					#	private_type = 'make'
					#	text = 'None'
					#	root_text = message.text
					if re.findall(r'id=(\d+)&', message.find_element_by_xpath('./div[1]/a').get_attribute('href')):
						private_type = 'receive'
						text = message.text
						root_text = ''
					else:
						private_type = 'make'
						text = ''
						root_text = message.text
				except:
					private_type = 'unknown'
					text = message.text
					root_text = ''
			self.list.append({'uid':sx['author_id'], 'photo_url':sx['photo_url'], 'nick_name':sx['author_name'], 'timestamp':messageTime, 'update_time':self.update_time, 'text':text, 'root_text':root_text, 'private_type':private_type})

		self.driver.quit()
		return self.list

	def save(self, indexName, typeName, list):
		self.es.executeES(indexName, typeName, list)

if __name__ == '__main__':
	message = Message('8613520874771','13018119931126731x')
	list = message.get_message()
	print list
	# message.save('facebook_feedback_private','text',list)




