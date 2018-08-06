#!/usr/bin/env python
#encoding: utf-8

from launcher import Launcher
import time
from Elasticsearch_fb import Es_fb
import re
import json

class Friend():
	def __init__(self, username, password):
		self.launcher = Launcher(username, password)
		self.driver = self.launcher.login_mobile()
		time.sleep(2)
		self.driver.get('https://m.facebook.com/friends/center/mbasic')
		time.sleep(3)
		#加载更多
		if self.driver.find_element_by_xpath('//div[@id="friends_center_main"]/div[2]/a'):
			self.driver.find_element_by_xpath('//div[@id="friends_center_main"]/div[2]/a').click()
		else:
			pass

		self.es = Es_fb()
		self.friends_list = []
		self.current_ts = int(time.time())
		self.update_time = self.current_ts

	def get_friend(self):
		if self.driver.find_element_by_xpath('//div[@id="friends_center_main"]/h3[1]').text == u'加好友请求':
			for each in self.driver.find_elements_by_xpath('//div[@id="friends_center_main"]/div[1]/div'):
				item = {}
				try:
					pic_url = each.find_element_by_xpath('./table/tbody/tr/td[1]/img').get_attribute('src')
					name = each.find_element_by_xpath('./table/tbody/tr/td[2]/a').text
					user_id = ''.join(re.findall(re.compile('uid=(\d+)'),each.find_element_by_xpath('./table/tbody/tr/td[2]/a').get_attribute('href')))
					profile_url = profile_url = 'https://m.facebook.com/profile.php?id=' + str(user_id)
				except:
					pass
				item['uid'] = user_id
				item['photo_url'] = pic_url
				item['nick_name'] = name
				item['profile_url'] = profile_url
				self.friends_list.append(item)
			for i in self.friends_list:
				self.driver.get(i['profile_url'])
				try:
					self.driver.find_element_by_xpath('//div[@id="m-timeline-cover-section"]/div[4]/a[2]').click()
				except:
					self.driver.find_element_by_xpath('//div[@id="root"]/div[1]/div[1]/div[4]/a[2]').click()
				time.sleep(2)
				try:
					friends = int(re.search(r'(\d+)', self.driver.find_element_by_xpath('//div[@id="root"]/div[1]/h3').text.replace(',', '').replace(' ', '')).group(1))
				except:
					friends = 0
				i['friends'] = friends
				i['update_time'] = self.update_time
		else:
			self.friends_list = []
		self.driver.quit()
		return self.friends_list

	def save(self, indexName, typeName, list):
		self.es.executeES(indexName, typeName, list)

if __name__ == '__main__':
	# friend = Friend('8618348831412','Z1290605918')
	friend = Friend('8613520874771','13018119931126731x')
	# friend = Friend('+8613269704912', 'chenhuiping')
	list = friend.get_friend()
	print(list)
	#friend.save('facebook_feedback_friends','text',list)

