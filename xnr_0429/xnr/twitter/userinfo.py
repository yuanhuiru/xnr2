#!/usr/bin/env python
#encoding: utf-8

from launcher import Launcher
from Elasticsearch_tw import Es_twitter
import time
import re

class Userinfo():
	def __init__(self, username, password, consumer_key, consumer_secret, access_token, access_secret):
		self.launcher = Launcher(username, password, consumer_key, consumer_secret, access_token, access_secret)

	def getUserinfo(self):
		#driver = self.launcher.login_mobile()
		#time.sleep(2)
		#driver.find_element_by_xpath('//nav[@aria-label="Primary"]/a[@aria-label="Home timeline"]/div/svg/g/path').click()
		#time.sleep(2)
		#screen_name_url = driver.find_element_by_xpath('//div[@class="DashboardProfileCard-content"]//a').get_attribute('href')
		#driver.get(screen_name_url)
		api = self.launcher.api()
		user = api.me()
		#time.sleep(1)
		#screen_name = driver.find_element_by_xpath('//div[@class="ProfileHeaderCard"]/h2//b').text
		uid = user.id_str
		screen_name = user.screen_name
		#uid = api.get_user(screen_name).id
		try:
			description = user.description
		except:
			description = ''
		try:
			location = user.location
		except:
			location = ''
		#today = int(time.strftime('%Y',time.localtime(int(time.time()))))
		#try:
		#	birth = driver.find_element_by_xpath('//span[@class="ProfileHeaderCard-birthdateText u-dir"]/span').text
		#except:
		#	birth = False
		#pattern = re.compile(u'(\d+)年')
		#pattern1 = re.compile(', (\d+)')
		#if birth:
		#	try:
		#		birthday = int(re.findall(pattern,birth)[0])
		#	except:
		#		birthday = int(re.findall(pattern1,birth)[0])
		#	age = today - birthday
		#else:
		#	age = None
		dict = {'uid':uid,'desccription':description,'location':location}
		return dict
		
if __name__ == '__main__':
	#userinfo = Userinfo('8617078448226','xnr123456')
	userinfo = Userinfo('feifanhanmc@163.com', 'han8528520258', 'N1Z4pYYHqwcy9JI0N8quoxIc1', 'VKzMcdUEq74K7nugSSuZBHMWt8dzQqSLNcmDmpGXGdkH6rt7j2', '943290911039029250-yWtATgV0BLE6E42PknyCH5lQLB7i4lr', 'KqNwtbK79hK95l4X37z9tIswNZSr6HKMSchEsPZ8eMxA9')
	print userinfo.getUserinfo()
