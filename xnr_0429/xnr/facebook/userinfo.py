#!/usr/bin/env python
#encoding: utf-8

from launcher import Launcher
import time
import re

class Userinfo:
	def __init__(self, username, password):
		self.launcher = Launcher(username, password)

	def getUserinfo(self):
		driver = self.launcher.login_mobile()
		time.sleep(10)
		html = driver.page_source
		with open("Userinfo.html", "wb") as f:
			f.write(html)
		
		driver.find_element_by_xpath('//div[@role="navigation"]/a[2]').click()
		time.sleep(1)

		current_url = driver.page_source
		with open('Userinfo111.html', 'wb') as f:
			f.write(current_url)
		#driver.find_element_by_xpath('//a[@data-tab-key="about"]').click()
		try:
			driver.find_element_by_xpath('//div[@id="m-timeline-cover-section"]/div[3]/a[1]').click()
		except:
			pass
		html = driver.page_source
		with open("Userinfo222.html", "wb") as f:
			f.write(html)
		time.sleep(1)
		#current_url = driver.page_source
		pattern0 = re.compile(';id=(\d+)&')
		id = re.findall(pattern0, current_url)[0]
		print id
		#eachs = [each for each in driver.find_elements_by_xpath('//div[@data-pnref="overview"]//li')]
		#career = eachs[0].text
		#location = eachs[2].text
		#pattern = re.compile(u'(\d+)年')
		#print(eachs[5].text)
		#birth = int(re.findall(pattern,eachs[5].text)[0])
		career = driver.find_element_by_xpath('//div[@id="work"]/div/div[2]/div/div[2]/a/span').text
		if u'添加' in career:
			career = None
		print career
		try:
			location = driver.find_element_by_xpath('//div[@id="living"]/div/div[2]/div[1]/div[@title="所在地"]/table/tbody/tr/td[2]/div/a').text
		except:
			location = None
		print location
		birth = int(driver.find_element_by_xpath('//div[@id="basic-info"]/div/div[2]/div[@title="生日"]/table/tbody/tr/td[2]/div').text.replace(u'年', '-').replace(u'月', '-').replace(u'日', '').split('-')[0].strip())
		print birth
		today = int(time.strftime('%Y',time.localtime(int(time.time()))))
		age = today - birth
		print age
		discription = driver.find_element_by_xpath('//div[@id="bio"]/div/div[2]/div/a/span').text
		if u'介绍一下自己吧' in discription:
			discription = None
		#driver.find_element_by_xpath('//ul[@data-testid="info_section_left_nav"]/li[6]/a').click()
		time.sleep(1)
		#description = driver.find_element_by_xpath('//div[@id="pagelet_bio"]/div/ul/li').text

		dict = {'id':id,'career':career,'location':location,'age':age, 'description':discription}
		driver.quit()
		return dict

if __name__ == '__main__':
	userinfo = Userinfo('13520874771','13018119931126731x')
	#userinfo = Userinfo('+8613269704912','chenhuiping')
	print userinfo.getUserinfo()
