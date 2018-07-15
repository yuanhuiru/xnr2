#!/usr/bin/env python
# encoding: utf-8

import requests
from retry import retry
from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import time
import json
from lxml import etree
import re
from pybloom import BloomFilter
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import *
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from launcher import Launcher

class Operation():
	def __init__(self, username, password):
		print '11'
		self.launcher = Launcher(username, password)
		print '22'
		self.username = username
		self.password = password

	
	def publish(self, text):
		driver = self.launcher.login_mobile()
		time.sleep(20)
		html = driver.page_source
		with open("publish1.html", "wb") as f:
			f.write(html)
		driver.save_screenshot("publish1.png")
		driver.find_element_by_xpath('//input[@type="submit"]').click()
		print "publish111111"
		html = driver.page_source
		with open('publish.html', 'wb') as f:
			f.write(html)
		driver.save_screenshot('publish.png')
		driver.find_element_by_xpath('//textarea[@name="xc_message"]').send_keys(text)
		print "publish222222"
		driver.find_element_by_xpath('//input[@name="view_post"]').click()
		print "publish333333"

		time.sleep(60)

		html = driver.page_source
		with open("publish2.html", "wb") as f:
			f.write(html)
		driver.save_screenshot("publish.png")
		driver.quit()

		print "publish Success!!!!!!"

		return [True, '']


	def mention(self, username, text):
		driver = self.launcher.login()
		#driver,display = self.launcher.login()
		try:
			# 退出通知弹窗进入页面
			time.sleep(1)
			try:
				#driver.find_element_by_xpath('//div[@class="_n8 _3qx uiLayer _3qw"]').click()
				driver.find_element_by_xpath('//div[@class="_1k67 _cy7"]').click()
				print "mention Success11111111111111111111111"
			except BaseException, e:
				print "mention Position11111111111111111", e
				pass

			try:
				driver.find_element_by_xpath('//textarea[@title="分享新鲜事"]').click()
				driver.find_element_by_xpath('//textarea[@title="分享新鲜事"]').send_keys(text)
			except:
				driver.find_element_by_xpath('//div[@class="_1mwp navigationFocus _395 _1mwq _4c_p _5bu_ _34nd _21mu _5yk1"]').click()
				driver.find_element_by_xpath('//div[@class="_1mwp navigationFocus _395 _1mwq _4c_p _5bu_ _34nd _21mu _5yk1"]').send_keys(text)
			time.sleep(2)
			try:
				driver.find_element_by_xpath('//table[@class="uiGrid _51mz"]/tbody/tr[1]/td[1]//a/div').click()
			except:
				driver.find_element_by_xpath('//table[@class="uiGrid _51mz _5f0n"]/tbody/tr[2]/td[2]//a/div').click()
			time.sleep(1)
			driver.find_element_by_xpath('//input[@aria-label="你和谁一起？"]').send_keys(username)
			driver.find_element_by_xpath('//input[@aria-label="你和谁一起？"]').send_keys(Keys.ENTER)
			time.sleep(1)
			try:
				driver.find_element_by_xpath('//button[@class="_1mf7 _4jy0 _4jy3 _4jy1 _51sy selected _42ft"]').click()
			except:
				driver.find_element_by_xpath('//button[@data-testid="react-composer-post-button"]').click()
			time.sleep(5)
			return [True, '']
		except Exception as e:
			return [False, e]
		finally:
			driver.quit()
			#display.popen.kill()

	def follow(self, uid):
		try:
			#driver,display = self.launcher.target_page(uid)
			driver = self.launcher.target_page(uid)
			# 退出通知弹窗进入页面
			time.sleep(1)
			try:
				driver.find_element_by_xpath('//div[@class="_n8 _3qx uiLayer _3qw"]').click()
			except:
				pass
			try:
				driver.find_element_by_xpath('//button[@data-testid="page_profile_follow_button_test_id"]').click()
			except:
				driver.find_element_by_xpath('//div[@id="pagelet_timeline_profile_actions"]/div[2]/a[1]').click()
			time.sleep(5)
			driver.quit()
			return [True, '']
		except Exception as e:
			return [False, e]
		#finally:
			#driver.quit()
			#display.popen.kill()

	def not_follow(self, uid):
		try:
			#driver,display = self.launcher.target_page(uid)
			driver = self.launcher.target_page(uid)
			# 退出通知弹窗进入页面
			time.sleep(1)
			try:
				driver.find_element_by_xpath('//div[@class="_n8 _3qx uiLayer _3qw"]').click()
			except:
				pass
			chain = ActionChains(driver)
			try:
				implement = driver.find_element_by_xpath('//div[@id="pagelet_timeline_profile_actions"]/div[2]/div[1]/div[1]')
				chain.move_to_element(implement).perform()
				time.sleep(2)
				implement = driver.find_element_by_xpath('//div[@id="pagelet_timeline_profile_actions"]/div[2]/div[1]/div[1]')
				chain.move_to_element(implement).perform()
				time.sleep(2)
				driver.find_element_by_xpath('//a[@ajaxify="/ajax/follow/unfollow_profile.php?profile_id=%s&location=1"]'%uid).click()
			except:
				try:
					implement = driver.find_element_by_xpath('//button[@data-testid="page_profile_follow_button_test_id"]')
					chain.move_to_element(implement).perform()
					time.sleep(2)
					implement = driver.find_element_by_xpath('//button[@data-testid="page_profile_follow_button_test_id"]')
					chain.move_to_element(implement).perform()
					time.sleep(2)
					driver.find_element_by_xpath('//a[@ajaxify="/ajax/follow/unfollow_profile.php?profile_id=%s&location=1"]'%uid).click()
				except:
					implement = driver.find_element_by_xpath('//button[@class="_42ft _4jy0 _3oz- _52-0 _4jy4 _517h _51sy"]')
					chain.move_to_element(implement).perform()
					time.sleep(2)
					implement = driver.find_element_by_xpath('//button[@class="_42ft _4jy0 _3oz- _52-0 _4jy4 _517h _51sy"]')
					chain.move_to_element(implement).perform()
					time.sleep(2)
					driver.find_element_by_xpath('//a[@ajaxify="/ajax/follow/unfollow_profile.php?profile_id=%s&location=1"]'%uid).click()
			time.sleep(5)
			driver.quit()
			return [True, '']
		except Exception as e:
			return [False, e]
		#finally:
			#driver.quit()
			#display.popen.kill()

# 私信
	def send_message(self, uid, text):
		#发送给未关注的用户
		driver = self.launcher.login_mobile()
		message_url = 'https://m.facebook.com/messages/thread/' + uid + "/"
		driver.get(message_url)
		time.sleep(10)
		driver.save_screenshot('send_message.png')
		html = driver.page_source
		with open('send_message.html', 'wb') as f:
			f.write(html)
		try:
			driver.find_element_by_xpath('//textarea[@id="composerInput"]').send_keys(text)
		except:
			driver.find_element_by_xpath('//textarea[@name="body"]').send_keys(text)
		time.sleep(10)
		html = driver.page_source
		with open("send.html", "wb") as f:
			f.write(html)
		try:
			driver.find_element_by_xpath('//input[@name="send"]').click()
		except:
			driver.find_element_by_xpath('//input[@name="Send"]').click()
		print "send_message Success!!!"
		driver.quit()
		return [True, '']


# 私信(已关注)
	# def send_message2(self, uid, text):
	# 	#发送给已关注的用户
	# 	try:
	# 		driver = self.launcher.target_page(uid)
	# 		url = driver.find_element_by_xpath('//a[@class="_51xa _2yfv _3y89"]/a[1]').get_attribute('href')
	# 		driver.get('https://www.facebook.com' + url)
	# 		time.sleep(4)
	# 		driver.find_element_by_xpath('//div[@class="_1mf _1mj"]').click()
	# 		driver.find_element_by_xpath('//div[@class="_1mf _1mj"]').send_keys(text)
	# 		driver.find_element_by_xpath('//div[@class="_1mf _1mj"]').send_keys(Keys.ENTER)
	# 	finally:
	# 		driver.quit()


# 点赞
	def like(self, uid, fid):
		driver = self.launcher.login_mobile()
		#driver,display = self.launcher.login()
		#try:
		#post_url = 'https://m.facebook.com/' + uid + '/posts/' + fid
		post_url = 'https://m.facebook.com/story.php?story_fbid=' + fid + '&id=' + uid
		#	video_url = 'https://m.facebook.com/' + uid + '/videos/' + fid
		driver.get(post_url)
		time.sleep(10)
		html = driver.page_source
		with open('like.html', 'wb') as f:
			f.write(html)
		#driver.find_element_by_xpath('//div[@class="cb cc"]/div[@class="cd ce cf cg"]/table[@role="presentation"]/tbody/tr/td[1]/a').click()
		driver.find_element_by_xpath('//div[@id="m_story_permalink_view"]/div[2]/div/div/table[@role="presentation"]/tbody/tr/td[1]/a').click()
		driver.quit()
		print "like Success!!!!!!"
		return [True, '']
			#try:
				# 退出通知弹窗进入页面
		#time.sleep(1)
		#		try:
		#			driver.find_element_by_xpath('//div[@class="_n8 _3qx uiLayer _3qw"]').click()
		#		except:
		#			pass
		#		driver.find_element_by_xpath('//div[@aria-label="Facebook 照片剧场模式"]')
		#		driver.get(video_url)
		#		time.sleep(2)
				# 退出通知弹窗进入页面
		#		time.sleep(1)
		#		try:
		#			driver.find_element_by_xpath('//div[@class="_n8 _3qx uiLayer _3qw"]').click()
		#		except:
		#			pass
		#for each in driver.find_elements_by_xpath('//a[@data-testid="fb-ufi-likelink"]'):
					#try:
		#	each.click()
					#except:
					#	pass
			#except:
				# 退出通知弹窗进入页面
			#	time.sleep(1)
			#	try:
			#		driver.find_element_by_xpath('//div[@class="_n8 _3qx uiLayer _3qw"]').click()
			#	except:
			#		pass
		#		for each in driver.find_elements_by_xpath('//a[@data-testid="fb-ufi-likelink"]'):
		#			try:
		#				each.click()
		#			except:
		#				pass
		#	time.sleep(5)
		#	return [True, '']
		#except Exception as e:
		#	return [False, e]
		#finally:
		#	driver.quit()
			#display.popen.kill()

# 评论
	def comment(self, uid, fid, text):
		driver = self.launcher.login_mobile()
		#driver,display = self.launcher.login()
		#try:
		post_url = 'https://m.facebook.com/' + uid + '/posts/' + fid
		#post_url = 'https://m.facebook.com/story.php?story_fbid=' + fid + '&id=' + uid
		#video_url = 'https://m.facebook.com/' + uid + '/videos/' + fid
		print post_url
		driver.get(post_url)
		#wait = WebDriverWait(driver, timeout=600)
		time.sleep(5)
			#try:
				# 退出通知弹窗进入页面
		#time.sleep(1)
				#try:
					#driver.find_element_by_xpath('//div[@class="_n8 _3qx uiLayer _3qw"]').click()
				#except:
					#pass
		#driver.find_element_by_xpath('//div[@aria-label="Facebook 照片剧场模式"]')
		#driver.get(post_url)
				# 退出通知弹窗进入页面
		#time.sleep(1)
				#try:
				#	driver.find_element_by_xpath('//div[@class="_n8 _3qx uiLayer _3qw"]').click()
				#except:
				#	pass
		#time.sleep(30)
		html = driver.page_source
		with open("comment.html", "wb") as f:
			f.write(html)
		driver.find_element_by_xpath('//textarea[@name="comment_text"]').send_keys(text)
		time.sleep(5)
		driver.find_element_by_xpath('//form/table/tbody/tr/td[2]/div/input[@type="submit"]').click()
		#driver.save_screenshot("test.png")
		#html = driver.page_source
		#with open('test.html', 'wb') as f:
		#	f.write(html)
		#textbox = wait.until(
		#	EC.presence_of_element_located((By.XPATH, '//div[@class="UFICommentContainer"]/div/div'))
		#)
		#textbox.click()
		time.sleep(10)
		#textbox.send_keys(text)		
		#driver.find_element_by_xpath('//div[@class="UFICommentContainer"]/div/div').click()
		#time.sleep(30)
		#textbox.send_keys(Keys.ENTER)
				#driver.find_element_by_xpath('//div[@class="notranslate _5rpu"]').click()
		#time.sleep(1)
				#driver.find_element_by_xpath('//div[@class="notranslate _5rpu"]').send_keys(text)
		#driver.find_element_by_xpath('//div[@class="UFICommentContainer"]/div/div').send_keys(text)
		#time.sleep(1)
				#driver.find_element_by_xpath('//div[@class="notranslate _5rpu"]').send_keys(Keys.ENTER)
		#driver.find_element_by_xpath('//div[@class="UFICommentContainer"]/div/div').send_keys(Keys.ENTER)
			#except:
				# 退出通知弹窗进入页面
				#time.sleep(1)
				#try:
					#driver.find_element_by_xpath('//div[@class="_n8 _3qx uiLayer _3qw"]').click()
				#except:
					#pass
				#time.sleep(3)
				#driver.find_element_by_xpath('//div[@class="UFICommentContainer"]/div/div').click()
				#time.sleep(1)
				#driver.find_element_by_xpath('//div[@class="notranslate _5rpu"]').click()
				#time.sleep(1)
				#driver.find_element_by_xpath('//div[@class="notranslate _5rpu"]').send_keys(text)
				#time.sleep(1)
				#driver.find_element_by_xpath('//div[@class="notranslate _5rpu"]').send_keys(Keys.ENTER)
		#time.sleep(5)
		driver.quit()
		return [True, '']
		#except Exception as e:
			#print e
			#return [False, e]
		#finally:
		#time.sleep(3)
		#driver.quit()
		#display.popen.kill()

# 分享
	def share(self, uid, fid, text):
		driver = self.launcher.login_mobile()
		#driver,display = self.launcher.login()
		#try:
		print 'uid, fid, text...',uid, fid, text
		#post_url = 'https://m.facebook.com/' + uid + '/posts/' + fid
		post_url = 'https://m.facebook.com/story.php?story_fbid=' + fid + '&id=' + uid
		#video_url = 'https://m.facebook.com/' + uid + '/videos/' + fid
		driver.get(post_url)
		time.sleep(30)
		html = driver.page_source
		with open("test0.html", "wb") as f:
			f.write(html)
			#try:
				# 退出通知弹窗进入页面
		#time.sleep(1)
				#try:
		#driver.find_element_by_xpath('//div[@class="_n8 _3qx uiLayer _3qw"]').click()
				#except:
					#pass
		#driver.find_element_by_xpath('//div[@aria-label="Facebook 照片剧场模式"]')
		#driver.get(video_url)
		#time.sleep(1)
				# 退出通知弹窗进入页面
		#time.sleep(1)
				#try:
					#driver.find_element_by_xpath('//div[@class="_n8 _3qx uiLayer _3qw"]').click()
				#except:
					#pass

		driver.find_element_by_xpath('//table[@role="presentation"]/tbody/tr/td[4]').click()
				#driver.find_element_by_xpath('//a[@title="发送给好友或发布到你的时间线上。"]').click()
		html = driver.page_source
		with open("test.html", "wb") as f:
			f.write(html)
		#time.sleep(3)
		#driver.find_element_by_xpath('//ul[@class="_54nf"]/li[2]').click()
		time.sleep(3)
				#try:
		driver.find_element_by_xpath('//textarea[@name="xc_message"]').click()
		time.sleep(1)
		driver.find_element_by_xpath('//textarea[@name="xc_message"]').send_keys(text)
		time.sleep(5)
				#except:
					#driver.find_element_by_xpath('//div[@class="_1mwp navigationFocus _395  _21mu _5yk1"]/div').click()
					#time.sleep(1)
					#driver.find_element_by_xpath('//div[@class="_1mwp navigationFocus _395  _21mu _5yk1"]/div').send_keys(text)
					#time.sleep(1)
		driver.find_element_by_xpath('//input[@value="分享"]').click()
			#except:
				# 退出通知弹窗进入页面
				#time.sleep(1)
				#try:
					#driver.find_element_by_xpath('//div[@class="_n8 _3qx uiLayer _3qw"]').click()
				#except:
					#pass
				#driver.find_element_by_xpath('//a[@title="发送给好友或发布到你的时间线上。"]').click()
				#driver.find_element_by_xpath('//a[@title="发送给好友或发布到你的时间线上。"]').click()
				#time.sleep(5)
				#driver.find_element_by_xpath('//ul[@class="_54nf"]/li[2]').click()
				#time.sleep(5)
				#try:
					#driver.find_element_by_xpath('//div[@class="notranslate _5rpu"]').click()
					#time.sleep(1)
					#driver.find_element_by_xpath('//div[@class="notranslate _5rpu"]').send_keys(text)
					#time.sleep(1)
				#except:
					#driver.find_element_by_xpath('//div[@class="_1mwp navigationFocus _395  _21mu _5yk1"]/div').click()
					#time.sleep(1)
					#driver.find_element_by_xpath('//div[@class="_1mwp navigationFocus _395  _21mu _5yk1"]/div').send_keys(text)
					#time.sleep(1)
				#driver.find_element_by_xpath('//button[@data-testid="react_share_dialog_post_button"]').click()
		time.sleep(5)
		driver.quit()
		return [True, '']
		#except Exception as e:
			#return [False, e]
		#finally:
		#driver.quit()
		#display.popen.kill()

#添加好友
	def add_friend(self, uid):
		#try:
		driver = self.launcher.target_page(uid)
		html = driver.page_source
		with open("add_friend.html", "wb") as f:
			f.write(html)
			#driver,display = self.launcher.target_page(uid)
		driver.find_element_by_xpath('//table[@class="by"]/tbody/tr/td[1]/a').click()
		time.sleep(5)
		driver.quit()
		return [True, '']
		#except Exception as e:
		#	return [False, e]
		#finally:
			#driver.quit()
			#display.popen.kill()

#确认好友请求
	def confirm(self, uid):
		#try:
		driver = self.launcher.target_page(uid)
		#driver = self.launcher.login_mobile()
			#driver,display = self.launcher.target_page(uid)
		time.sleep(5)
		html = driver.page_source
		with open("confirm.html", "wb") as f:
			f.write(html)
		driver.find_element_by_xpath('//table[@class="by"]/tbody/tr/td[1]/a').click()
		time.sleep(1)
		html = driver.page_source
		with open("confirm1.html", "wb") as f:
			f.write(html)
		#driver.find_element_by_xpath('//li[@data-label="确认"]/a').click()
		time.sleep(5)
		driver.quit()
		return [True, '']
		#except Exception as e:
		#	return [False, e]
		#finally:
			#driver.quit()
			#display.popen.kill()

#删除好友
	def delete_friend(self, uid):
		#try:
		driver = self.launcher.target_page(uid)
			#driver,display = self.launcher.target_page(uid)
		time.sleep(1)
		driver.find_element_by_xpath('//table[@class="bw"]/tbody/tr/td[2]/a').click()
		time.sleep(2)
		driver.find_element_by_xpath('//ul[@class="bt bu"]/li[1]/a').click()
		time.sleep(5)
		driver.find_element_by_xpath('//input[@name="confirm"]').click()
		driver.quit()
		return [True, '']
		#except Exception as e:
		#	return [False, e]
		#finally:
			#driver.quit()
			#display.popen.kill()

if __name__ == '__main__':
	operation = Operation('13520874771','13018119931126731x')
	#operation = Operation('+8613269704912','chenhuiping')
	#operation = Operation('shishengren_1@163.com', 'sss_820828')
	print "登陆成功"
	# operation = Operation('13041233988','Hanmc0322*')
	# operation = Operation('8618348831412','Z1290605918')
	time.sleep(10)
	#list = operation.publish(u'2018年6月23日21点50分')
	#print(list)
	#print '发布成功！'
	#operation.mention('张静明','2018年6月16日')
	#print "发布成功！"
	#operation.not_follow('100023080760480')
	#operation.send_message('100023080760480', 'hi, nice to meet u')
	#print "私信成功！"
	#operation.like('183774741715570','1487409108018787')
	#operation.like('100012258524129', '418205591931388')
	#print "点赞成功！"
	#operation.comment('100012258524129','418205591931388','awesome！')
	#print "评论成功！"
	#operation.share('183774741715570','1487409108018787','emmmm')
	#print "分享成功！"
	#operation.add_friend('183774741715570')
	#operation.add_friend('100026978657712')
	#print "添加好友成功！"
	#operation.confirm('100026978657712')
	#print "确认成功！"
	operation.delete_friend('100026978657712')
	print "删除好友成功！"
