# -*-coding:utf-8-*-
import json
import sys
import os
sys.path.append('../')
from global_utils import R_OPERATE_QUEUE as r, operate_queue_name
from utils import add_operate2redis
from parameter import WEIBO_XNR_OPERATE_PATH,FACEBOOK_XNR_OPERATE_PATH,  TWITTER_XNR_OPERATE_PATH
# sys.path.append('../facebook_xnr_operate/')
sys.path.append(FACEBOOK_XNR_OPERATE_PATH)
from fb_op_utils import get_submit_tweet_fb, get_comment_operate_fb, get_retweet_operate_fb, get_at_operate_fb,\
				get_like_operate_fb,get_private_operate_fb, get_add_friends, get_confirm_friends, \
				get_delete_friend 


# sys.path.append('../twitter_xnr_operate/')
sys.path.append(TWITTER_XNR_OPERATE_PATH)
from tw_op_utils import get_submit_tweet_tw, get_comment_operate_tw, get_retweet_operate_tw, get_at_operate_tw,\
				get_like_operate_tw,get_private_operate_tw, get_follow_operate_tw, get_unfollow_operate_tw


sys.path.append(WEIBO_XNR_OPERATE_PATH)
# os.path.dirname('wb_op_utils.py')
# a = os.path.dirname(os.path.abspath('wb_op_utils.py'))
#a=os.path.abspath("../weibo_xnr_operate/")


from wb_op_utils import get_submit_tweet, get_reply_total, get_reply_comment, get_reply_retweet, get_reply_private, \
				get_reply_follow, get_reply_unfollow, get_like_operate

import traceback
# publish-发帖、retweet-转发、comment-评论、like-点赞、follow-关注、unfollow-取消关注、at-提到、private-私信
    # add-发送添加好友请求、confirm-确认好友请求、delete-删除好友请求
    # receive - 回复

def operate_out_of_redis():

	while True:
		print 1111
		while 1:
			count = 0
			try:
				print operate_queue_name
				temp = r.rpop(operate_queue_name)
				break
			except Exception, e:
				print e
				count += 1
				if count == 10:
					raise e
				else:
					continue
		print 2222
		print 'temp.',temp
		if not temp:
			break

		queue_dict = json.loads(temp)
		
		channel = queue_dict['channel']
		operate_type = queue_dict['operate_type']

		task_detail = queue_dict['content']


		if channel == 'facebook':
			while 1:
				try:
					if operate_type == 'publish':
						try:
							print 'task_detail..',task_detail
							print '==========================================22222222222222222222222222222222222222222222222222facebookredis',task_detail
							mark = get_submit_tweet_fb(task_detail)
						except Exception, e:
							#add_operate2redis(queue_dict)
							print 111111111111111, e
							print '111111111111111get submit_baocuo', e
							pass

					elif operate_type == 'retweet':
						try:
							mark = get_retweet_operate_fb(task_detail)
						except Exception,e:
							print 222222222222222, e
							# add_operate2redis(queue_dict)
					elif operate_type == 'comment':
						try:
							mark = get_comment_operate_fb(task_detail)
						except Exception, e:
							#add_operate2redis(queue_dict)
							print 333333333333333, e
							pass
					elif operate_type == 'like':
						try:
							mark = get_like_operate_fb(task_detail)
						except Exception, e:
							#add_operate2redis(queue_dict)
							print 444444444444444, e
							pass
					elif operate_type == 'at':
						try:
							mark = get_at_operate_fb(task_detail)
						except Exception, e:
							#add_operate2redis(queue_dict)
							print 55555555555555, e
							pass

					elif operate_type == 'private':
						try:
							mark = get_private_operate_fb(task_detail)
						except Exception, e:
							#add_operate2redis(queue_dict)
							print 66666666666666, e
							pass

					elif operate_type == 'add':
						try:
							mark = get_add_friends(task_detail)
						except Exception, e:
							#add_operate2redis(queue_dict)
							print 77777777777777, e
							pass

					elif operate_type == 'confirm':
						try:
							mark = get_confirm_friends(task_detail)
						except Exception, e:
							#add_operate2redis(queue_dict)
							print 8888888888888, e
							pass

					elif operate_type == 'delete':
						try:
							mark = get_delete_friends(task_detail)
						except Exception, e:
							#add_operate2redis(queue_dict)
							print 99999999999999, e
							pass
					break
				except:
					continue

		elif channel == 'twitter':
			if operate_type == 'publish':
				try:
					mark = get_submit_tweet_tw(task_detail)
				except:
					#add_operate2redis(queue_dict)
					pass

			elif operate_type == 'retweet':
				try:
					mark = get_retweet_operate_tw(task_detail)
				except:
					#add_operate2redis(queue_dict)
					pass

			elif operate_type == 'comment':
				try:
					mark = get_comment_operate_tw(task_detail)
				except:
					#add_operate2redis(queue_dict)
					pass

			elif operate_type == 'like':
				try:
					mark = get_like_operate_tw(task_detail)
				except:
					#add_operate2redis(queue_dict)
					pass

			elif operate_type == 'at':
				try:
					mark = get_at_operate_tw(task_detail)
				except:
					#add_operate2redis(queue_dict)
					pass

			elif operate_type == 'private':
				try:
					mark = get_private_operate_tw(task_detail)
				except:
					#add_operate2redis(queue_dict)
					pass

			elif operate_type == 'follow':
				try:
					mark = get_follow_operate_tw(task_detail)
				except:
					#add_operate2redis(queue_dict)
					pass

			elif operate_type == 'unfollow':
				try:
					mark = get_unfollow_operate_tw(task_detail)
				except:
					#add_operate2redis(queue_dict)
					pass

		elif channel == 'weibo':
			if operate_type == 'publish':
				#count = 0
				#while count < 3:
				try:
		   			#print 'task_detail..',task_detail
					mark = get_submit_tweet(task_detail)
					print 'weibo!!!!!!!'
					break
				except Exception, e:
				#		count += 1
					print 'operate_timer.py'
					print e
					traceback.print_exc()
					#add_operate2redis(queue_dict)
					#continue
					pass
						

			elif operate_type == 'retweet':
				try:
					mark = get_reply_retweet(task_detail)
				except:
					#add_operate2redis(queue_dict)
					pass

			elif operate_type == 'comment':
				try:
					mark = get_reply_comment(task_detail)
				except:
					#add_operate2redis(queue_dict)
					pass

			elif operate_type == 'like':
				try:
					mark = get_like_operate(task_detail)
				except:
					#add_operate2redis(queue_dict)
					pass

			# elif operate_type == 'at':
			# 	try:
			# 	    mark = get_at_operate_tw(task_detail)
			# 	except:
			# 		add_operate2redis(queue_dict)

			elif operate_type == 'private':
				try:
					mark = get_reply_private(task_detail)
				except:
					#add_operate2redis(queue_dict)
					pass

			elif operate_type == 'follow':
				try:
					mark = get_reply_follow(task_detail)
				except:
					#add_operate2redis(queue_dict)
					pass

			elif operate_type == 'unfollow':
				try:
					mark = get_reply_unfollow(task_detail)
				except:
					add_operate2redis(queue_dict)
					pass

			elif operate_type == 'receive':
				try:
					mark = get_reply_total(task_detail)
				except:
					#add_operate2redis(queue_dict)
					pass



if __name__ == '__main__':
    print 'WEIBO_XNR_OPERATE_PATH..',WEIBO_XNR_OPERATE_PATH	
    print 'FACEBOOK_XNR_OPERATE_PATH..',FACEBOOK_XNR_OPERATE_PATH
    operate_out_of_redis()
    print "END.."

