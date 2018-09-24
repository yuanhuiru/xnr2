#!/bin/bash
while ((1))
do
	python /home/xnr1/xnr_0429/xnr/facebook_xnr_create/fb_user_to_redis.py >> /var/log/fb_redis_log/fb_user_to_redis.log
    sleep 21600
	python /home/xnr1/xnr_0429/xnr/facebook_xnr_create/get_userinfo_from_redis.py >> /var/log/fb_redis_log/get_user_from_redis.log
    sleep 10000
done
