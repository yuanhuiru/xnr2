#!/bin/bash
while ((1))
do
    python /home/xnr1/xnr_0429/xnr/timed_python_files/fb_tw_trans_redis_push.py >> /var/log/fb_tw_trans_ali_push.log
    python /home/xnr1/xnr_0429/xnr/timed_python_files/fb_tw_trans_redis_pop.py >> /var/log/fb_tw_trans_ali_pop.log
    sleep 840
done


