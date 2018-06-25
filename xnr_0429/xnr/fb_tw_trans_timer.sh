#!/bin/bash
while ((1))
do
	python /home/xnr1/xnr_0429/xnr/timed_python_files/fb_tw_trans_timer.py >> /var/log/fb_tw_trans_timer.log
    sleep 840
done
