#!/bin/bash
while ((1))
do
  cd /home/xnr1/xnr_0429/xnr/timed_python_files
  python retweet_in_time.py >> /var/log/retweet_in_time.py.log
  sleep 120
done

