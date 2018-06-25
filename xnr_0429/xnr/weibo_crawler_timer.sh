#!/bin/bash
while ((1))
do
  cd /home/xnr1/xnr_0429/xnr/sina/
  python weibo_crawler.py >> /var/log/weibo_crawler.log
  sleep 300
done
