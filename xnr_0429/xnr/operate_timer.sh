#!/bin/bash
while ((1))
do
    cd /home/xnr1/xnr_0429/xnr/timed_python_files/
    python operate_timer.py >> /var/log/operate_timer.log
    sleep 100
done

