#!/bin/bash
while ((1))
do
    cd /home/xnr1/xnr_0429/xnr/qq/
    python receiveQQGroupMessage_v2.py >> /var/log/receiveQQGroupMessage_v2.log
    sleep 60000000000000
done

