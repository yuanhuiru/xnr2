#！/bin/bash
######################################################
# $Name:        clean_xnr_es_index.sh
# $Version:     v1.0
# $Function:    clean xnr es log index
# $Author:      GOOGLE
# $Create Date: 2018-9-16
# $Description: shell
######################################################
#本文未加文件锁，需要的可以加
#脚本的日志文件路径
CLEAN_LOG="/var/log/clean_xnr_es_index.log"
#判断日志文件是否存在，不存在需要创建。
if [ ! -f  "${CLEAN_LOG}" ]
then
    touch "${CLEAN_LOG}"
fi


#elasticsearch 的主机ip及端口
SERVER_PORT=localhost:9205

esindex_list=("weibo_feedback_comment_" "weibo_feedback_retweet_" "weibo_feedback_private_" "weibo_feedback_at_" "weibo_feedback_like_" "new_xnr_flow_text_" "group_message_" "twitter_flow_text_" "facebook_flow_text_" "twitter_count_" "facebook_count_" "tw_bci_" "fb_bci_" "wx_group_message_" "wx_sent_group_message_" "facebook_feedback_comment_" "facebook_feedback_retweet_" "facebook_feedback_private_" "facebook_feedback_at_" "facebook_feedback_like_" "new_fb_xnr_flow_text_" "twitter_feedback_comment_" "twitter_feedback_retweet_" "twitter_feedback_private_" "twitter_feedback_at_" "twitter_feedback_like_" "new_tw_xnr_flow_text_" "weibo_community_" "weibo_user_warning_" "facebook_user_warning_" "twitter_user_warning_" "weibo_event_warning_" "facebook_event_warning_" "twitter_event_warning_" "weibo_speech_warning_" "facebook_speech_warning_" "twitter_speech_warning_" "weibo_time_warning_" "facebook_time_warning_" "twitter_time_warning_")
datenum_list=(30 30 30 30 30 1000 30 8 8 2 2 8 8 30 30 30 30 30 30 30 1000 30 30 30 30 30 1000 180 1000 1000 1000 1000 1000 1000 365 365 365 2000 2000 2000)
cutindex_list=(4 4 4 4 4 5 3 4 4 3 3 3 3 4 5 4 4 4 4 4 6 4 4 4 4 4 6 3 4 4 4 4 4 4 4 4 4 4 4 4)
index_list=(0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39)

for index in ${index_list[@]}
do
    #切割位置
    CUTINDEX=${cutindex_list[${index}]}

    #删除多少天以前的日志，假设输入10，意味着10天前的日志都将会被删除
    DELTIME=${datenum_list[${index}]}
    # seconds since 1970-01-01 00:00:00 seconds
    SECONDS=$(date -d  "$(date  +%F) -${DELTIME} days" +%s)

    #索引前缀
    INDEX_PRFIX=${esindex_list[${index}]}
    #取出已有的索引信息
    INDEXS=$(curl -s "${SERVER_PORT}/_cat/indices/${INDEX_PRFIX}*-*-*" | awk '{print $3}')
    #删除指定日期索引
    echo "----------------------------clean time is $(date +%Y-%m-%d_%H:%M:%S) ------------------------------" >>${CLEAN_LOG}
    for del_index in ${INDEXS}
    do
        #根据索引的名称的长度进行切割，不同长度的索引在这里需要进行对应的修改
        indexDate0=$( echo ${del_index} |cut -d "_" -f ${CUTINDEX})
        indexDate=$( echo ${indexDate0} |cut -d "-" -f 1,2,3)
        indexSecond=$( date -d ${indexDate} +%s )
        if [ $(( $SECONDS - $indexSecond )) -gt 0 ]
        then
            echo "${del_index}" >>${CLEAN_LOG}
            #取出删除索引的返回结果
            delResult=`curl -s  -XDELETE "${SERVER_PORT}/"${del_index}"?pretty" |sed -n '2p'`
            #写入日志
            echo "clean time is $(date)" >>${CLEAN_LOG}
            echo "delResult is ${delResult}" >>${CLEAN_LOG}
            sleep 10
        fi
    done
done
