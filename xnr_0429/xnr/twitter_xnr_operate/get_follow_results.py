#-*- coding:utf-8 -*-
import json
import sys
sys.path.append('../')
sys.path.append('/home/xnr1/xnr_0429/xnr/')
from global_utils import es_xnr_2 as es, es_xnr, tw_xnr_index_name,tw_xnr_index_type,RE_QUEUE as ali_re, results_twitter_relation_params 


def get_follow_results():
    print "get_follow_results"
    while 1:
        print ali_re.lrange(results_twitter_relation_params, 0, 10)
        follow_results = ali_re.rpop(results_twitter_relation_params)
        if follow_results:
            print follow_results
        else:
            print "no results in redis"
            break
    


if __name__ == "__main__":
    get_follow_results()
