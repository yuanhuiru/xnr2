#_*_coding:utf-8_*_
# 删除领域 输入乱码的领域名称
# http://114.255.183.221:6010/weibo_xnr_knowledge_base_management/delete_domain/?domain_name=%E8%8D%89%E6%A0%B9
# 删除show_domain_second中的部分数据(渗透领域)
from elasticsearch import Elasticsearch


weibo_domain_index_name = "weibo_domain"
weibo_domain_index_type = "group"
es_xnr = Elasticsearch(['192.168.169.45:9205','192.168.169.46:9205','192.168.169.47:9205'], timeout=600)

query_body = {'query':{'term':{'compute_status':3}},'size':900}
#query_body = {'query':{'term':{'domain_name':"��??��????��???"}}}

es_results = es_xnr.search(index=weibo_domain_index_name,doc_type=weibo_domain_index_type,body=query_body)['hits']['hits']
#es_xnr.delete(index=weibo_domain_index_name,doc_type=weibo_domain_index_type,body=query_body)['hits']['hits']

for result in es_results:
    print result['_source']['domain_name']
    print result['_source']
