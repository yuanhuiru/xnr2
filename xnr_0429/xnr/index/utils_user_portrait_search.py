# -*- coding: utf-8 -*-
from elasticsearch import Elasticsearch


es = Elasticsearch(['10.128.55.69:9200','10.128.55.70:9200','10.128.55.71:9200'], timeout=600)
portrait_index_name = 'user_portrait_1222'
portrait_index_type = 'user'


def utils_user_portrait_search(uid):
    query_body = {
        'query': {
            'filtered': {
                'filter': {
                    'bool': {
                        'must': [
                            {'term': {'uid': '%s' % uid}}
                        ]
                    }
                }
            }
        },
        'size': 999,
    }
    result = es.search(portrait_index_name, portrait_index_type, query_body)['hits']['hits']
    if result:
        info = result[0]['_source']
        return info
    return {'Err': 'Not Found.'}


if __name__ == '__main__':
    utils_user_portrait_search('2098130837')
