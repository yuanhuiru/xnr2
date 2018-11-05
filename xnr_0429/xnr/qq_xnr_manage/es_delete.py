# -*- coding: utf-8 -*-
from elasticsearch import Elasticsearch
import json

es_xnr = Elasticsearch('192.168.169.45:9205', timeout=600)

query_body = {
    'query':{
        'term':{'submitter':''}
    },
    'size': 9999
}

qq_xnr_index_type = 'user'

es_xnr.delete(index='qq_xnr', doc_type=qq_xnr_index_type, id='QXNR0027')


