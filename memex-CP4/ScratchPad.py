import TableFunctions, codecs, json
from elasticsearch import Elasticsearch
import nltk
import re
"""

Various odds and ends
"""

def _try_nltk_grammar():
    """

    :return: None
    """
    grammar = nltk.CFG.fromstring("""
    P -> 'in'
    """)
    sent = ['in']
    parser = nltk.ChartParser(grammar)
    for tree in parser.parse(sent):
        print(tree)



def _run_localhost_query():
    """
    I want to make sure that we can run elasticsearch queries on the localhost
    :return: None
    """
    query = {}
    query['query'] = TableFunctions.build_match_clause('first_name', 'Doug*')
    url_localhost = "http://localhost:9200/"

    es = Elasticsearch(url_localhost)
    retrieved_frames = es.search(index='megacorp', size = 10, body = query)
    print(retrieved_frames)

def _bulkload_json_file(input_file):
    """

    :param input_file: The kind of file output by JSONToBulkFormat
    :return: None
    """
    url_localhost = "http://localhost:9200/"
    bulk_body = []
    with codecs.open(input_file, 'r', 'utf-8') as f:
            for line in f:
                entry = json.loads(line)
                bulk_body.append(entry)


    es = Elasticsearch(url_localhost)
    res = es.bulk(body = bulk_body, refresh = True)



#path = '/home/mayankkejriwal/Downloads/dig-data/sample-datasets/escorts/'
#_bulkload_json_file(path+'all-extractions-webpage-bulk-load.jl')
#_try_nltk_grammar()
separator = ','
e = ['a','b','c']
print separator.join(e)