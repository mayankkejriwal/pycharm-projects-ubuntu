import TableFunctions, codecs, json
from elasticsearch import Elasticsearch
import nltk
import pprint
from sqparser import SQParser
# import re
#import sqparser
#Various odds and ends


def _try_sparql_parser():
    file_path = '/home/mayankkejriwal/Downloads/raw-queries-29July2016.txt'
    with codecs.open(file_path, 'r', 'utf-8') as f:
            raw_sparql_queries = json.loads(f.read())
    str_input = raw_sparql_queries['Point Fact']['54']['sparql']
    target_component = ''
    result = SQParser.parse(str_input, target_component=target_component)
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(result)
    #print len(raw_sparql_queries['Aggregate'])


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


# test_string = 'this is a test'
# print test_string
# path = '/home/mayankkejriwal/Downloads/dig-data/sample-datasets/escorts/'
# print re.split('/',path)[-1]
#_bulkload_json_file(path+'all-extractions-webpage-bulk-load.jl')
#_try_sparql_parser()