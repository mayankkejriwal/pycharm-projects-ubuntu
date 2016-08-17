import ESQueryBuilders
import pprint, codecs, json
from elasticsearch import Elasticsearch, RequestsHttpConnection


def run_match_all(output_file=None, upper_limit=260000):
    """
    :param: output_file If not none, we will write out all retrieved frames to file
    :return: None
    """
    pp = pprint.PrettyPrinter(indent=4)
    index = 'ebola_data'
    url_EShost = "http://52.7.75.159:9020/"
    es = Elasticsearch(url_EShost, connection_class = RequestsHttpConnection, http_auth = ('lorelei', 'thorthor'))
    indices=es.indices.get_aliases().keys()
    # pp.pprint(indices)
    query = dict()
    query['query'] = ESQueryBuilders.build_match_all_query()
    # use the count function to calculate an appropriate upper_limit, then call it
    retrieved_frames = es.search(index= index, doc_type='record', size = upper_limit, body = query)
    print 'num frames retrieved: '
    pp.pprint(len(retrieved_frames['hits']['hits']))
    if output_file:
        file = codecs.open(output_file, 'w', 'utf-8')
        for frame in retrieved_frames['hits']['hits']:
            json.dump(frame['_source'], file) #use indent here for pretty printing
            file.write('\n')
        file.close()


def run_match():
    """
    :return: None
    """
    pp = pprint.PrettyPrinter(indent=4)
    index = 'ebola_data'
    url_EShost = "http://52.7.75.159:9020/"
    es = Elasticsearch(url_EShost, connection_class = RequestsHttpConnection, http_auth = ('lorelei', 'thorthor'))
    indices=es.indices.get_aliases().keys()
    # pp.pprint(indices)
    query = dict()
    query['query'] = ESQueryBuilders.build_match_clause('loreleiJSONMapping.text', 'Laguna verde')

    retrieved_frames = es.search(index= index, doc_type='record', size = 10, body = query)
    pp.pprint(retrieved_frames)


def count_records_in_ebola_data():
    """

    :return:
    """
    query = dict()
    index = 'ebola_data'
    url_EShost = "http://52.7.75.159:9020/"
    es = Elasticsearch(url_EShost, connection_class = RequestsHttpConnection, http_auth = ('lorelei', 'thorthor'))
    query['query'] = ESQueryBuilders.build_match_all_query()
    print es.count(index=index, body = query)['count']

#count_records_in_ebola_data()
run_match_all('/home/mayankkejriwal/Downloads/lorelei/ebola_data/record_dump_260000.json')