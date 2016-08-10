import ESQueryBuilders
import pprint, codecs, json
from elasticsearch import Elasticsearch, RequestsHttpConnection

def run_match_all(output_file=None):
    """
    :param: output_file If not none, we will write out all retrieved frames to file
    :return:
    """
    pp = pprint.PrettyPrinter(indent=4)
    index =  'ebola_data'
    url_EShost = "http://52.7.75.159:9020/"
    es = Elasticsearch(url_EShost, connection_class = RequestsHttpConnection, http_auth = ('lorelei', 'thorthor'))
    indices=es.indices.get_aliases().keys()
    # pp.pprint(indices)
    query = dict()
    query['query'] = ESQueryBuilders.build_match_all_query()
    #50,000 is the upper limit for the current data in ebola_data. Update as necessary.
    retrieved_frames = es.search(index= index, doc_type='record', size = 50000, body = query)
    print 'num frames retrieved: '
    pp.pprint(len(retrieved_frames['hits']['hits']))
    if output_file:
        file = codecs.open(output_file, 'w', 'utf-8')
        for frame in retrieved_frames['hits']['hits']:
            json.dump(frame['_source'], file)
            file.write('\n')
        file.close()


run_match_all('/home/mayankkejriwal/Downloads/lorelei/ebola_data/record_dump.json')