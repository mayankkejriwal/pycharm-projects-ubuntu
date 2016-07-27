import SparqlTranslator, SelectExtractors, Grouper, ResultExtractors

from elasticsearch import Elasticsearch
#import gensim
import pprint, codecs, json
import PrintUtils

class ExecuteESQueries:
    """Anticipated starting point class. This is where a sparql query will be read in,
    serialized into internal data structures, and converted into elasticsearch queries.
    """

    @staticmethod
    @DeprecationWarning
    def _trial():
        """
        To try things out, test scenarios
        """
        sparql_stuff_path = "/home/mayankkejriwal/Downloads/"
        whereTriples1 = [['subject','itemOffered.name1','jasmine'],
                ['subject','itemOffered.ethnicity1','latina'],
                ['subject','mainEntityOfPage.description1','raleigh']]
        whereTriples2 = [['subject','itemOffered.name1','mary'],
                         ['subject','itemOffered.age1','?age'],
                         ['subject','mainEntityOfPage.description1','massage']]
        filterTriples = [['?age', '!=', '21']]
        optionalTriples = [['subject', 'mainEntityOfPage.description1','beautiful']]
        #must = SparqlTranslator.SparqlTranslator.translateSimpleWhereToES(whereTriples2, sparql_stuff_path+'offer_table.jl')
        query = {}
        query['query'] = SparqlTranslator.SparqlTranslator.translateFilterWhereOptionalToBool(whereTriples2, filterTriples,
                                                                                              optionalTriples,
                                                                                      sparql_stuff_path+'offer_table.jl')
        url_memex = "http://memex:digdig@52.36.12.77:8080/dig-latest"

        es = Elasticsearch(url_memex)
        retrieved_frames = es.search(index='offer', size = 10, body = query)
        pp = pprint.PrettyPrinter(indent=4)
        #pp.pprint(retrieved_frames)
        #print('Number of retrieved frames ',len(retrieved_frames))
        PrintUtils.PrintUtils.printItemOfferedFieldFromOfferFrames(retrieved_frames['hits']['hits'], 'age')


    @staticmethod
    @DeprecationWarning
    def _trial_pedro_queries_v1():
        """
        To try things out, test scenarios
        """
        sparql_stuff_path = "/home/mayankkejriwal/Downloads/"
        whereTriples1 = [['?ad',':location','San Diego, CA'],
                ['?ad',':phone','510-926-4391']]
        whereTriples2 = [['?ad',':location','South Kensington'],
                ['?ad',':service','GFE'],
                ['?ad', ':name', 'Estrella']
                         ]
        filterTriples = None
        optionalTriples = None
        #must = SparqlTranslator.SparqlTranslator.translateSimpleWhereToES(whereTriples2, sparql_stuff_path+'offer_table.jl')
        query = {}
        query['query'] = SparqlTranslator.SparqlTranslator.translateReadabilityTextToQuery(whereTriples1,
                                                                    sparql_stuff_path+'just_readability_text_table.jl')
        url_localhost = "http://localhost:9200/"

        es = Elasticsearch(url_localhost)
        retrieved_frames = es.search(index='pr-index-1', size = 10, body = query)
        pp = pprint.PrettyPrinter(indent=4)
        #del(query['query']['bool']['should'][1]['match'][u'readability_text']['operator'])
        pp.pprint(query)
        #pp.pprint(retrieved_frames)
        print('Number of retrieved frames ',len(retrieved_frames['hits']['hits']))
        PrintUtils.PrintUtils.printField(retrieved_frames['hits']['hits'], 'identifier')


    @staticmethod
    @DeprecationWarning
    def _trial_v1_queries():
        """
        To try things out, test scenarios
        """
        sparql_stuff_path = "/home/mayankkejriwal/Downloads/"
        sparql_data_structure = {
            'variable': '?ad',
            'clauses':[{'predicate': ':location', 'constraint':'San Diego, CA'},
                       {'predicate': ':phone', 'constraint':'510-926-4391'}]
        }

        query = {}
        query['query'] = SparqlTranslator.SparqlTranslator.translatePointFactQueries_v1(sparql_data_structure,
                                                                    sparql_stuff_path+'adsTable-v1.jl')
        url_localhost = "http://localhost:9200/"

        es = Elasticsearch(url_localhost)
        retrieved_frames = es.search(index='pr-index-1', size = 10, body = query)
        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(query)
        pp.pprint(retrieved_frames['hits']['hits'][3])
        print('Number of retrieved frames ',len(retrieved_frames['hits']['hits']))
        PrintUtils.PrintUtils.printField(retrieved_frames['hits']['hits'], 'identifier')

    @staticmethod
    def _trial_pedro_queries_v2():
        """
        To try things out, test scenarios
        """
        sparql_stuff_path = "/home/mayankkejriwal/Downloads/"
        queries = []
        with codecs.open(sparql_stuff_path+'test-sparql-queries.jl', 'r', 'utf-8') as f:
            for line in f:
                queries.append(json.loads(line))

        #test = queries[0]
        CDR = "B50A8D37BD2F01EA7C43E9EACDAC23473B03692B1872712EACF6D54FE597C5DD"
        for q in queries:
            if 'cdr' in q and q['cdr'] == CDR:
                test = q
                break


        sparql_query = {'parsed': {'where': test}}


        query = {}
        url_localhost = "http://localhost:9200/"
        es = Elasticsearch(url_localhost)

        query['query'] = SparqlTranslator.SparqlTranslator.translatePointFactQueries_v2(sparql_query,
                                                            sparql_stuff_path+'adsTable-v1.jl', 0)['query']
        retrieved_frames = es.search(index='pr-index-1', size = 10, body = query)
        if not retrieved_frames['hits']['hits']:
            del query
            del retrieved_frames
            query = {}
            query['query'] = SparqlTranslator.SparqlTranslator.translatePointFactQueries_v2(sparql_query,
                                                            sparql_stuff_path+'adsTable-v1.jl', 1)['query']
            retrieved_frames = es.search(index='pr-index-1', size = 10, body = query)
            val = (retrieved_frames['hits']['hits'][0]['_source']['identifier'] == CDR)
            print('val is ',val)
            pp = pprint.PrettyPrinter(indent=4)
            pp.pprint(query)
            if not retrieved_frames['hits']['hits']:
                print 'no results'
            else:
                pp.pprint(retrieved_frames['hits']['hits'][0])
                print('Number of retrieved frames ',len(retrieved_frames['hits']['hits']))
                PrintUtils.PrintUtils.printField(retrieved_frames['hits']['hits'], 'identifier')

    @staticmethod
    @DeprecationWarning
    def _trial_pedro_queries_v3(test_file, mapping_table_file):
        """
        The first real 'testing' scenario. For test_file example see Downloads/test-sparql-queries.jl
        I've posted the results of the 14 queries (at least the summary statistics) on slack.

        The 'new' data structure for the sparql queries is not compatible with this, because we
        have an 'explicit' where clause. We can't use this anymore.
        """

        queries = []
        with codecs.open(test_file, 'r', 'utf-8') as f:
            for line in f:
                queries.append(json.loads(line))
        url_localhost = "http://localhost:9200/"
        es = Elasticsearch(url_localhost)
        count = 0
        top1 = 0
        top5 = 0
        top10 = 0
        #test = queries[0]
        #CDR = "B50A8D37BD2F01EA7C43E9EACDAC23473B03692B1872712EACF6D54FE597C5DD"
        for q in queries:
            if 'cdr' not in q:
                continue
            count += 1
            cdr = q['cdr']
            test = q
            sparql_query = {'parsed': {'where': test}}
            query = {}
            query['query'] = SparqlTranslator.SparqlTranslator.translatePointFactQueries_v2(sparql_query,
                                                                        mapping_table_file, 0)
            pp = pprint.PrettyPrinter(indent=4)
            retrieved_frames = es.search(index='pr-index-1', size = 10, body = query)
            if retrieved_frames['hits']['hits']:
                identifiers = PrintUtils.PrintUtils.returnField(retrieved_frames['hits']['hits'], 'identifier')
                results = ExecuteESQueries._eval(identifiers, cdr)
                top1 += results[0]
                top5 += results[1]
                top10 += results[2]
                if results[0] + results[1] + results[2] == 0:
                    print('no results in top 10 for query: ')
                    pp.pprint(query)
                    pp.pprint(retrieved_frames['hits']['hits'][0])

            else:   #conservative strategy failed, try liberal
                del query
                del retrieved_frames
                query = {}
                query['query'] = SparqlTranslator.SparqlTranslator.translatePointFactQueries_v2(sparql_query,
                                                                       mapping_table_file, 1)
                retrieved_frames = es.search(index='pr-index-1', size = 10, body = query)

                if not retrieved_frames['hits']['hits']:
                    print 'no results'
                else:

                    identifiers = PrintUtils.PrintUtils.returnField(retrieved_frames['hits']['hits'], 'identifier')
                    results = ExecuteESQueries._eval(identifiers, cdr)
                    top1 += results[0]
                    top5 += results[1]
                    top10 += results[2]
                    if results[0] + results[1] + results[2] == 0:
                        print('no results in top 10 for query: ')
                        pp.pprint(query)
                        pp.pprint(retrieved_frames['hits']['hits'][0])

        print('total number of cdr queries processed : ',count)
        print('top 1 results : ',top1)
        print('top 5 results : ',top5)
        print('top 10 results : ',top10)

    @staticmethod
    def _eval(identifiers, cdr):
        """
        """
        #print(identifiers[0],' ', cdr)
        answer = []
        top1 = 0
        top5 = 0
        top10 = 0
        if identifiers[0] == cdr:
            top1 = 1
        else:
            broke = False
            for i in range(2,5):
                if identifiers[i] == cdr:
                    top5 = 1
                    broke = True
                    break
            if not broke:
                for i in range(5,10):
                    if identifiers[i] == cdr:
                        top10 = 1
                        break

        answer.append(top1)
        answer.append(top5)
        answer.append(top10)
        return answer

    @staticmethod
    def _trial_v2_queries():
        """
        contains 2-level queries, and has the same capabilities as translatePointFactQueries_v2.
        We expect to keep adding to this (a 'master' release, so to speak)
        """
        sparql_stuff_path = "/home/mayankkejriwal/Downloads/"
        with codecs.open(sparql_stuff_path+'test-sparql-25July2016.txt', 'r', 'utf-8') as f:
            sparql_queries = json.loads(f.read())

        sparql_query = sparql_queries['Point Fact']['test_4']

        url_localhost = "http://localhost:9200/"
        es = Elasticsearch(url_localhost)
        query = {}
        translatedDS = SparqlTranslator.SparqlTranslator.translateQueries(sparql_query,
                                                            sparql_stuff_path+'adsTable-v1.jl', 0)
        query['query'] = translatedDS['query']
        pp = pprint.PrettyPrinter(indent=4)
        print 'level 0 query:'
        pp.pprint(query)
        retrieved_frames = es.search(index='pr-index-1', size = 10, body = query)
        if not retrieved_frames['hits']['hits']:
            del query
            del retrieved_frames
            del translatedDS
            query = {}
            translatedDS = SparqlTranslator.SparqlTranslator.translateQueries(sparql_query,
                                                            sparql_stuff_path+'adsTable-v1.jl', 1)
            query['query'] = translatedDS['query']
            #print translatedDS
            retrieved_frames = es.search(index='pr-index-1', size = 10, body = query)
            print 'level 1 query:'
            pp.pprint(query)
            if not retrieved_frames['hits']['hits']:
                print 'no results'
            else:
                # pp.pprint(retrieved_frames['hits']['hits'][0])
                print('Number of retrieved frames ',len(retrieved_frames['hits']['hits']))
                results = ResultExtractors.ResultExtractors.standard_extractor(retrieved_frames,translatedDS, sparql_query)

        else:
            # pp.pprint(retrieved_frames['hits']['hits'][0])
            print('Number of retrieved frames ',len(retrieved_frames['hits']['hits']))
            results = ResultExtractors.ResultExtractors.standard_extractor(retrieved_frames,translatedDS, sparql_query)

        print 'Top retrieved result is :'
        pp.pprint(retrieved_frames['hits']['hits'][0]['_source'])
        print 'Results from ResultExtractors:'
        pp.pprint(results)

#path = '/home/mayankkejriwal/Downloads/'
#ExecuteESQueries._trial_pedro_queries_v2()
ExecuteESQueries._trial_v2_queries()