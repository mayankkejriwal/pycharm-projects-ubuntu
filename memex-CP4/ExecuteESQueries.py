import SparqlTranslator, SelectExtractors, Grouper, ResultExtractors
from elasticsearch.helpers import bulk
from elasticsearch import Elasticsearch
# import gensim
import pprint, codecs, json
import PrintUtils
from sqparser import SQParser
from os import listdir
from os.path import isfile, join
import TableFunctions

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
    @DeprecationWarning
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
    @DeprecationWarning
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

    @DeprecationWarning
    @staticmethod
    def _trial_v2_queries():
        """
        contains 2-level queries, and has the same capabilities as translatePointFactQueries_v2.
        We expect to keep adding to this (a 'master' release, so to speak)
        """
        results = None
        sparql_stuff_path = '/home/mayankkejriwal/Downloads/'
        raw_query_file = sparql_stuff_path+'raw-queries-copy.txt'
        with codecs.open(raw_query_file, 'r', 'utf-8') as f:
            raw_sparql_queries = json.loads(f.read())
        raw_query = raw_sparql_queries['Cluster']['1636.1818']['sparql']
        pp = pprint.PrettyPrinter(indent=4)
        #pp.pprint(raw_query)
        sparql_query = SQParser.parse(raw_query, target_component = '')
        #index = 'dig-extractions'
        index =  'dig'
        #index = 'pr-index-1'
        url_localhost = "http://52.42.180.215:9200/"
        es = Elasticsearch(url_localhost)
        query = {}
        translatedDS = SparqlTranslator.SparqlTranslator.translateQueries(sparql_query,
                                                            sparql_stuff_path+'adsTable-v1.jl', 0)
        query['query'] = translatedDS['query']
        pp = pprint.PrettyPrinter(indent=4)
        print 'level 0 query:'
        pp.pprint(query)
        retrieved_frames = es.search(index= index, size = 10, body = query)
        if not retrieved_frames['hits']['hits']:
            del query
            del retrieved_frames
            del translatedDS
            query = {}
            translatedDS = SparqlTranslator.SparqlTranslator.translateQueries(sparql_query,
                                                            sparql_stuff_path+'adsTable-v1.jl', 1)
            query['query'] = translatedDS['query']
            #print translatedDS
            retrieved_frames = es.search(index = index, size = 10, body = query)
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

        if results:
            print 'Top retrieved result is :'
            pp.pprint(retrieved_frames['hits']['hits'][0]['_source'])
            print 'Results from ResultExtractors:'
            pp.pprint(results)

    @staticmethod
    def _parse_raw_queries(raw_queries_file, output_file):
        with codecs.open(raw_queries_file, 'r', 'utf-8') as f:
            raw_sparql_queries = json.loads(f.read())
        for aggregate in raw_sparql_queries['Aggregate'].itervalues():
            aggregate['parsed'] = SQParser.parse(aggregate['sparql'], target_component = '')
        for cluster in raw_sparql_queries['Cluster'].itervalues():
            cluster['parsed'] = SQParser.parse(cluster['sparql'], target_component = '')
        for pointfact in raw_sparql_queries['Point Fact'].itervalues():
            pointfact['parsed'] = SQParser.parse(pointfact['sparql'], target_component = '')
        file = codecs.open(output_file, 'w', 'utf-8')
        json.dump(raw_sparql_queries, file, indent = 4)
        file.close()

    @staticmethod
    def _wrap_results_isd_format(resultExtractions, question_id):
        answer = dict()
        answer['question_id'] = question_id
        answer['answers'] = resultExtractions
        return answer

    @staticmethod
    def _all_minus_incomplete_frames(all_frames, incomplete_frames):
        """

        :param all_frames: The list of all frames
        :param incomplete_frames: A list of frames that must not be included in answer
        :return: A new list of frames
        """
        answer = list()
        identifiers_set = set()
        for frame in incomplete_frames:
            #answer.append(frame)
            identifiers_set.add(frame['_source']['identifier'])
        for frame in all_frames:
            if frame['_source']['identifier'] not in identifiers_set:
                answer.append(frame)
        return answer

    @staticmethod
    def trial_v3_queries(raw_query_file, ads_table_file):
        """
        integrates 2-level queries, and can accommodate raw_query_files
        """
        results = None

        with codecs.open(raw_query_file, 'r', 'utf-8') as f:
            raw_sparql_queries = json.loads(f.read())
        raw_query = raw_sparql_queries['Point Fact']['61']['sparql']
        pp = pprint.PrettyPrinter(indent=4)
        #pp.pprint(raw_query)
        sparql_query = SQParser.parse(raw_query, target_component = '')
        # pp.pprint(sparql_query)
        #index = 'dig-extractions'
        index =  'dig'
        #index = 'pr-index-1'
        url_localhost = "http://52.42.180.215:9200/"
        es = Elasticsearch(url_localhost)
        query = {}
        translatedDS = SparqlTranslator.SparqlTranslator.translateToDisMaxQuery(sparql_query,ads_table_file, False)
        query['query'] = translatedDS['query']

        # print 'query:'
        pp.pprint(query)
        retrieved_frames = es.search(index= index, doc_type='webpage', size = 10, body = query)
        if not retrieved_frames['hits']['hits']:
            print 'no results'
        else:
            # pp.pprint(retrieved_frames['hits']['hits'][0])
            print('Number of retrieved frames ',len(retrieved_frames['hits']['hits']))
            results = ResultExtractors.ResultExtractors.standard_extractor(retrieved_frames,translatedDS, sparql_query)
        if results:
            print 'Top retrieved result is :'
            pp.pprint(retrieved_frames['hits']['hits'][0]['_source'])
            print 'Results from ResultExtractors:'
            pp.pprint(ExecuteESQueries._wrap_results_isd_format(results, '41'))
            #pp.pprint(results)

    @staticmethod
    def August3_2016__pointfactexecution():
        """
        Make sure to use the correct raw-queries file and index. Also, hash/hash-out the call to 'fill_out_frames'
        based on whether you want ALL results from the index to be included in the list. This entire set of methods
        should be moved or deprecated after the evaluations finish the week of Aug. 1, 2016
        """
        root_path = "/home/mayankkejriwal/Downloads/memex-cp4/"
        ads_table_file = root_path+'adsTable-v1.jl'
        raw_query_file = root_path+'raw-queries-ground-truth.txt'
        results = None
        with codecs.open(raw_query_file, 'r', 'utf-8') as f:
            raw_sparql_queries = json.loads(f.read())
        url_localhost = "http://52.42.180.215:9200/"
        es = Elasticsearch(url_localhost)
        index =  'dig-gt'
        #hash out the next three lines if we don't want to do the 'filling out'
        match_all_query = dict()
        match_all_query['query']=TableFunctions.build_match_all_query()
        all_frames = es.search(index= index, doc_type='webpage', size = 10000, body = match_all_query)
        keys = sorted(raw_sparql_queries['Point Fact'].keys())
        for k in keys:
            # if int(k)<82:
            #     continue
            print k
            v = raw_sparql_queries['Point Fact'][k]
            raw_query = v['sparql']
            pp = pprint.PrettyPrinter(indent=4)
            #pp.pprint(raw_query)
            sparql_query = SQParser.parse(raw_query, target_component = '')
            # pp.pprint(sparql_query)
            query = {}
            translatedDS = SparqlTranslator.SparqlTranslator.translateToDisMaxQuery(sparql_query,ads_table_file, False)
            query['query'] = translatedDS['query']
            #pp.pprint(query)
            retrieved_frames = es.search(index= index, doc_type='webpage', size = 10000, body = query)

            if not retrieved_frames['hits']['hits']:
                print 'no results'
             #   continue   #unhash both these lines if we don't fill out, and hash the following if
            #else:
            if True:
                # pp.pprint(retrieved_frames['hits']['hits'][0])
                print('Number of retrieved frames ',len(retrieved_frames['hits']['hits']))
                results = ResultExtractors.ResultExtractors.standard_extractor(retrieved_frames,translatedDS, sparql_query)
                #hash out the next four lines if we don't want to do the 'filling out'
                extra_frames = {'hits':{}}
                extra_frames['hits']['hits'] = ExecuteESQueries._all_minus_incomplete_frames\
                    (all_frames['hits']['hits'], retrieved_frames['hits']['hits'])
                extra_results = ResultExtractors.ResultExtractors.standard_extractor(extra_frames,translatedDS, sparql_query)
                results += extra_results
            if results:
                #print 'Top retrieved result is :'
                #pp.pprint(retrieved_frames['hits']['hits'][0]['_source'])
                #print 'Results from ResultExtractors:'
                bindings_dict = (ExecuteESQueries._wrap_results_isd_format(results, k))
                #pp.pprint(results)
                output_file = root_path+'PointFact/'+k+'.txt'
                file = codecs.open(output_file, 'w', 'utf-8')
                json.dump(bindings_dict, file)
                file.close()

    @staticmethod
    def August3_2016__clusterexecution():
        """
        see pointfactexecution for warnings
        """
        root_path = "/home/mayankkejriwal/Downloads/memex-cp4/"
        ads_table_file = root_path+'adsTable-v1.jl'
        raw_query_file = root_path+'raw-queries-ground-truth.txt'
        results = None

        with codecs.open(raw_query_file, 'r', 'utf-8') as f:
            raw_sparql_queries = json.loads(f.read())
        url_localhost = "http://52.42.180.215:9200/"
        es = Elasticsearch(url_localhost)
        index =  'dig-gt'
        #hash out the next three lines if we don't want to do the 'filling out'
        match_all_query = dict()
        match_all_query['query']=TableFunctions.build_match_all_query()
        all_frames = es.search(index= index, doc_type='webpage', size = 10000, body = match_all_query)
        keys = sorted(raw_sparql_queries['Cluster'].keys())
        for k in keys:
            # if k < '1701.1891':
            #     continue
            print k
            v = raw_sparql_queries['Cluster'][k]
            raw_query = v['sparql']
            pp = pprint.PrettyPrinter(indent=4)
            #pp.pprint(raw_query)
            sparql_query = SQParser.parse(raw_query, target_component = '')
            # pp.pprint(sparql_query)
            query = {}
            translatedDS = SparqlTranslator.SparqlTranslator.translateToDisMaxQuery(sparql_query,ads_table_file, False)
            query['query'] = translatedDS['query']
            # print 'query:'
            pp.pprint(query)
            retrieved_frames = es.search(index= index, doc_type='webpage', size = 10000, body = query)
            if not retrieved_frames['hits']['hits']:
                print 'no results'
             #   continue   #unhash both these lines if we don't fill out, and hash the following if
            #else:
            if True:
                # pp.pprint(retrieved_frames['hits']['hits'][0])
                print('Number of retrieved frames ',len(retrieved_frames['hits']['hits']))
                results = ResultExtractors.ResultExtractors.standard_extractor(retrieved_frames,translatedDS, sparql_query)
                #hash out the next four lines if we don't want to do the 'filling out'
                extra_frames = {'hits':{}}
                extra_frames['hits']['hits'] = ExecuteESQueries._all_minus_incomplete_frames\
                    (all_frames['hits']['hits'], retrieved_frames['hits']['hits'])
                extra_results = ResultExtractors.ResultExtractors.standard_extractor(extra_frames,translatedDS, sparql_query)
                results += extra_results
            if results:
                #print 'Top retrieved result is :'
                #pp.pprint(retrieved_frames['hits']['hits'][0]['_source'])
                #print 'Results from ResultExtractors:'
                bindings_dict = (ExecuteESQueries._wrap_results_isd_format(results, k))
                #pp.pprint(results)
                output_file = root_path+'Cluster/'+k+'.txt'
                file = codecs.open(output_file, 'w', 'utf-8')
                json.dump(bindings_dict, file)
                file.close()

    @staticmethod
    def August3_2016__aggregateexecution():
        """
        see pointfactexecution for warnings
        """
        root_path = "/home/mayankkejriwal/Downloads/memex-cp4/"
        ads_table_file = root_path+'adsTable-v1.jl'
        raw_query_file = root_path+'raw-queries-ground-truth.txt'
        results = None
        url_localhost = "http://52.42.180.215:9200/"
        es = Elasticsearch(url_localhost)
        index =  'dig-gt'
        #hash out the next three lines if we don't want to do the 'filling out'
        match_all_query = dict()
        match_all_query['query']=TableFunctions.build_match_all_query()
        all_frames = es.search(index= index, doc_type='webpage', size = 10000, body = match_all_query)
        with codecs.open(raw_query_file, 'r', 'utf-8') as f:
            raw_sparql_queries = json.loads(f.read())

        keys = sorted(raw_sparql_queries['Aggregate'].keys())
        for k in keys:
            #if int(k) > 1519:
             #   continue
            print k
            v = raw_sparql_queries['Aggregate'][k]
            raw_query = v['sparql']
            pp = pprint.PrettyPrinter(indent=4)
            #pp.pprint(raw_query)
            sparql_query = SQParser.parse(raw_query, target_component = '')
            #pp.pprint(sparql_query)
            query = {}
            translatedDS = SparqlTranslator.SparqlTranslator.translateToDisMaxQuery(sparql_query,ads_table_file, True)
            query['query'] = translatedDS['query']
            # print 'query:'
            #pp.pprint(query)
            retrieved_frames = es.search(index= index, doc_type='webpage', size = 10000, body = query)
            if not retrieved_frames['hits']['hits']:
                print 'no results'
             #   continue   #unhash both these lines if we don't fill out, and hash the following if
            #else:
            if True:
                # pp.pprint(retrieved_frames['hits']['hits'][0])
                print('Number of retrieved frames ',len(retrieved_frames['hits']['hits']))
                results = ResultExtractors.ResultExtractors.standard_extractor(retrieved_frames,translatedDS, sparql_query)
                #hash out the next four lines if we don't want to do the 'filling out'
                extra_frames = {'hits':{}}
                extra_frames['hits']['hits'] = ExecuteESQueries._all_minus_incomplete_frames\
                    (all_frames['hits']['hits'], retrieved_frames['hits']['hits'])
                extra_results = ResultExtractors.ResultExtractors.standard_extractor(extra_frames,translatedDS, sparql_query)
                results += extra_results
            if results:
                #print 'Top retrieved result is :'
                #pp.pprint(retrieved_frames['hits']['hits'][0]['_source'])
                #print 'Results from ResultExtractors:'
                bindings_dict = (ExecuteESQueries._wrap_results_isd_format(results, k))
                #pp.pprint(results)
                output_file = root_path+'Aggregate/'+k+'.txt'
                file = codecs.open(output_file, 'w', 'utf-8')
                json.dump(bindings_dict, file)
                file.close()

    @staticmethod
    def _append_path_to_list(list_of_files, path_prefix):
        answer = list()
        for f in list_of_files:
            answer.append(path_prefix+f)
        return answer

    @staticmethod
    def August3_2016__compilesubmissionfile():
        path = "/home/mayankkejriwal/Downloads/memex-cp4/"
        pointfact_files = ExecuteESQueries._append_path_to_list(
            [f for f in listdir(path+'PointFact/') if isfile(join(path+'PointFact/', f))], path+'PointFact/')
        aggregate_files = ExecuteESQueries._append_path_to_list(
            [f for f in listdir(path+'Aggregate/') if isfile(join(path+'Aggregate/', f))], path+'Aggregate/')
        cluster_files = ExecuteESQueries._append_path_to_list(
            [f for f in listdir(path+'Cluster/') if isfile(join(path+'Cluster/', f))], path+'Cluster/')
        files = pointfact_files+aggregate_files+cluster_files
        output_file = codecs.open(path+'usc-isi-submissionOnGroundTruth-highrecallextr-cp4.json', 'w', 'utf-8')
        output_file.write('[\n')
        for i in range(0, len(files)-1):
            input_file = files[i]
            with codecs.open(input_file, 'r', 'utf-8') as f:
                answer = json.loads(f.read())
                json.dump(answer, output_file)
                output_file.write(',\n')

        input_file = files[len(files)-1]
        with codecs.open(input_file, 'r', 'utf-8') as f:
            answer = json.loads(f.read())
            json.dump(answer, output_file)
            output_file.write('\n')
        output_file.write(']')
        output_file.close()

    @staticmethod
    def bulk_load_jl_to_index(jl_file, elasticsearch_host="http://localhost:9200/", index='de-output-03-index'):
        """
        will delete/re-create index if it already exists. Can handle exceptions gracefully. Because we only
        design this for serial experiments, we assume data to be loaded in is small (can be read in memory)
        and we load each object into the index in its own call.
        :param jl_file: to load into elasticsearch. _id field must already exist
        :param elasticsearch_host:
        :param index:
        :return:
        """
        data = list()
        count = 1
        with codecs.open(jl_file, 'r', 'utf-8') as f:
            for line in f:
                # if count>4:
                #     break
                data.append(json.loads(line))
                count += 1

        es = Elasticsearch(elasticsearch_host)
        if es.indices.exists(index):
            print("deleting '%s' index..." % (index))
            res = es.indices.delete(index=index)
            print(" response: '%s'" % (res))

        # since we are running locally, use one shard and no replicas
        request_body = {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0
            }
        }

        print("creating '%s' index..." % (index))
        res = es.indices.create(index=index, body=request_body)
        print(" response: '%s'" % (res))
        print 'number of documents to be indexed is ',
        print str(len(data)/2)
        # print data
        for i in range(0, len(data), 2):
            print 'processing document : ',
            print str(i/2)
            small_data = list()
            small_data.append(data[i])
            small_data.append(data[i+1])
            flag = True
            while flag:
                try:
                    res = es.bulk(index=index, doc_type='ads', body=small_data, refresh=True)
                    # print(" response: '%s'" % (res))
                except:
                    print 'Error in document : ',
                    print i
                else:
                    flag = False
        res = es.search(index=index, size=2, body={"query": {"match_all": {}}})
        print(" response: '%s'" % (res))

    @staticmethod
    def _current_trial():
        """
        Trials for the november qpr. Just to get me pumping.
        :return:
        """
        es = Elasticsearch('http://localhost:9200/')
        path = '/Users/mayankkejriwal/datasets/memex-evaluation-november/'
        optionalTriples = [['subject', 'service', 'bdsm'],['subject', 'names', 'jessica']]
        whereTriples = [['subject', 'text', 'escort']]
        query = {}
        query['query'] = SparqlTranslator.SparqlTranslator.translateFilterWhereOptionalToBool(whereTriples,None,optionalTriples,
                                                                      path+'adsTable-v2.jl')
        # print query
        retrieved_frames = es.search(index='de-output-03-index', size=500, body=query)
        print len(retrieved_frames['hits']['hits'])
        for frame in retrieved_frames['hits']['hits']:
            print frame['_id']
        # pp = pprint.PrettyPrinter(indent=4)
        # pp.pprint(retrieved_frames)


# ExecuteESQueries._current_trial()
# path = '/Users/mayankkejriwal/datasets/memex-evaluation-november/'
# ExecuteESQueries.bulk_load_jl_to_index(path+'de_output_03_elasticsearch_bulk_load.jl')
#ExecuteESQueries.August3_2016__compilesubmissionfile()
# root_path = "/home/mayankkejriwal/Downloads/memex-cp4/"
# ads_table = root_path+'adsTable-v1.jl'
# #ExecuteESQueries._parse_raw_queries(root_path+'raw-queries.txt', root_path+'parsed-queries.txt')
# #ExecuteESQueries._trial_v2_queries()
# raw_sparql_queries = root_path+'raw-queries-ground-truth.txt'
# ExecuteESQueries.trial_v3_queries(raw_sparql_queries, ads_table)

