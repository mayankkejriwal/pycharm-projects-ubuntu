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
             #   continue   #unhash both these lines if we don't fill out, and hash them (and also indent) if you do
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
    def bulk_load_ads_jl_to_index(jl_file, elasticsearch_host="http://localhost:9200/", index='gt-index-1',
                                  doc_type='ads', mapping_file=None):
        """
        Use this only for bulk-loading ads! For clusters, I'm writing a separate piece of code.
        will delete/re-create index if it already exists. Can handle exceptions gracefully. Because we only
        design this for serial experiments, we assume data to be loaded in is small (can be read in memory)
        and we load each object into the index in its own call.
        :param jl_file: to load into elasticsearch. _id field must already exist
        :param elasticsearch_host:
        :param index:
        :param mapping_file: should be a json: if this exists, we'll load it in as the request body, otherwise
        we'll use a default request body.
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
        if not mapping_file:
            request_body = {
                "settings": {
                    "index.mapping.total_fields.limit": 20000,
                    "number_of_shards": 1,
                    "number_of_replicas": 1
                }
            }
        else:
            mapping_in = codecs.open(mapping_file, 'r', 'utf-8')
            request_body = json.load(mapping_in)
            mapping_in.close()
            settings_dict = {
                    "number_of_shards": 1,
                    "number_of_replicas": 1
                }
            request_body['settings'] = settings_dict
            del request_body['warmers']
            print request_body['settings']

        print("creating '%s' index..." % (index))
        res = es.indices.create(index=index, body=request_body)
        print(" response: '%s'" % (res))
        print 'number of documents to be indexed is ',
        print str(len(data)/2)
        # print data
        for i in range(0, len(data), 2):
            # print 'processing document : ',
            # print str(i/2)
            small_data = list()
            small_data.append(data[i])
            small_data.append(data[i+1])
            flag = True
            while flag:
                try:
                    res = es.bulk(index=index, doc_type=doc_type, body=small_data, refresh=True)
                    if res:
                        for item in res['items']:
                            if item['index']['status'] != 201:
                                print res
                    # print res
                    # print 'processed index...',
                    # print i
                    # print(" response: '%s'" % (res))
                except:
                    print 'Error in document : ',
                    print i
                else:
                    flag = False
        res = es.search(index=index, size=2, body={"query": {"match_all": {}}})
        print(" response: '%s'" % (res))

    @staticmethod
    def bulk_load_clusters_jl_to_index(jl_file, elasticsearch_host="http://localhost:9200/", index='gt-index-1',
                                  doc_type='clusters', mapping_file=None):
        """
        Use this only for bulk-loading ads! For clusters, I'm writing a separate piece of code.
        will delete/re-create index if it already exists. Can handle exceptions gracefully. Because we only
        design this for serial experiments, we assume data to be loaded in is small (can be read in memory)
        and we load each object into the index in its own call.
        :param jl_file: to load into elasticsearch. _id field must already exist
        :param elasticsearch_host:
        :param index:
        :param mapping_file: should be a json: if this exists, we'll load it in as the request body, otherwise
        we'll use a default request body.
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
        # if es.indices.exists(index):
        #     print("deleting '%s' index..." % (index))
        #     res = es.indices.delete(index=index)
        #     print(" response: '%s'" % (res))

        # since we are running locally, use one shard and no replicas
        # if not mapping_file:
        #     request_body = {
        #         "settings": {
        #             "index.mapping.total_fields.limit": 20000,
        #             "number_of_shards": 1,
        #             "number_of_replicas": 1
        #         }
        #     }
        # else:
        #     mapping_in = codecs.open(mapping_file, 'r', 'utf-8')
        #     request_body = json.load(mapping_in)
        #     mapping_in.close()
        #     settings_dict = {
        #         "number_of_shards": 1,
        #         "number_of_replicas": 1
        #     }
        #     request_body['settings'] = settings_dict
        #     del request_body['warmers']
        #     print request_body['settings']
        #
        # print("creating '%s' index..." % (index))
        # res = es.indices.create(index=index, body=request_body)
        # print(" response: '%s'" % (res))
        print 'number of documents to be indexed is ',
        print str(len(data) / 2)
        # print data
        for i in range(0, len(data), 2):
            # print 'processing document : ',
            # print str(i/2)
            small_data = list()
            small_data.append(data[i])
            small_data.append(data[i + 1])
            flag = True
            while flag:
                try:
                    res = es.bulk(index=index, doc_type=doc_type, body=small_data, refresh=True)
                    if res:
                        for item in res['items']:
                            if item['index']['status'] != 201:
                                print res
                                # print res
                                # print 'processed index...',
                                # print i
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
        optionalTriples = [#['subject', 'ethnicity', 'Asian'],
                          #['subject', 'name', 'TINA'],
                           ['subject', 'phone', '425-609-2235']]

        whereTriples = list()
        k = list()
        for triple in optionalTriples:
            k.append(triple[2])
        whereTriples.append(['subject', 'loose_text', ' '.join(k)])
        print whereTriples
        print optionalTriples
        filterTriples = None
        query = dict()
        query['query'] = SparqlTranslator.SparqlTranslator.translateFilterWhereOptionalToBool(
            whereTriples, filterTriples, optionalTriples, path+'adsTable-v2.jl')
        # print query
        retrieved_frames = es.search(index='de-output-07-index-1', size=5, body=query)
        print len(retrieved_frames['hits']['hits'])
        for frame in retrieved_frames['hits']['hits']:
            # print frame
            print frame['_id']+'\t'+frame['_source']['url']
        # pp = pprint.PrettyPrinter(indent=4)
        # pp.pprint(retrieved_frames)


    @staticmethod
    def test_ES_index(host="https://10.1.94.103:9201/", index = 'dig-nov-eval-gt-02'):
        es = Elasticsearch(host)
        query = dict()
        query['query'] = TableFunctions.build_match_all_query()
        retrieved_frames = es.search(index=index, size=10, body=query)
        print retrieved_frames

    @staticmethod
    def print_ad_corresponding_to_id(id, host="https://10.1.94.103:9201/", index = 'dig-nov-eval-gt-02'):
        match = TableFunctions.build_match_clause('cdr_id', id)
        query = dict()
        query['query'] = match



    @staticmethod
    def read_embedding_file(embeddings_file):
        unigram_embeddings = dict()
        with codecs.open(embeddings_file, 'r', 'utf-8') as f:
            for line in f:
                obj = json.loads(line)
                for k, v in obj.items():
                    unigram_embeddings[k] = v
        return unigram_embeddings

    @staticmethod
    def train_embedding_classifiers(training_folder, embeddings_file, use_embeddings=False):
        """
        We will read in dictionaries required for both Rahul and Majid's code.
        :param training_folder: Contains the training files we need
        :return: A dictionary of classifiers; also, embeddings_dict
        """
        country_dict = training_folder+'city_country_dict_15000.json'
        city_state_dict = training_folder+'city_state_dict.json'
        city_dict = training_folder+'city_dict_over1K.json'
        answer = dict()

        if use_embeddings:
            from rankingExtractions import train_ranker # avoid unnecessary installation of nltk, if this is not used.

            TRAINING_FILE_CITIES = training_folder+'manual_7_cities.jl'
            TRAINING_FILE_NAMES = training_folder+'manual_50_names.jl'
            TRAINING_FILE_ETHNICITIES = training_folder+'manual_50_ethnicities.jl'
            # ACTUAL_FILE_CITIES = training_folder+'manual_50_cities.jl' # testing data that Rahul used.
            # ACTUAL_FILE_NAMES = training_folder+'manual_50_names.jl'
            # ACTUAL_FILE_ETHNICITIES = training_folder+'manual_50_ethnicities.jl'

            EMBEDDINGS_FILE = embeddings_file
            FIELD_NAMES_CITIES = {
                "text_field": "readability_text",
                "annotated_field": "annotated_cities",
                "correct_field": "correct_cities"
            }
            FIELD_NAMES_NAMES = {
                "text_field": "readability_text",
                "annotated_field": "annotated_names",
                "correct_field": "correct_names"
            }
            FIELD_NAMES_ETHNICITIES = {
                "text_field": "readability_text",
                "annotated_field": "annotated_ethnicities",
                "correct_field": "correct_ethnicities"
            }

            embeddings_dict = ExecuteESQueries.read_embedding_file(EMBEDDINGS_FILE)

            classifier_cities = train_ranker.train_ranker(embeddings_dict, TRAINING_FILE_CITIES, FIELD_NAMES_CITIES)
            classifier_names = train_ranker.train_ranker(embeddings_dict, TRAINING_FILE_NAMES, FIELD_NAMES_NAMES)
            classifier_ethnicities = train_ranker.train_ranker(embeddings_dict, TRAINING_FILE_ETHNICITIES, FIELD_NAMES_ETHNICITIES)



            answer['city'] = classifier_cities # warning, this property does not actually exist in our mapping table.
            answer['name'] = classifier_names
            answer['ethnicity'] = classifier_ethnicities
            answer['embeddings'] = embeddings_dict


        # if you make changes here, you must make equivalent changes in SelectExtractors.extract_locations (go right to the end of that func.)
        answer['city_dict'] = json.load(open(city_dict)) # dictionary required by Majid
        # answer['city_state_dict'] = json.load(open(city_state_dict))
        # answer['country_dict'] = json.load(open(country_dict))
        # print type(classifier_cities)
        # print type(classifier_names)
        # print type(classifier_ethnicities)
        return answer

    @staticmethod
    def November_2016_pre_execution1():
        """
        Because so much is changing between the november and august evaluations, I want to use this
        code as a kind of 'proving ground'
        :return:
        """
        root_path = '/Users/mayankkejriwal/datasets/memex-evaluation-november/'
        classifiers = ExecuteESQueries.train_embedding_classifiers(root_path + 'embedding_training_files/',
                                                                   root_path + 'unigram-part-00000-v2.json')
        ads_table_file = root_path + 'adsTable-v3.jl'
        # raw_query_file = root_path + 'raw-queries-ground-truth.txt'
        # parsed_query_file = root_path + 'json_file_for_25_parsed_sparql_queries.js' # the sample queries we designed
        parsed_query_file = root_path+'november-sample-questions.json'
        # parsed_query_file = root_path + 'parsed-queries-2.txt'
        parsed_query_file_in = codecs.open(parsed_query_file, 'r', 'utf-8')
        # parsed_PF_queries = json.load(parsed_query_file_in)
        # print parsed_PF_queries
        parsed_PF_queries = json.load(parsed_query_file_in)['Point Fact']
        parsed_query_file_in.close()
        url_localhost = "http://10.1.94.103:9201/"
        # url_localhost = "http://localhost:9200"
        es = Elasticsearch(url_localhost)
        index = 'dig-nov-eval-gt-02'
        # index = 'gt-index-1'
        results = None
        # with codecs.open(raw_query_file, 'r', 'utf-8') as f:
        #     raw_sparql_queries = json.loads(f.read())
        keys = sorted(parsed_PF_queries.keys())
        # for k in range(0, len(parsed_PF_queries)):
        for k in keys:
            print k
            # sparql_query = parsed_PF_queries[k]['parsed']
            sparql_query = parsed_PF_queries[k]['parsed']
            if not sparql_query:
                continue
            # raw_query = v['sparql']
            pp = pprint.PrettyPrinter(indent=4)
            # pp.pprint(v)
            # sparql_query = SQParser.parse(raw_query, target_component='')
            pp.pprint(sparql_query)
            query = dict()
            translatedDS = SparqlTranslator.SparqlTranslator.translateToDisMaxQuery(sparql_query, ads_table_file, False)
            # query['query'] = TableFunctions.build_match_all_query()
            query['query'] = translatedDS['query']
            # outtmp= codecs.open(root_path+'queryexample.json', 'w', 'utf-8')
            # json.dump(query, outtmp)
            # outtmp.close()
            # print query
            # pp.pprint(query)
            # out = codecs.open(root_path+'query_tmp.json', 'w', 'utf-8')
            # json.dump(query, out)
            # out.close()
            try:
                retrieved_frames = es.search(index=index, doc_type='ads', size=1000, body=query)
            except:
                pass
            if not retrieved_frames['hits']['hits']:
                print 'no results'
            else:
                print('Number of retrieved frames ', len(retrieved_frames['hits']['hits']))
                results = ResultExtractors.ResultExtractors.standard_extractor(retrieved_frames, translatedDS,
                                                                               sparql_query,
                                                                               classifier_dict=classifiers)
                if results:
                    # print 'Top retrieved result is :'
                    # pp.pprint(retrieved_frames['hits']['hits'][0]['_source'])
                    # print 'Results from ResultExtractors:'
                    bindings_dict = (ExecuteESQueries._wrap_results_isd_format(results, k))
                    # pp.pprint(results)
                    output_file = root_path + 'output-folder-1/' + k + '.txt'
                    file = codecs.open(output_file, 'w', 'utf-8')
                    json.dump(bindings_dict, file)
                    file.close()
                    # break

    @staticmethod
    def November_2016_pre_execution2():
        """
        If necessary:

        1. Paths must be changed in: this function, TableFunctions.build_match_clause_with_keyword_expansion,
        MappingTable (if you change the table)

        2. Index, elasticsearch IP must be changed in: this function, SparqlTranslator.translateClusterQueries

        3. If attributes in schema change (e.g. hair-color instead of hair_color), you should go through all the
        code in this package to replace. Attribute-name changes are not limited to MappingTable anymore.

        4.  If Majid posts new code, copy paste into GetLocation and MAKE SURE to hash out anything that declares
        it as a main file (see the current GetLocation). If he needs new dictionaries, you'll need to change
        train_embedding

        ONLY if you're desperate (queries are not returning results, and you need to short-circuit):

        Look at the code in _current_trial. You can construct a simpler query using optional, filter, where
        triples, but you'll have to build wrapper code to make it work. If things become that bad, just route
        all constraints to the text props and make everything optional (a simple version of the IR strategy).
        :return: None
        """
        root_path = '/home/mayankkejriwal/Downloads/memex-cp2/nov-2016/'
        embedding_training_folder = 'embedding_training_files/'
        embedding_training_file = 'lrr_unigram-v2.json'
        ads_table_file = 'lattice-isi-adsTable-v1.jl'
        url_localhost = "http://memex:digdig@52.36.12.77:8080/"
        # url_localhost = "http://10.1.94.103:9201/"
        index = 'dig-nov-eval-gt-05'
        parsed_query_file = 'parsed-queries/PF-queries-parsed.json'
        ads_table_file = root_path +  ads_table_file
        parsed_query_file = root_path + parsed_query_file
        output_folder = 'lattice-isi-point-fact/'
        # set use_embeddings to True if you want to use Rahul's code. You can replace the unigram file with a different
        # one if it improves performance (e.g. lrr, hrr, ground-truth etc.)
        #the function name is a complete misnomer. We must call it for Majid's code.

        classifiers = ExecuteESQueries.train_embedding_classifiers(root_path+embedding_training_folder,
                                root_path+embedding_training_folder+embedding_training_file, use_embeddings=True)

        # if anything changes in the mapping table, regenerate this file; see the last line in MappingTable.py

        parsed_query_file_in = codecs.open(parsed_query_file, 'r', 'utf-8')
        parsed_PF_queries = json.load(parsed_query_file_in)#[0:1]
        # print len(parsed_PF_queries)
        # print parsed_PF_queries
        parsed_query_file_in.close()

        # url_localhost = "http://localhost:9200"
        es = Elasticsearch(url_localhost)

        # index = 'gt-index-1'
        results = None

        # if something goes wrong, you'll know where in the list it occurred
        for k in range(0, len(parsed_PF_queries)):
            # print k
            # if parsed_PF_queries[k]['id'] != "94-2":
            #     continue
            sparql_query = parsed_PF_queries[k]['SPARQL']
            print 'processing query...',
            print parsed_PF_queries[k]['id']
            if not sparql_query:
                print 'Nothing to query!'
                continue

            pp = pprint.PrettyPrinter(indent=4)
            # pp.pprint(sparql_query)
            query = dict()
            translatedDS = SparqlTranslator.SparqlTranslator.translateToDisMaxQuery(sparql_query, ads_table_file, False)
            # query['query'] = TableFunctions.build_match_all_query()
            query['query'] = translatedDS['query']
            query['_source'] = dict()
            query['_source']['exclude'] = ["tokens_extracted_text",
                                                           "tokens_high_precision",
                                                           "tokens_high_recall",
                                                           "tokens_title",
                                                           "tokens"]

            ## use these print statements for debugging
            # pp.pprint(query['query']['dis_max']['queries'][2])
            # outtmp= codecs.open(root_path+'queryexample.json', 'w', 'utf-8')
            # json.dump(query['query']['dis_max']['queries'][2], outtmp)
            # outtmp.close()
            # print query
            # pp.pprint(query)
            # out = codecs.open(root_path+'query_tmp.json', 'w', 'utf-8')
            # json.dump(query, out)
            # out.close()
            try:

                retrieved_frames = es.search(index=index, doc_type='ads', size=10, body=query)
            except:
                pass
            if not retrieved_frames['hits']['hits']:
                print 'no results'
            else:
                print('Number of retrieved frames ', len(retrieved_frames['hits']['hits']))
                results = ResultExtractors.ResultExtractors.standard_extractor(retrieved_frames, translatedDS,
                                                                               sparql_query, classifier_dict=classifiers)
                if results:
                    # print 'Top retrieved result is :'
                    # pp.pprint(retrieved_frames['hits']['hits'][0]['_source'])

                    bindings_dict = (ExecuteESQueries._wrap_results_isd_format(results, k))

                    # everything gets written out to a folder
                    output_file = root_path + output_folder + str(parsed_PF_queries[k]['id'])
                    file = codecs.open(output_file, 'w', 'utf-8')
                    json.dump(bindings_dict, file)
                    file.close()
            # break

# root_path = '/Users/mayankkejriwal/datasets/memex-evaluation-november/'
# classifiers = ExecuteESQueries.train_embedding_classifiers(root_path + 'embedding_training_files/',
#                                                                        root_path + 'unigram-part-00000-v2.json')
ExecuteESQueries.November_2016_pre_execution2()
# ExecuteESQueries.test_ES_index()
# ExecuteESQueries._current_trial()
# path = '/Users/mayankkejriwal/datasets/memex-evaluation-november/'
# ExecuteESQueries.bulk_load_clusters_jl_to_index(path+'clustering-mapping-CDR-id/phone-sim-de-24-0.1-bulk-load.jl')
# ExecuteESQueries.bulk_load_ads_jl_to_index(path+'summer_output_24_elasticsearch_bulk_load_2.jl')
#ExecuteESQueries.August3_2016__compilesubmissionfile()
# root_path = "/home/mayankkejriwal/Downloads/memex-cp4/"
# ads_table = root_path+'adsTable-v1.jl'
# #ExecuteESQueries._parse_raw_queries(root_path+'raw-queries.txt', root_path+'parsed-queries.txt')
# #ExecuteESQueries._trial_v2_queries()
# raw_sparql_queries = root_path+'raw-queries-ground-truth.txt'
# ExecuteESQueries.trial_v3_queries(raw_sparql_queries, ads_table)

