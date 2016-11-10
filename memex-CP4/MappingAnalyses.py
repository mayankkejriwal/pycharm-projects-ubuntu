from elasticsearch import Elasticsearch
import pprint, codecs, json
import TableFunctions, BuildCompoundESQueries

class MappingAnalyses:
    """
    This class contains methods for getting various statistics on our index in elastic search. For example,
    how many fields are missing, are missing fields correlated etc.
    """

    @staticmethod
    def print_dense_docs_sample(elasticsearch_host, index_name, doc_type, minimum_should_match, sample_size):
        """

        :param elasticsearch_host:
        :param index_name:
        :param doc_type:
        :param minimum_should_match: a number that has percentage semantics
        :param sample_size:
        :return:
        """
        webpage_properties = MappingAnalyses._get_list_of_all_webpage_properties()
        should = list()
        for property in webpage_properties:
            should.append(TableFunctions.build_constant_score_exists_clause(property))
        bool_query = BuildCompoundESQueries.BuildCompoundESQueries.build_bool_arbitrary(should = should)
        msm_str = str(minimum_should_match) + '%'
        bool_query['bool']['minimum_should_match'] = msm_str
        query = dict()
        query['query'] = bool_query
        es = Elasticsearch(elasticsearch_host)
        retrieved_frames = es.search(index= index_name, doc_type=doc_type, size = sample_size, body = query)
        pp = pprint.PrettyPrinter(indent=4)
        if retrieved_frames:
            pp.pprint(retrieved_frames)

    @staticmethod
    def docs_density_frequency_statistics(elasticsearch_host, index_name, doc_type, output_file = None):
        """
        We will vary the 'density' (the percentage of exists queries that should match) and print out the CUMULATIVE
        frequency at each density point. Interpret the results with care.

        Because of timeout issues, this function has to be called in lags at times. Change the 'range' line
        in the function to start from where you left off the last time something crashes.
        :param elasticsearch_host: the elasticsearch host
        :param index_name: the name of the elasticsearch index
        :param doc_type: the type in the index
        :return: None
        """


        webpage_properties = MappingAnalyses._get_list_of_all_webpage_properties()
        should = list()
        for property in webpage_properties:
            should.append(TableFunctions.build_constant_score_exists_clause(property))
        bool_query = BuildCompoundESQueries.BuildCompoundESQueries.build_bool_arbitrary(should = should)
        cumul_freq_dict = dict() # key is the 'density' in percent, value is total number of docs retrieved.
        es = Elasticsearch(elasticsearch_host)
        for i in range(0, 101, 5):
            msm_str = str(i) + '%'
            bool_query['bool']['minimum_should_match'] = msm_str
            query = dict()
            query['query'] = bool_query
            count = es.count(index= index_name, doc_type=doc_type, body = query)['count']
            print str(i)+'\t'+str(count)
            cumul_freq_dict[i] = count
        query = dict()
        query['query'] = TableFunctions.build_match_all_query()
        cumul_freq_dict[0] = es.count(index= index_name, doc_type=doc_type, body = query)['count']
        if output_file:
            file = codecs.open(output_file, 'w', 'utf-8')
            json.dump(cumul_freq_dict, file)
            file.write('\n')
            file.close()
        else:
            pp = pprint.PrettyPrinter(indent=4)
            pp.pprint(cumul_freq_dict)


    @staticmethod
    def missing_field_statistics(elasticsearch_host, index_name, output_file=None):
        """
        The method works by first retrieving the mapping file of the doc_type. It then poses
        field stats query and prints out results to file/console. For the definitions of the
        various statistics returned
        see https://www.elastic.co/guide/en/elasticsearch/reference/1.7/search-field-stats.html
        :param elasticsearch_host: the elasticsearch host
        :param index_name: the name of the elasticsearch index
        :param output_file: the file where the mapping analyses will be printed, if specified.
        :return: None
        """
        es = Elasticsearch(elasticsearch_host)
        # mapping = es.indices.get_mapping(index = index_name, doc_type = doc_type)
        fields = MappingAnalyses._get_list_of_all_webpage_properties()
        field_stats = es.field_stats(index=index_name, level='indices', fields=fields)
        if output_file:
            file = codecs.open(output_file, 'w', 'utf-8')
            json.dump(field_stats, file)
            file.write('\n')
            file.close()
        else:
            pp = pprint.PrettyPrinter(indent=4)
            pp.pprint(field_stats)

    @staticmethod
    def _get_list_of_all_webpage_properties():
        """
       There's a better way to do this (i.e. by taking the mapping dict and running a recursive search);
       right now, we'll do it the brute-force way.
        :return: A list of all properties
        """
        non_raw = ['age', 'age_count', 'business_type_count', 'dateCreated', 'description', 'drug_use','drug_use_count',
                'email.isObfuscated', 'email.uri', 'email_count', 'ethnicity_count', 'eyeColor_count', 'gender_count',
                'hairColor_count', 'identifier', 'inferlink_date', 'inferlink_text','inferlink_text_count','name_count',
                   'nationality_count', 'postalCode', 'price', 'price_count', 'price_per_hour', 'price_per_hour_count',
                   'readability_date', 'readability_text', 'readability_date_count', 'readability_text_count',
                   'relatedLink_count', 'review_id_count', 'review_site_count', 'seller.clusterMethod',
                   'seller.email.isObfuscated', 'seller.email.uri', 'seller.telephone.isObfuscated',
                   'seller.telephone.uri', 'seller.uri', 'serviceType_count', 'streetAddress_count',
                   'telephone.isObfuscated', 'telephone.uri', 'telephone_count', 'title_count', 'uri']
        raw = ['addressLocality', 'business_name', 'business_type', 'email.name', 'ethnicity', 'eyeColor', 'gender',
               'hairColor', 'name', 'nationality', 'relatedLink', 'review_id', 'review_site', 'seller.email.name',
               'seller.telephone.name', 'serviceType', 'streetAddress', 'telephone.name', 'title', 'url',
               'top_level_domain']
        appendRaw = list()
        for field in raw:
            appendRaw.append(field+'.raw')
        return non_raw+raw+appendRaw

    @staticmethod
    def get_mapping_for_index(elasticsearch_host='http://localhost:9200/', index_name='de-output-03-index', doc_type='ads', output_file=None):
        es = Elasticsearch(elasticsearch_host)
        mapping = es.indices.get_mapping(
            index=index_name,
            doc_type=doc_type)
        if output_file:
            out = codecs.open(output_file, 'w', 'utf-8')
            json.dump(mapping, out)
            out.close()
        else:
            pp = pprint.PrettyPrinter(indent=4)
            pp.pprint(mapping)


# MappingAnalyses.get_mapping_for_index(output_file='/Users/mayankkejriwal/datasets/memex-evaluation-november/mapping-groundtruth.json')
# memex_cp4_path = "/home/mayankkejriwal/Downloads/memex-cp4/"
# MappingAnalyses.print_dense_docs_sample("http://52.42.180.215:9200/","dig", "webpage",75,10)
# MappingAnalyses.docs_density_frequency_statistics("http://52.42.180.215:9200/","dig", "webpage",
#                                                   memex_cp4_path+'dig-index-cdf.txt')
# MappingAnalyses.missing_field_statistics("http://localhost:9200/","de-output-03-index", "ads") # doesn't work

