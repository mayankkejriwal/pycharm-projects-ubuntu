import codecs
import json
import pprint

class Analysis:
    """
    Use this for writing all the analytical stuff. Mainly exploratory code.
    """



    @staticmethod
    def analyzeExactMatchBaseline(records_file, results_file, keys):
        """
        Exploratory code. Typically used for ranges of keys, but you can also specify your own.
        :param records_file: The (unindented) records file
        :param results_file: the results file of ExactMatchBaseline
        :param key_min:
        :param key_max
        :param keys: a list of mentions (case matters at present, so only input in lowercase keys).
        :return: None
        """
        results = dict()
        records = dict()
        with codecs.open(results_file, 'r', 'utf-8') as f:
            for line in f:
                answer = json.loads(line)
                for k, v in answer.items():
                    results[k] = v

        with codecs.open(records_file, 'r', 'utf-8') as f:
            for line in f:
                answer = json.loads(line)
                records[answer['uuid']] = answer
        #At present, print only user and translated text
        pp = pprint.PrettyPrinter(indent=4)
        for k in keys:
            print '***'
            print k
            print '***'
            if k in results:
                uuids = results[k]
                for uuid in uuids:
                    if uuid in records:

                        print('user.screenName: '+records[uuid]['loreleiJSONMapping']['status']['user']['screenName'])
                        print('originalText: '+records[uuid]['loreleiJSONMapping']['originalText'])
                        if 'translatedText' in records[uuid]['loreleiJSONMapping']:
                            print('translatedText: '+ records[uuid]['loreleiJSONMapping']['translatedText'])

path = '/home/mayankkejriwal/Downloads/lorelei/ebola_data/'
Analysis.analyzeExactMatchBaseline(path+'record_dump_50000.json', path+'exactMatchOnRecordDump50000.json',
                ['california', 'ca'])