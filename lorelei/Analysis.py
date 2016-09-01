import codecs
import json
import pprint
import glob

class Analysis:
    """
    Use this for writing all the analytical stuff. Mainly exploratory code.
    """


    @staticmethod
    def countNumUniqueWordsInRWPWordClouds(RWPDir):
        """
        At present, I will print out number of unique words in the wordcloud (without
        any preprocessing). We break if we exceed 1 million.
        :param records_file:
        :return: None
        """
        listOfFiles = glob.glob(RWPDir+'*.json') # I've tested this; it works
        wordCloud = set()
        count = 1
        for f in listOfFiles:
            print count
            if len(wordCloud) > 1000000:
                break
            obj = None
            with codecs.open(f, 'r', 'utf-8') as openFile:
                obj = json.load(openFile)
            wordCloud = wordCloud.union(set(obj['loreleiJSONMapping']['wordcloud']))
            count += 1
        print 'Number of unique words in wordcloud: ',
        print len(wordCloud)


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


    @staticmethod
    def find_seeds_in_ebola_data(ebola_records, seeds_file):
        """
        Take one of two files (ebola_data/{freetown,westafrica}-uuids.txt and a records file and
        locate the objects containing the uuids. We want to be sure they all exist.If some uuid
        does not exist, it will print out that uuid.
        :param ebola_records:
        :param seeds_file:
        :return: None
        """
        #let's read in all those uuids first
        uuids = set()
        with codecs.open(seeds_file, 'r', 'utf-8') as f:
            for line in f:
                uuids.add(line[:-1])    # to get rid of the newline at the end

        with codecs.open(ebola_records, 'r', 'utf-8') as f:
            for line in f:
                uuid = json.loads(line)['uuid']
                if uuid in uuids:
                    uuids.discard(uuid)

        if not uuids:
            print 'all uuids found'
        else:
            print 'these uuids not found:',
            print uuids


# Analysis.countNumUniqueWordsInRWPWordClouds('/home/mayankkejriwal/Downloads/lorelei/reliefWebProcessed/')
# path = '/home/mayankkejriwal/Downloads/lorelei/ebola_data/'
# Analysis.find_seeds_in_ebola_data(path+'ebolaXFer.json', path+'westafrica-uuids.txt')
# Analysis.analyzeExactMatchBaseline(path+'record_dump_50000.json', path+'exactMatchOnRecordDump50000.json',
#                 ['california', 'ca'])