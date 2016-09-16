import codecs
import json
import JaccardOnWordCloudBaseline
import TestRandomIndexing
import math
import WordCloudPreprocessor

"""
Primarily for evaluating the results of TestRandomIndexing, JaccardOnWordCloudBaseline and any other baselines (e.g. tf-idf)
that we will use for forming clusters.
"""


def  _derive_list_of_objects(RWPDirPath, list_of_uuids):
    """
    Based closely, but not equivalent to, on Jaccard..._derive...
    :param RWPDirPath:
    :param list_of_uuids: a list of uuids
    :return: a list of objects
    """
    list_of_objects = list()
    for uuid in list_of_uuids:
        with codecs.open(RWPDirPath+'data_'+uuid+'.json', 'r', 'utf-8') as f:
            list_of_objects.append(json.load(f))
    return list_of_objects


def expand_ranked_uuid_file(ranked_uuid_file, output_file,
                            RWPDirPath='/home/mayankkejriwal/Downloads/lorelei/reliefWebProcessed/'):
    """

    :param ranked_uuid_file: e.g. 250d-10-nn-for-first-10-uuids.txt
    :param output_file: e.g. something like WCjaccard-10-nn-for-first-10-uuids-FULL.txt
    :return: None
    """
    out = codecs.open(output_file, 'w', 'utf-8')
    with codecs.open(ranked_uuid_file, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line)
            answer = dict()
            for k, v in obj.items():
                answer['ranked_list'] = _derive_list_of_objects(RWPDirPath, v)
                answer['subject'] = _derive_list_of_objects(RWPDirPath, [k])[0]
            json.dump(answer, out, indent=4)
            out.write('\n')
    out.close()


def calculate_reciprocal_rank(WCJaccard_file, input_file):
    """
    Will evaluate input_file against the WCjaccard file (the 'ground-truth'). Be careful about the
    format: both files are different. The specific protocol is to calculate the reciprocal rank of
    the first relevant result in input_file/uuid. These will get printed out to console.
    :param WCJaccard_file: e.g. WCjaccard-10-nn-for-first-10-uuids-FULL-nonindent.txt
    :param input_file: e.g. 250d-all-nn-for-ref-uuids.txt
    :return: None
    """
    ground_truth = dict() # uuid references a SET of uuids
    with codecs.open(WCJaccard_file, 'r', 'utf-8') as f:
        for line in f:
            big_obj = json.loads(line)
            answer = set()
            for element in big_obj['ranked_list']:
                answer.add(element['uuid'])
            ground_truth[big_obj['subject']['uuid']] = answer
    print 'ref. uuid,found uuid,rank,reciprocal rank'
    with codecs.open(input_file, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line)
            for k, v in obj.items():
                for i in range(0, len(v)):
                    if v[i] in ground_truth[k]:
                        print k+','+v[i]+','+str(i+1)+','+str(1.0/(i+1))
                        break


def _find_jaccard_knn(subject_uuid, tweet_dicts, k=10, disable_k=False):
    """
    This code is slightly different from the one in JaccardOnWordCloudBaseline
    :param subject_uuid: a uuid
    :param tweet_dicts: a dictionary of objects, with uuids as keys. The dict can contain the 'subject_uuid' object also
    :param k:
    :param disable_k:
    :return: A ranked list of uuids
    """

    subject_words = tweet_dicts[subject_uuid]['loreleiJSONMapping.wordcloud']
    scored_results = dict() # a dictionary with the score as key, and a list as values. The list ensures determinism
    for uuid,tweet in tweet_dicts.items():
        if uuid == subject_uuid:
            continue
        score = JaccardOnWordCloudBaseline._jaccard_on_word_cloud(subject_words, tweet['loreleiJSONMapping.wordcloud'])
        if score not in scored_results:
            scored_results[score] = list()
        scored_results[score].append(uuid)
    return TestRandomIndexing._extract_top_k(scored_results, k, disable_k)


def compute_mean_reciprocal_rank(relevant_set, list_of_ranked_lists):
    """

    :param relevant_set: a set of items (e.g. uuids)
    :param list_of_ranked_lists: a list of list of items (e.g. uuids)
    :return: a float that is the mrr
    """
    mrr = 0.0
    for ranked_list in list_of_ranked_lists:
        for i in range(0, len(ranked_list)):
            if ranked_list[i] in relevant_set:
                # print ranked_list[i]
                mrr += (1.0/(i+1))
                break
    return mrr/len(list_of_ranked_lists)


def print_ranked_results_statistics(relevant_set, list_of_ranked_lists):
    """
    Prints out the number of relevant entries, a vector containing ranks (starting from 1) of relevant docs
    per relevant entry, and the length of that vector.
    :param relevant_set: a set of items (e.g. uuids)
    :param list_of_ranked_lists: a list of list of items (e.g. uuids)
    :return: None
    """
    print 'number of relevant entries is : ',
    print len(relevant_set)
    for ranked_list in list_of_ranked_lists:
        vec = list()
        for i in range(0, len(ranked_list)):
            if ranked_list[i] in relevant_set:
                vec.append(i+1)
        print 'number of relevant entries in list: ',
        print len(vec)
        print vec



def compute_avg_dcgAtk(relevant_set, list_of_ranked_lists):
    """
    Unlike MRR, DCG in this function does not have a 'natural' bound between 0 and 1. Thus, only use it
     to compare across results.
    :param relevant_set: a set of items (e.g. uuids)
    :param list_of_ranked_lists: a list of list of items (e.g. uuids)
    :return: a float that is the avg DCG till the kth document (that is, if any relevant document has rank greater
    than k, we will make it 0)
    """
    dcg = 0.0
    for ranked_list in list_of_ranked_lists:
        if ranked_list[i] in relevant_set:
            dcg += 1.0
        for i in range(1, len(ranked_list)):
            if ranked_list[i] in relevant_set:
                # print ranked_list[i]
                dcg += (1.0/math.log(i+1, 2))
                # break
    return dcg/len(list_of_ranked_lists)


def analyze_condensed_tweets(condensed_tweets, reference_uuids, k=100):
    """
    Will take a condensed_tweets_file and possibly other reference files, and run a variety of analyses. I will
    comment in the code on each level of analysis. Will print stuff out. Also convert wordcloud to lowercase when
    processing.
    :return: None
    """
    tweets_dict = dict()
    uuids_set = set()
    with codecs.open(condensed_tweets, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line)
            WordCloudPreprocessor.WordCloudPreprocessor.convert_list_to_lower(obj['loreleiJSONMapping.wordcloud'])
            tweets_dict[obj['uuid']] = obj
    with open(reference_uuids, 'r') as f:
        for line in f:
            uuids_set.add(line[0:-1])
    list_of_ranked_lists = list()
    forbidden = set()
    for uuid in uuids_set:
        if uuid in tweets_dict:
            ranked_list = _find_jaccard_knn(uuid, tweets_dict, k=k)
            print ranked_list[0:10]
            list_of_ranked_lists.append(ranked_list)
        else:
            forbidden.add(uuid)
    print_ranked_results_statistics(uuids_set.difference(forbidden), list_of_ranked_lists)
    #print compute_mean_reciprocal_rank(uuids_set, list_of_ranked_lists)



# path = '/home/mayankkejriwal/Downloads/lorelei/ebola_data/'
# analyze_condensed_tweets(path+'ebolaXFer-condensed.json', path+'freetown-uuids.txt')
# calculate_reciprocal_rank(path+'WCjaccard-10-nn-for-first-10-uuids-FULL-nonindent.txt',
#                           path+'3000d-all-nn-for-ref-uuids.txt')


