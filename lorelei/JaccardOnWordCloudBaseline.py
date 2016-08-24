import codecs, json, RandomIndexing, TestRandomIndexing

"""
Do not mistake this with the ExactMatchBaseline; this is for situation frame linking on ReliefWebProcessed, not
mention linking.
"""


def _jaccard_on_word_cloud(word_cloud1, word_cloud2):
    """
    We will do minimal preprocessing: convert words to lower-case.
    :param word_cloud1: list of words
    :param word_cloud2: list of words
    :return: similarity score in [0,1]
    """
    return len(set(word_cloud1).intersection(set(word_cloud2)))*1.0/len(set(word_cloud1).union(set(word_cloud2)))


def _find_jaccard_knn(subject_index, word_cloud_list, k, disable_k):
    subject_words = word_cloud_list[subject_index]
    scored_results = dict() # a dictionary with the score as key, and a list as values. The list ensures determinism
    for i in range(0, len(word_cloud_list)):
        if i == subject_index:
            continue
        score = _jaccard_on_word_cloud(subject_words, word_cloud_list[i])
        if score not in scored_results:
            scored_results[score] = list()
        scored_results[score].append(i)
    return TestRandomIndexing._extract_top_k(scored_results, k, disable_k)


def _derive_list_of_objects(RWPDirPath, uuid_list, knn_indexes):
    list_of_objects = list()
    for i in range(0, len(knn_indexes)):
        with codecs.open(RWPDirPath+'data_'+uuid_list[knn_indexes[i]]+'.json', 'r', 'utf-8') as f:
            list_of_objects.append(json.load(f))
    return list_of_objects


def run_k_nearest_jaccard_baseline(input_file, output_file,
                                   RWPDirPath='/home/mayankkejriwal/Downloads/lorelei/reliefWebProcessed/',
                                   find_num=10, k=10,
                                   disable_k = False):
    """

    :param input_file: the document file
    :param output_file: each line contains a json. the json contains a 'subject' field which contains the original
     document,
     and a 'ranked_list' of k jsons with each json corresponding to a 'matching' document. We do not record scores.
     In total, output_file contains find_num lines.
     :param RWPDirPath the directory where the original jsons are stored
    :param find_num: number of objects to consider
    :param k: the number of nearest neighbors to find
    :param disable_k: if True, k will not be considered, instead we employ 'all' semantics.
    :return: None
    """
    word_cloud_list = list()
    uuid_list = list()
    with codecs.open(input_file, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line)
            if not obj['loreleiJSONMapping.wordcloud']:
                print 'no wordcloud in object with uuid:'
                print obj['uuid']
                continue
            words = RandomIndexing._preprocess_word_list(obj['loreleiJSONMapping.wordcloud'])
            word_cloud_list.append(words)
            uuid_list.append(obj['uuid'])
    out = codecs.open(output_file, 'w', 'utf-8')
    for i in range(0, find_num):
        knn_indexes = _find_jaccard_knn(i, word_cloud_list, k, disable_k)
        answer = dict()
        with codecs.open(RWPDirPath+'data_'+uuid_list[i]+'.json', 'r', 'utf-8') as f:
            answer['subject'] = json.load(f)
        answer['ranked_list'] = _derive_list_of_objects(RWPDirPath, uuid_list, knn_indexes)
        json.dump(answer, out)
        out.write('\n')
    out.close()


# path = '/home/mayankkejriwal/Downloads/lorelei/reliefWebProcessed-prepped/'
# run_k_nearest_jaccard_baseline(path+'condensed-objects.json',
#                                path+'WCjaccard-10-nn-for-first-10-uuids-FULL-nonindent.txt')