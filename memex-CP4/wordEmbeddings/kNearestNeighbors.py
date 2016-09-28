import codecs
import json
import math
import SimFunctions
import pprint


def _extract_top_k(scored_results_dict, k, disable_k=False, reverse=True):
    """

    :param scored_results_dict: a score always references a list
    :param k: Max. size of returned list.
    :param disable_k: ignore k, and sort the list by k
    :param reverse: if reverse is true, the top k will be the highest scoring k. If reverse is false,
    top k will be the lowest scoring k.
    :return:
    """
    count = 0
    # print k
    results = list()
    scores = scored_results_dict.keys()
    scores.sort(reverse=reverse)
    for score in scores:
        # print score
        # print count
        if count >= k and not disable_k:
            break
        vals = scored_results_dict[score]
        if disable_k:
            results += vals
            continue
        if count + len(vals) <= k:
            results += vals
            count = len(results)
        else:
            results += vals[0: k - count]
            count = len(results)
    # print results[0]
    return results


def _compute_abs_cosine_sim(vector1, vector2):
    return SimFunctions.SimFunctions.abs_cosine_sim(vector1, vector2)


def _generate_scored_dict(unigram_embeddings, seed_token):
    scored_dict = dict()
    seed_vector = unigram_embeddings[seed_token]
    for token, vector in unigram_embeddings.items():
        if token == seed_token:
            continue
        else:
            score = _compute_abs_cosine_sim(seed_vector, vector)
            if score not in scored_dict:
                scored_dict[score] = list()
            scored_dict[score].append(token)
    return scored_dict


def read_in_embeddings(embeddings_file):
    unigram_embeddings = dict()
    with codecs.open(embeddings_file, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line)
            for k, v in obj.items():
                unigram_embeddings[k] = v
    return unigram_embeddings


@DeprecationWarning
def find_k_nearest_neighbors(embeddings_file, seed_token, k=10):
    """

    :param embeddings_file: e.g.
    :param seed_token: some token that must occur in the embeddings_file
    :param k:
    :return: None
    """
    unigram_embeddings = read_in_embeddings(embeddings_file)
    scored_dict = _generate_scored_dict(unigram_embeddings, seed_token)
    print _extract_top_k(scored_dict, k=k, disable_k=False)


def find_k_nearest_neighbors_multi(embeddings_file, seed_tokens, k=10, suppress_print=True):
    """

    :param embeddings_file: e.g.
    :param seed_tokens: a set of tokens that must occur in the embeddings_file
    :param k:
    :param suppress_print
    :return: None
    """
    results = dict()
    unigram_embeddings = read_in_embeddings(embeddings_file)

    # let's remove the dummies.
    del unigram_embeddings['dummy_punct']
    del unigram_embeddings['dummy_alpha_num']
    del unigram_embeddings['dummy_alpha_punct']
    del unigram_embeddings['dummy_idf']
    del unigram_embeddings['dummy_num']
    del unigram_embeddings['dummy_unicode']

    for seed_token in seed_tokens:
        scored_dict = _generate_scored_dict(unigram_embeddings, seed_token)
        results[seed_token]=_extract_top_k(scored_dict, k=k, disable_k=False)
        if not suppress_print:
            print 'seed_token: ',
            print seed_token
            print results[seed_token]
            print '\n'
    return results


def _reverse_dict(dictionary):
        """
        Borrowed from FieldAnalyses.
        Turn keys into (lists of) values, and values into keys. Values must originally be primitive.
        :param dictionary:
        :return: Another dictionary
        """
        new_dict = dict()
        for k, v in dictionary.items():
            if v not in new_dict:
                new_dict[v] = list()
            new_dict[v].append(k)
        return new_dict


def prune_low_scores_from_score_dict(score_dict, threshold):
    """
    Modifies score_dict
    :param score_dict:
    :param threshold: any entries (strictly) below this threshold get deleted
    :return: None
    """
    k = score_dict.keys()
    for score in k:
        if score < threshold:
            del score_dict[score]


def supplement_dictionary_v1(input_dictionary_file, embeddings_file, output_dictionary_file, threshold=5, k=20):
    """
    It's okay if there are words in the dictionary file that are not in the embeddings file. See threshold
    description below.
    :param dictionary_file:
    :param embeddings_file:
    :param threshold: a word must show up at least this many top-k lists generated for the words in
     the dictionary file.
    :return: None
    """
    seed_tokens = list()
    all_seed_tokens = set()
    word_embeddings = read_in_embeddings(embeddings_file).keys()
    total_seed_tokens = 0
    with codecs.open(input_dictionary_file, 'r', 'utf-8') as f:
        for line in f:
            line = line.lower()
            token = line[0:-1]
            all_seed_tokens.add(token)
            if token in word_embeddings:
                seed_tokens.append(token)
            total_seed_tokens += 1
    print 'number of total seed tokens in dictionary: ',
    print total_seed_tokens
    print 'number of seed tokens in dictionary that are also in embeddings: ',
    print len(seed_tokens)
    del word_embeddings
    knn_multi_dict = find_k_nearest_neighbors_multi(embeddings_file, seed_tokens, k=k)
    dict_words = set(_get_knn_multi_dict_counts(knn_multi_dict, prune=threshold).keys())
    dict_words = dict_words.union(all_seed_tokens)
    out = codecs.open(output_dictionary_file, 'w', 'utf-8')
    for word in dict_words:
        out.write(word)
        out.write('\n')
    out.close()


def _get_knn_multi_dict_counts(knn_multi_dict, prune=5):
    """

    :param knn_multi_dict: The dictionary generated by find_k_nearest_neighbors_multi
    :param prune: any items with count less than this will be pruned out and not returned. Set to 0 to disable
    :return: A word/count dictionary
    """
    result = dict()
    to_remove = set()
    for v in knn_multi_dict.values():
        for word in v:
            if word not in result:
                result[word] = 0
                to_remove.add(word)
            result[word] += 1
            if result[word] >= prune:
                to_remove.discard(word)

    for word in to_remove:
        del result[word]

    return result


@DeprecationWarning
def supplement_dictionary_v0(dictionary_file, embeddings_file, k=20):
    """
    At present dictionary_file must only contain words that are in embeddings, otherwise I'll raise an exception
    Will print a list of words that should be included but aren't, in increasing order of probability.
    :param dictionary_file:
    :param embeddings_file:
    :param k:
    :return: None
    """
    seed_tokens = list()
    with codecs.open(dictionary_file, 'r', 'utf-8') as f:
        for line in f:
            seed_tokens.append(line[0:-1])

    knn_multi_dict = find_k_nearest_neighbors_multi(embeddings_file, seed_tokens, k=k)
    results = dict()  # only contains tokens that do not occur in dictionary_file
    seed_tokens = set(seed_tokens)
    for v in knn_multi_dict.values():
        for i in range(0, len(v)):
            val = v[i]
            if val in seed_tokens:
                continue
            if val not in results:
                results[val] = 0
            results[val] += (len(v)-i)
    score_dict = _reverse_dict(results)
    prune_low_scores_from_score_dict(score_dict, threshold=200)
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(_extract_top_k(score_dict, k=0, disable_k=True))

#
# path = '/Users/mayankkejriwal/ubuntu-vm-stuff/home/mayankkejriwal/Downloads/memex-cp4-october/'
# tmp_path = '/Users/mayankkejriwal/ubuntu-vm-stuff/home/mayankkejriwal/tmp/'
# RWP_path = '/Users/mayankkejriwal/ubuntu-vm-stuff/home/mayankkejriwal/Downloads/lorelei/reliefWebProcessed-prepped/embedding/'
# find_k_nearest_neighbors_multi(RWP_path+'unigram-embedding-v2.json', ['war', 'flood', 'disaster'], False)
# supplement_dictionary_v1(path+'dictionary-supervised/names.txt',tmp_path+'unigram-part-00000-v2.json',tmp_path+'supplemented-names.txt')