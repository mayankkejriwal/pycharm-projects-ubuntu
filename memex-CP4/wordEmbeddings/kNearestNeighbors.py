import codecs
import json
import math
import SimFunctions


def _extract_top_k(scored_results_dict, k, disable_k=False, reverse=True):
    count = 0
    # print k
    results = list()
    scores = scored_results_dict.keys()
    scores.sort(reverse=reverse)
    for score in scores:
        # print score
        # print count
        if count >= k:
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


def find_k_nearest_neighbors(embeddings_file, seed_token, k=10):
    """

    :param embeddings_file: e.g.
    :param seed_token: some token that must occur in the embeddings_file
    :param k:
    :return: None
    """
    unigram_embeddings = read_in_embeddings(embeddings_file)
    scored_dict = _generate_scored_dict(unigram_embeddings, seed_token)
    print _extract_top_k(scored_dict, 10, False)


# path = '/home/mayankkejriwal/Downloads/memex-cp4-october/'
# find_k_nearest_neighbors(path+'unigram-embeddings.json', 'ebony')