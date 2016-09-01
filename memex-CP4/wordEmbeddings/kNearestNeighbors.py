import codecs
import json
import math


def _extract_top_k(scored_results_dict, k, disable_k):
    count = 0
    results = list()
    scores = scored_results_dict.keys()
    scores.sort(reverse=True)
    for score in scores:
        vals = scored_results_dict[score]
        if disable_k:
            results += vals
            continue
        if count + len(vals) <= k:
            results += vals
            count += len(vals)
        else:
            results += vals[0: k - count]
    return results


def _compute_abs_cosine_sim(vector1, vector2):
    if len(vector1) != len(vector2):
        raise Exception
    total1 = 0.0
    total2 = 0.0
    sim = 0.0
    for i in range(0, len(vector1)):
        sim += (vector1[i]*vector2[i])
        total1 += (vector1[i]*vector1[i])
        total2 += (vector2[i]*vector2[i])
    total1 = math.sqrt(total1)
    total2 = math.sqrt(total2)
    if total1 == 0.0 or total2 == 0.0:
        print 'divide by zero problem. Returning 0.0'
        return 0.0
    else:
        return math.fabs(sim/(total1*total2))


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