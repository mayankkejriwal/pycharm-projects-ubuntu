import codecs
import json

"""
We need to test whether the RandomIndexing vectors we generated for each doc are giving intuitively feasible results.
To that end, we implement a simple test
"""

def _read_doc_vectors(doc_vec_file):
    """

    :param doc_vec_file:
    :return:
    """
    doc_vecs = dict()
    with codecs.open(doc_vec_file, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line)
            for k, v in obj.items():
                doc_vecs[k] = v

    return doc_vecs


def _read_in_first_n_uuids(input_file, n=10):
    subject_uuids = dict()
    with codecs.open(input_file, 'r', 'utf-8') as f:
        count = 0
        for line in f:
            if count == 10:
                break
            subject_uuids[json.loads(line)['uuid']] = list()
            count += 1
    return subject_uuids


def _dot_product(vec1, vec2):
    if not vec1 or not vec2:
        raise Exception
    if len(vec1) != len(vec2):
        raise Exception
    score = 0.0
    for i in range(0, len(vec1)):
        score += (vec1[i]*vec2[i])
    return score


def _extract_top_k(scored_results_dict, k):
    count = 0
    results = list()
    scores = scored_results_dict.keys()
    scores.sort(reverse=True)
    for score in scores:
        vals = scored_results_dict[score]
        if count + len(vals) <= k:
            results += vals
            count += len(vals)
        else:
            results += vals[0: k - count]
    return results


def _find_knn_of_uuid(subject_uuid, doc_vecs, k):
    subject_vec = doc_vecs[subject_uuid]
    scored_results = dict() # a dictionary with the score as key, and a list as values. The list ensures determinism

    for object_uuid, object_vec in doc_vecs.items():
        if object_uuid == subject_uuid:
            continue
        score = _dot_product(subject_vec, object_vec)
        if score not in scored_results:
            scored_results[score] = list()
        scored_results[score].append(object_uuid)
    return _extract_top_k(scored_results, k)



def k_nearest_neighbors(input_file, doc_vec_file, output_file, reference_file=None, find_num=10, k=10):
    """
    Consider adding an explicit function parameter for the sim.
    :param input_file: the document file
    :param doc_vec_file: the file containing the document vectors
    :param output_file: the output file. Each line contains a json with a 'uuid' field, a 'ranked_list' field
    and a 'score_list' field. The latter two should be considered 'aligned'
    :param find_num: Will only read in the first n docs from input_file
    :param k: the number of nearest neighbors to retrieve based on dot product sim.
    :return: None
    """
    doc_vecs = _read_doc_vectors(doc_vec_file)
    subject_uuids = _read_in_first_n_uuids(input_file, find_num)
    count = 0
    out = codecs.open(output_file, 'w', 'utf-8')
    for uuid in subject_uuids:
        print 'Accessing object: '+str(count)
        knn = dict()
        knn[uuid] = _find_knn_of_uuid(uuid, doc_vecs, k)
        json.dump(knn, out)
        out.write('\n')
        count += 1
    out.close()

path = '/home/mayankkejriwal/Downloads/lorelei/reliefWebProcessed-prepped/'
k_nearest_neighbors(path+'WCjaccard-10-10-condensed.json', path+'doc-vecs-wcjaccard-250d.json',
                    path+'250d-10-nn-for-first-10-uuids.txt', 10, 10)