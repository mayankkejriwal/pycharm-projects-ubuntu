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


def find_most_similar(input_file, doc_vec_file, output_file, find_num=10, retrieval_num=10):
    """

    :param input_file: the document file
    :param doc_vec_file: the file containing the document vectors
    :param output_file: the output file. Each line contains a json with a 'uuid' field, a 'ranked_list' field
    and a 'score_list' field. The latter two should be considered 'aligned'
    :param find_num: Will only read in the first n docs from input_file
    :param retrieval_num: the number of nearest neighbors to retrieve based on dot product sim.
    :return: None
    """
    subject_uuids = dict()
    with codecs.open(input_file, 'r', 'utf-8') as f:
        for line in f:
            pass


