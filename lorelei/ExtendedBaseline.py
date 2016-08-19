import json
import codecs

"""
This file builds off of the results of ExactMatchBaseline. We currently implement two strategies: abbreviation expansion,
and common tokens subset. Other strategies, we will implement in a different file. Note however that the impl.
is very generic and is based on aliasing+set unions.
"""

def baseline(exactmatch_file, output_file):
    """
    :param exactmatch_file Must not be indented
    :param output_file
    :return:
    """
    exactmatch_records = dict()
    inverted_index = dict() # key is an 'alias', value is a set containing actual keys
    with codecs.open(exactmatch_file, 'r', 'utf-8') as f:
        for line in f:
            answer = json.loads(line)
            for k, v in answer.items():
                exactmatch_records[k] = v
                terms = generate_aliases(k) # guaranteed to contain k itself
                for term in terms:
                    if term not in inverted_index:
                        inverted_index[term] = set()
                    inverted_index[term].add(k)
    keys = list(inverted_index.keys())
    keys.sort()
    forbidden_indices = set()
    for i in range(0, len(keys)):
        if i in forbidden_indices:
            continue
        tmp_i = inverted_index[keys[i]]
        for j in range(0,len(keys)):
            if j == i or j in forbidden_indices:
                continue
            tmp_j = inverted_index[keys[j]]
            if tmp_i.intersection(tmp_j):
                tmp_i = tmp_i.union(tmp_j)
                forbidden_indices.add(j)
    for i in forbidden_indices:
        del inverted_index[keys[i]]

    out = codecs.open(output_file, 'w', 'utf-8')
    for r, v in inverted_index.items():
        answer = dict()
        for k in v:
            answer[k] = exactmatch_records[k]
        json.dump(answer, out)
        out.write('\n')
    out.close()


def generate_aliases(key):
    pass
