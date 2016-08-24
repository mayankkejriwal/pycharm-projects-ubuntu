import json
import codecs
import re
import sys

"""
This file builds off of the results of ExactMatchBaseline. We currently implement only the common tokens subset strategy.
Other strategies, we will implement in a different file. I decided to leave off on abbr. expansion since they
are mostly useful for places, and we have advanced modules for those.
(1) The first try was based on aliasing+set unions. It quickly ran into problems.
(2) The second try was more successful.
"""


def extended_baseline_v1(exactmatch_file, output_file):
    """
    Uses is_match_v1 and naive quadratic matching.
    :param exactmatch_file: Must not be indented
    :param output_file:
    :return: None
    """
    exactmatch_records = list()
    with codecs.open(exactmatch_file, 'r', 'utf-8') as f:
        for line in f:
           exactmatch_records.append(json.loads(line))

    exactmatch_records = _key_sort(exactmatch_records) # this is an important step.

    # let's just to do an exhaustive pairwise run now.
    # Note that we assume transitivity and symmetry in our match algorithm.
    answer = list()
    forbidden_indices = set()
    for i in range(0, len(exactmatch_records)-1):
        if i in forbidden_indices:
            continue
        tmp_dict = dict()
        mention1 = exactmatch_records[i].keys()[0]  # there must be exactly one key for this to work correctly.
        tmp_dict[mention1] = exactmatch_records[i][mention1]
        for j in range(i+1, len(exactmatch_records)):
            if j in forbidden_indices:
                continue
            mention2 = exactmatch_records[j].keys()[0]
            if (len(mention1) <= len(mention2) and is_match_v1(mention1, mention2)) or \
                    (len(mention1) > len(mention2) and is_match_v1(mention2, mention1)):
                tmp_dict[mention2] = exactmatch_records[j][mention2]
                forbidden_indices.add(j)
        answer.append(tmp_dict)

    out = codecs.open(output_file, 'w', 'utf-8')
    for a in answer:
        json.dump(a, out)
        out.write('\n')
    out.close()


def _key_sort(listOfDicts):
    """
    Will be descending order
    :param listOfDicts:
    :return: Returns new list
    """
    answer = list()
    keys = list()
    dictOfDicts = dict()
    for dic in listOfDicts:
        keys.append(dic.keys()[0])
        dictOfDicts[dic.keys()[0]] = dic
    keys.sort()
    for i in range(0, len(keys)):
        key = keys[(len(keys)-1) - i]
        answer.append(dictOfDicts[key])
    return answer


def is_match_v1(mention1, mention2):
    """
    Consider filling this out in the future
    len(mention1) must be less than equal to len(mention2). At present we:
    (1) check if one of the mentions is an exact token-subset of the other (assuming only space as token delimiter)
    :param mention1: just a surface string
    :param mention2: just a surface string
    :return: True if matching, else False
    """
    if len(mention1) > len(mention2):
        raise Exception
    regex = ' '
    tokens = re.split(regex, mention2)
    # initials = ''
    # dot_initials = ''
    for token in tokens:
        if token == '':
            break
        elif mention1 == token:  # sub-token match.
            return True
        # else:
        #     initials += token[0]
        #     dot_initials += (token[0]+'.')
    # abbreviation expansion. We assume the abbr. is of the form 'u.s.' or 'us' for example
    # if initials != '' and len(initials) > 1:
    #     if initials == mention1:
    #         return True
    # if dot_initials != '' and len(dot_initials) > 1:
    #     if dot_initials == mention1:
    #         return True
    return False  # nothing matches, sorry


@DeprecationWarning
def baseline_try1(exactmatch_file, output_file):
    """
    :param exactmatch_file: Must not be indented
    :param output_file:
    :return: None
    """
    exactmatch_records = dict()
    inverted_index = dict() # key is an 'alias', value is a set containing actual keys
    with codecs.open(exactmatch_file, 'r', 'utf-8') as f:
        for line in f:
            answer = json.loads(line)
            for k, v in answer.items():
                exactmatch_records[k] = v
                terms = _generate_aliases(k) # guaranteed to contain k itself
                for term in terms:
                    if term not in inverted_index:
                        inverted_index[term] = set()
                    inverted_index[term].add(k)
    # print inverted_index['gandhi']
    keys = list(inverted_index.keys())
    keys.sort()
    forbidden_indices = set()
    for i in range(0, len(keys)):
        if i in forbidden_indices:
            continue
        tmp_i = inverted_index[keys[i]]
        for j in range(0, len(keys)):
            if j == i or j in forbidden_indices:
                continue
            tmp_j = inverted_index[keys[j]]
            if tmp_i.intersection(tmp_j):
                # if 'gandhi' in tmp_i:
                #     print tmp_i
                #     sys.exit()
                tmp_i = tmp_i.union(tmp_j)

                forbidden_indices.add(j)
        inverted_index[keys[i]] = tmp_i

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

@DeprecationWarning
def _generate_aliases(key):
    """
    E.g. 'united states of america' generates
    set(['united', 'usoa', 'of', 'united states of america', 'states', 'america'])
    :param key: just the surface string
    :return: a set of aliases
    """
    answer = set()
    answer.add(key)
    regex = '\.| '
    tokens = re.split(regex, key)
    initials = ''
    for token in tokens:
        if token == '':
            break
        else:
            answer.add(token)
            initials += token[0]
    # if initials != '' and len(initials) > 1:
    #     answer.add(initials)
    return answer

#print re.split('\.| ','u.')
#print generate_aliases('united states of america')
# path = '/home/mayankkejriwal/Downloads/lorelei/ebola_data/'
# extended_baseline_v1(path+'exactMatchOnRecordDump50000.json', path+'tokenSubsetOnExactMatch50000.json')
#print 'united states' > 'z'