import json
import codecs
from random import shuffle
import math


def _preprocess_word_list(listOfWords):
    """
    At present, I am only converting to lower case
    :param listOfWords:
    :return: returns new list
    """
    answer = list()
    for word in listOfWords:
        answer.append(word.lower())
    return answer


# I've tested this method
def _generate_random_sparse_vector(d, non_zero_ratio):
    answer = [0]*d
    indices = [i for i in range(d)]
    shuffle(indices)
    k = int(non_zero_ratio*d)
    for i in range(0, k):
        answer[indices[i]] = 1
    for i in range(k, 2*k):
        answer[indices[i]] = -1
    return answer


def _build_context_vectors(input_file, output_context_file, d=3000, non_zero_ratio=0.05):
    """
    for parameters, see build_random_index_vectors
    :param input_file:
    :param output_context_file:
    :param d:
    :param non_zero_ratio:
    :return: a dictionary, with the uuid referencing the context_vector field
    """
    out = codecs.open(output_context_file, 'w', 'utf-8')
    output = dict()
    with codecs.open(input_file, 'r', 'utf-8') as f:
        for line in f:
            uuid = json.loads(line)['uuid']
            vec = _generate_random_sparse_vector(d, non_zero_ratio)
            answer = dict()
            answer['uuid'] = uuid
            answer['context_vector'] = vec
            output[uuid] = vec
            json.dump(answer, out)
            out.write('\n')
    out.close()
    return output


def _l2_norm_on_word_vecs(word_vectors_obj):
    """
    Modified word_vectors_obj
    :param word_vectors_obj:
    :return: None
    """
    for k, v in word_vectors_obj.items():
        total = 0
        for element in v:
            total += (element*element)
        new_vec = [math.sqrt(math.fabs(element)/total) for element in v]
        word_vectors_obj[k] = new_vec


def build_random_index_vectors(input_file, output_word_file, output_context_file, d=3000, non_zero_ratio=0.05):
    """
    At present, I've designed this for the condensed file in reliefWebProcessed. We use the random
    indexing procedure outlined in  http://eprints.sics.se/221/1/RI_intro.pdf

    I am also (l2) normalizing the word vectors. Context vectors remain unnormalized.

    :param input_file: the condensed file. we will use the loreleiJSONMapping.wordcloud field, and
    lowercase all words in the field.
    :param output_word_file: The output file will contain a single json where the key is a (lower-case) word, and the object
     is a list representing the d dimensional vector. Note that if new contexts are introduced in the future,
     these words should be updated!
     :param output_context_file: contains a json per line, with each json containing uuid and context_vector
     keys. The values should be obvious.
    :param d: the dimensionality of the random index vectors
    :param non_zero_ratio: the ratio of +1s and also -1s, that we distribute randomly through each vector.
    The fraction of 0's in each vector will be (1-2*ratio)
    :return: None
    """
    # first, let's use input_file to write out output_context_file.
    context_vectors_obj = _build_context_vectors(input_file, output_context_file, d, non_zero_ratio)
    word_vectors_obj = dict()
    word_uuids_obj = dict()
    print 'context calculations complete'
    count = 0
    with codecs.open(input_file, 'r', 'utf-8') as f:
        for line in f:
            print 'processing document : ',
            print count
            count += 1
            obj = json.loads(line)
            if not obj['loreleiJSONMapping.wordcloud']:
                print 'no wordcloud in object with uuid:'
                print obj['uuid']
                continue
            words = _preprocess_word_list(obj['loreleiJSONMapping.wordcloud'])
            uuid = obj['uuid']
            context_vec = context_vectors_obj[uuid]
            for word in words:
                if word not in word_uuids_obj:
                    word_uuids_obj[word] = list()
                word_uuids_obj.append(uuid)
            #     if word not in word_vectors_obj:
            #         word_vectors_obj[word] = list(context_vec)  # must be a deep copy!
            #     word_vectors_obj[word] = [x + y for x, y in zip(word_vectors_obj[word], context_vec)]
            # _l2_norm_on_word_vecs(word_vectors_obj)
    # out = codecs.open(output_word_file, 'w', 'utf-8')
    # json.dump(word_vectors_obj, out, indent=4)
    # out.close()

#print _generate_random_sparse_vector(20, 0.05)
path = '/home/mayankkejriwal/Downloads/lorelei/reliefWebProcessed-prepped/'
build_random_index_vectors(path+'condensed-objects.json', path+'word-vecs.json', path+'context-vecs.txt')