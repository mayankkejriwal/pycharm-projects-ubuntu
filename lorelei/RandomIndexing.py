import json
import codecs
from random import shuffle
import math
from sklearn.preprocessing import normalize
import numpy as np
import warnings

def _remove_high_freq_words(word_uuids_obj, freq):
    forbidden_words = set()
    for word, uuids in word_uuids_obj.items():
        if len(uuids) > freq:
            forbidden_words.add(word)
    print 'Number of forbidden words: ',
    print len(forbidden_words)
    for word in forbidden_words:
        del word_uuids_obj[word]


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


def _read_in_context_vectors(context_file):
    """
    :param context_file: e.g. context-vec.txt
    :return: context_vector_obj
    """
    context_vector_obj = dict()
    with codecs.open(context_file, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line)
            context_vector_obj[obj['uuid']] = obj['context_vector']

    return context_vector_obj


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


def _l2_norm_on_vec(vec):
    warnings.filterwarnings("ignore")
    k = np.reshape(vec, (1,-1))
    return normalize(k)[0]


def _sum_context_vectors(uuids, context_vectors_obj):
    """

    :param uuids: a list of uuids
    :param context_vectors_obj: a dictionary where a uuid will reference a context vector
    :return: return the sum of context vectors corresponding to uuids
    """
    if not uuids:
        return None
    sum = [0.0]*len(context_vectors_obj[uuids[0]])
    for uuid in uuids:
        context_vec = context_vectors_obj[uuid]
        for i in range(0, len(sum)):
            sum[i] = sum[i]+context_vec[i]
    return sum


@DeprecationWarning
def _l2_norm_on_word_vecs(word_vectors_obj):
    """
    Modified word_vectors_obj
    :param word_vectors_obj:
    :return: None
    """
    for k, v in word_vectors_obj.items():
      word_vectors_obj[k] = _l2_norm_on_vec(v)


def build_random_index_vectors(input_file, output_file, context_file, is_input_context = True, d=250,
                               non_zero_ratio=0.01, absolute_ignore_freq=120):
    """
    At present, I've designed this for the condensed file in reliefWebProcessed. We use the random
    indexing procedure outlined in  http://eprints.sics.se/221/1/RI_intro.pdf Current preprocessing
    in this function is limited to converting words to lowercase.

    I am also (l2) normalizing the word vectors. Context vectors remain unnormalized.

    Originally, I was going to have an output_word_file where each word's vector would be printed
    out in a separate JSON. The problem is that we can't do this in a space efficient way
    because there are tens of thousands (possibly much higher) of words.

    :param input_file: the condensed file. we will use the loreleiJSONMapping.wordcloud field, and
    lowercase all words in the field.
    :param output_file: The output file will contain a json on each line, with the uuid of the document
        referencing the vector for it. We do this by averaging over the word vectors in the document.
     :param context_file: contains a json per line, with each json containing uuid and context_vector
     keys. The values should be obvious.
    :param is_input_context: a boolean value that indicates whether the context file is an input file, from which the
    context should be read in, or an output file to which the context vectors must be written in, after generation.
    We have already generated context vectors numerous times, so I've set the default to true.
    :param d: the dimensionality of the random index vectors
    :param non_zero_ratio: the ratio of +1s and also -1s, that we distribute randomly through each vector.
    The fraction of 0's in each vector will be (1-2*ratio)
    :param absolute_ignore_freq: we ignore words that occur in more number of contexts than this.
    :return: None
    """
    # first, let's use input_file to write out output_context_file.
    if not is_input_context:    # we need to freshly generate the context-vectors
        context_vectors_obj = _build_context_vectors(input_file, context_file, d, non_zero_ratio)
    else:
        context_vectors_obj = _read_in_context_vectors(context_file)
    # word_vectors_obj = dict()
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
            # context_vec = context_vectors_obj[uuid]
            for word in words:
                if word not in word_uuids_obj:
                    word_uuids_obj[word] = list()
                word_uuids_obj[word].append(uuid)
            #     if word not in word_vectors_obj:
            #         word_vectors_obj[word] = list(context_vec)  # must be a deep copy!
            #     word_vectors_obj[word] = [x + y for x, y in zip(word_vectors_obj[word], context_vec)]
            # _l2_norm_on_word_vecs(word_vectors_obj)
    _remove_high_freq_words(word_uuids_obj, freq=absolute_ignore_freq)# freq must be absolute
    valid_words = set(word_uuids_obj.keys())
    out = codecs.open(output_file, 'w', 'utf-8')
    count = 0
    # word_vec_cache = dict()
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
            doc_vec = [0.0]*d
            denom = len(words)
            # print len(words)
            for word in words:
                if word not in valid_words:
                    continue
                # print word
                uuids = word_uuids_obj[word]
                word_vec = _l2_norm_on_vec(_sum_context_vectors(uuids, context_vectors_obj))
                for i in range(0, d):
                    doc_vec[i] += (word_vec[i])
            for i in range(0, d):
                doc_vec[i] = (doc_vec[i]/denom)
            answer = dict()
            answer[uuid] = doc_vec
            json.dump(answer, out)
            out.write('\n')
    out.close()

    # for word, uuids in word_uuids_obj.items():
    #     if count % 50 == 0:
    #         print 'processing word number : ',
    #         print count
    #     count += 1
    #     word_vec = _l2_norm_on_vec(_sum_context_vectors(uuids, context_vectors_obj))
    #     answer = dict()
    #     answer[word] = word_vec
    #     json.dump(answer, out)
    #     out.write('\n')
    # json.dump(word_vectors_obj, out, indent=4)
    # out.close()

#print _generate_random_sparse_vector(20, 0.05)
# path = '/home/mayankkejriwal/Downloads/lorelei/reliefWebProcessed-prepped/'
# build_random_index_vectors(path+'WCjaccard-10-10-condensed.json', path+'doc-vecs-wcjaccard-3000d.json',
#     path+'context-vecs-3000d.txt', is_input_context=True)