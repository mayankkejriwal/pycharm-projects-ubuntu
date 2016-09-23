# -*- coding: UTF-8 -*-
from random import shuffle
import codecs
import json
import TextAnalyses
import string
import kNearestNeighbors

class RandomIndexer:
    """
    Code for implementing the actual random indexing algorithm. Tokens lists and idfs must have been generated.
    """

    @staticmethod
    def read_in_tokens_file(tokens_list_file):
        answer = dict()
        with codecs.open(tokens_list_file, 'r', 'utf-8') as f:
            for line in f:
                obj = json.loads(line)
                for k, v in obj.items():
                    answer[k] = v
        return answer

    @staticmethod
    def _isnonascii_unicode(s):
        return not all(ord(c) < 128 for c in s)

    @staticmethod
    def _ispunct(s):
        return all(c in string.punctuation for c in s)

    @staticmethod
    def _isalphapunct(s):
        return all(c in string.punctuation or c.isalpha() for c in s)

    @staticmethod
    def _find_right_dummy_v2(token):
        """
        The versions should be aligned with _generate_context_vectors and with generate_unigram_embeddings
        :param token:
        :return: One of the dummy strings (see generate_context_vectors_)
        """

        if token.isdigit():
            return 'dummy_num'
        elif token.isalnum() and not token.isalpha() and not token.isdigit():
            return 'dummy_alpha_num'
        elif RandomIndexer._ispunct(token):
            return 'dummy_punct'
        elif RandomIndexer._isalphapunct(token) and not token.isalpha():
            return 'dummy_alpha_punct'
        elif RandomIndexer._isnonascii_unicode(token):
            return 'dummy_unicode'
        else:
            return 'dummy_idf' # it's just a rare word

    @staticmethod
    def _generate_random_sparse_vector(d, non_zero_ratio):
        """
        Suppose d =200 and the ratio is 0.01. Then there will be 2 +1s and 2 -1s and all the rest are 0s.

        Borrowed from lorelei. I've tested this method
        :param d:
        :param non_zero_ratio:
        :return: a vector with d dimensions
        """
        answer = [0]*d
        indices = [i for i in range(d)]
        shuffle(indices)
        k = int(non_zero_ratio*d)
        for i in range(0, k):
            answer[indices[i]] = 1
        for i in range(k, 2*k):
            answer[indices[i]] = -1
        return answer

    @staticmethod
    def _generate_context_vectors_for_idf(idf_dict, include_dummy, d, non_zero_ratio):
        """
        Generate context vectors
        :param idf_dict:
        :param include_dummy: If true, then we will append '' as a key in idf_dict, and generate a context
        vector for that as well.
        :param d:
        :param non_zero_ratio:
        :return: A dictionary with idf keys as keys, and a context vector as value.
        """
        context_dict = dict()
        for k in idf_dict.keys():
            context_dict[k] = RandomIndexer._generate_random_sparse_vector(d, non_zero_ratio)
        if include_dummy:
            print 'Don\'t forget, dummy is the empty string'
            if '' in context_dict:
                raise Exception
            else:
                context_dict[''] = RandomIndexer._generate_random_sparse_vector(d, non_zero_ratio)
        return context_dict

    @staticmethod
    def _generate_context_vectors_for_idf_v2(idf_dict, include_dummies, d, non_zero_ratio):
        """
        Generate context vectors. For info on the dummies, see notes.txt
        :param idf_dict:
        :param include_dummies: If true, then we will append several 'dummy' keys in idf_dict, and generate context
        vectors for those as well.
        :param d:
        :param non_zero_ratio:
        :return: A dictionary with idf keys as keys, and a context vector as value.
        """
        context_dict = dict()
        for k in idf_dict.keys():
            context_dict[k] = RandomIndexer._generate_random_sparse_vector(d, non_zero_ratio)
        if include_dummies:
            if 'dummy_punct' in context_dict:
                raise Exception
            else:
                context_dict['dummy_punct'] = RandomIndexer._generate_random_sparse_vector(d, non_zero_ratio)

            if 'dummy_alpha_num' in context_dict:
                raise Exception
            else:
                context_dict['dummy_alpha_num'] = RandomIndexer._generate_random_sparse_vector(d, non_zero_ratio)

            if 'dummy_alpha_punct' in context_dict:
                raise Exception
            else:
                context_dict['dummy_alpha_punct'] = RandomIndexer._generate_random_sparse_vector(d, non_zero_ratio)

            if 'dummy_idf' in context_dict:
                raise Exception
            else:
                context_dict['dummy_idf'] = RandomIndexer._generate_random_sparse_vector(d, non_zero_ratio)

            if 'dummy_num' in context_dict:
                raise Exception
            else:
                context_dict['dummy_num'] = RandomIndexer._generate_random_sparse_vector(d, non_zero_ratio)

            if 'dummy_unicode' in context_dict:
                raise Exception
            else:
                context_dict['dummy_unicode'] = RandomIndexer._generate_random_sparse_vector(d, non_zero_ratio)
        return context_dict

    @staticmethod
    def _init_unigram_embeddings(token_cvs):
        unigram_embeddings = dict()
        for k, v in token_cvs.items():
            # if k == '':  # don't forget to exclude the dummy value (if there)
            #     continue
            # else:
                unigram_embeddings[k] = list(v)  # deep copy of list
        return unigram_embeddings

    @staticmethod
    def generate_unigram_embeddings_v1(tokens_file, idf_file, output_file=None, context_window_size=2,
                                    include_dummy=True, d=200, non_zero_ratio=0.01):
        """
        We are keeping the lower_prune and upper_prune ratios in the TextAnalyses file. Feel free to change
        those directly. Consider adding weighting scheme in future. Right now, we assume constant weighting
        for the tokens in the context window

        This code does not assume the tokens_file is small enough to fit in memory. Note
        that idf_file data must be read into main memory at the present time.
        :param tokens_file:
        :param idf_file:
        :param output_file: each line will be a dict
        :param context_window_size:
        :param include_dummy:
        :param d:
        :param non_zero_ratio:
        :return:
        """
        idf_dict = TextAnalyses.TextAnalyses.read_in_and_prune_idf(idf_file)
        token_cvs = RandomIndexer._generate_context_vectors_for_idf(idf_dict, include_dummy, d, non_zero_ratio)
        # tokens_dict = RandomIndexer.read_in_tokens_file(tokens_file)
        unigram_embeddings = RandomIndexer._init_unigram_embeddings(token_cvs)
        count = 1
        with codecs.open(tokens_file, 'r', 'utf-8') as f:
            for line in f:
                obj = json.loads(line)
                v = None
                for k, val in obj.items():
                    v = val
                print 'In document '+str(count)
                count += 1
                if count>10000:
                    break
                for i in range(0, len(v)):
                    if v[i] not in token_cvs:
                        continue
                    vec = unigram_embeddings[v[i]]
                    min = i-context_window_size
                    if min<0:
                        min = 0
                    max = i+context_window_size
                    if max>len(v):
                        max = len(v)
                    for j in range(min, max):
                        if j == i:
                            continue
                        context_token = v[j]
                        if context_token not in token_cvs:
                            if include_dummy:
                                context_token = ''
                            else:
                                continue
                        for k in range(0, len(token_cvs[context_token])):
                            vec[k] += token_cvs[context_token][k]
                    unigram_embeddings[v[i]] = vec
        if output_file:
            out = codecs.open(output_file, 'w', 'utf-8')
            for k, v in unigram_embeddings.items():
                answer = dict()
                answer[k] = v
                json.dump(answer, out)
                out.write('\n')
            out.close()

    @staticmethod
    def generate_unigram_embeddings_twitter_v1(tokens_file, idf_file, output_file=None, context_window_size=2,
                                    include_dummy=False, d=20, non_zero_ratio=0.05):
        """

        :param tokens_file:
        :param idf_file:
        :param output_file:
        :param context_window_size:
        :param include_dummies:
        :param d:
        :param non_zero_ratio:
        :return:
        """
        idf_dict = TextAnalyses.TextAnalyses.read_in_and_prune_idf(idf_file, lower_prune_ratio=0.0)
        token_cvs = RandomIndexer._generate_context_vectors_for_idf(idf_dict, include_dummy, d, non_zero_ratio)
        # tokens_dict = RandomIndexer.read_in_tokens_file(tokens_file)
        unigram_embeddings = RandomIndexer._init_unigram_embeddings(token_cvs)
        count = 1
        with codecs.open(tokens_file, 'r', 'utf-8') as f:
            for line in f:
                obj = json.loads(line)
                v = None
                for k, val in obj.items():
                    v = val
                print 'In document '+str(count)
                count += 1
                # if count>10000:
                #     break
                for i in range(0, len(v)):
                    if v[i] not in token_cvs:
                        continue
                    vec = unigram_embeddings[v[i]]
                    min = i-context_window_size
                    if min<0:
                        min = 0
                    max = i+context_window_size
                    if max>len(v):
                        max = len(v)
                    for j in range(min, max):
                        if j == i:
                            continue
                        context_token = v[j]
                        if context_token not in token_cvs:
                            if include_dummy:
                                context_token = ''
                            else:
                                continue
                        for k in range(0, len(token_cvs[context_token])):
                            vec[k] += token_cvs[context_token][k]
                    unigram_embeddings[v[i]] = vec
        if output_file:
            out = codecs.open(output_file, 'w', 'utf-8')
            for k, v in unigram_embeddings.items():
                answer = dict()
                answer[k] = v
                json.dump(answer, out)
                out.write('\n')
            out.close()

    @staticmethod
    def generate_unigram_embeddings_v2(tokens_file, idf_file, output_file=None, context_window_size=2,
                                    include_dummies=True, d=200, non_zero_ratio=0.01):
        """
        We are keeping the lower_prune and upper_prune ratios in the TextAnalyses file. Feel free to change
        those directly. Consider adding weighting scheme in future. Right now, we assume constant weighting
        for the tokens in the context window

        This code does not assume the tokens_file is small enough to fit in memory. Note
        that idf_file data must be read into main memory at the present time.

        The difference between v1 and v2 is that we now accommodate more dummy variables. The goal is that
        we should have vector representations for every token (no matter how bad). For the dummies I've
        considered so far, consult notes.txt (Sept. 6th entry)
        :param tokens_file:
        :param idf_file:
        :param output_file: each line will be a dict
        :param context_window_size:
        :param include_dummies:
        :param d:
        :param non_zero_ratio:
        :return:
        """
        idf_dict = TextAnalyses.TextAnalyses.read_in_and_prune_idf(idf_file)
        token_cvs = RandomIndexer._generate_context_vectors_for_idf_v2(idf_dict, include_dummies, d, non_zero_ratio)
        # tokens_dict = RandomIndexer.read_in_tokens_file(tokens_file)
        unigram_embeddings = RandomIndexer._init_unigram_embeddings(token_cvs)
        count = 1
        with codecs.open(tokens_file, 'r', 'utf-8') as f:
            for line in f:
                obj = json.loads(line)
                v = None
                for k, val in obj.items():
                    v = val
                print 'In document '+str(count)
                count += 1
                if count>10000:
                    break
                for i in range(0, len(v)):
                    token = v[i]
                    if token not in token_cvs:
                        if include_dummies:
                            token = RandomIndexer._find_right_dummy_v2(token)
                            vec = unigram_embeddings[token]
                        else:
                            continue
                    else:
                        vec = unigram_embeddings[token]
                    min = i-context_window_size
                    if min<0:
                        min = 0
                    max = i+context_window_size
                    if max>len(v):
                        max = len(v)
                    for j in range(min, max):
                        if j == i:
                            continue
                        context_token = v[j]
                        if context_token not in token_cvs:
                            if include_dummies:
                                context_token = RandomIndexer._find_right_dummy_v2(context_token)
                            else:
                                continue
                        for k in range(0, len(token_cvs[context_token])):
                            vec[k] += token_cvs[context_token][k]
                    unigram_embeddings[token] = vec
        if output_file:
            out = codecs.open(output_file, 'w', 'utf-8')
            for k, v in unigram_embeddings.items():
                answer = dict()
                answer[k] = v
                json.dump(answer, out)
                out.write('\n')
            out.close()

    @staticmethod
    def generate_unigram_embeddings_v1_batch(tokens_file, idf_file, output_file=None, context_window_size=2,
                                    include_dummy=True, d=200, non_zero_ratio=0.01):
        """
        We are keeping the lower_prune and upper_prune ratios in the TextAnalyses file. Feel free to change
        those directly. Consider adding weighting scheme in future. Right now, we assume constant weighting
        for the tokens in the context window

        This is in batch mode because the entire input file (tokens_file) is read into main memory
        before processing is done. If this file is too big, please use the non-batch version. Note
        that idf_file data must be read into main memory at the present time.
        :param tokens_file:
        :param idf_file:
        :param output_file: each line will be a dict
        :param context_window_size:
        :param include_dummy:
        :param d:
        :param non_zero_ratio:
        :return:
        """
        idf_dict = TextAnalyses.TextAnalyses.read_in_and_prune_idf(idf_file)
        token_cvs = RandomIndexer._generate_context_vectors_for_idf(idf_dict, include_dummy, d, non_zero_ratio)
        tokens_dict = RandomIndexer.read_in_tokens_file(tokens_file)
        unigram_embeddings = RandomIndexer._init_unigram_embeddings(token_cvs)
        count = 1
        for v in tokens_dict.itervalues():
            print 'In document '+str(count)
            count += 1
            for i in range(0, len(v)):
                if v[i] not in token_cvs:
                    continue
                vec = unigram_embeddings[v[i]]
                min = i-context_window_size
                if min<0:
                    min = 0
                max = i+context_window_size
                if max>len(v):
                    max = len(v)
                for j in range(min, max):
                    if j == i:
                        continue
                    context_token = v[j]
                    if context_token not in token_cvs:
                        if include_dummy:
                            context_token = ''
                        else:
                            continue
                    for k in range(0, len(token_cvs[context_token])):
                        vec[k] += token_cvs[context_token][k]
                unigram_embeddings[v[i]] = vec
        if output_file:
            out = codecs.open(output_file, 'w', 'utf-8')
            for k, v in unigram_embeddings.items():
                answer = dict()
                answer[k] = v
                json.dump(answer, out)
                out.write('\n')
            out.close()

    @staticmethod
    def generate_telephone_embeddings_v1(ground_truth_corpus, embeddings_file, output_file, context_field = 'location'):
        """
        We generate telephone embeddings based on the tokens in location. Note that there can be
        multiple telephone numbers per object, and they may not all contain locations. I have verified that
        'phone' is always a list, and 'location' is always a string. This will make life easier.
        :param ground_truth_corpus: we intend this to be corpora/ground-truth-corpus.json

        At present, there is no notion of dummies or idf. I will only use vectors if we generated them
        in previous embeddings.
        :param output_file: where the embeddings get written
        :param context_field: whichever field should be tokenized and then used as context
        :param embeddings_file: where the original embeddings were generated
        :return: None
        """
        full_embeddings = kNearestNeighbors.read_in_embeddings(embeddings_file)
        with codecs.open(ground_truth_corpus, 'r', 'utf-8') as f:
            pass

# str = 'bødy'
# print str.isalpha()
# print RandomIndexer._find_right_dummy_v2('..,')
# path = '/home/mayankkejriwal/Downloads/lorelei/ebola_data/'
# RandomIndexer.generate_unigram_embeddings_twitter_v1(path+'tokens/ebolaXFer_lowerCase.json',
#                         path+'tokens/ebolaXFer_lowerCase_df.txt',
#               path+'embedding/unigram-embeddings-v2.json')

