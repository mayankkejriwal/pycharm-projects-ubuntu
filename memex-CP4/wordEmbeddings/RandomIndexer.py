from random import shuffle
import codecs
import json
import TextAnalyses


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
    def _init_unigram_embeddings(token_cvs):
        unigram_embeddings = dict()
        for k, v in token_cvs.items():
            if k == '':  # don't forget to exclude the dummy value (if there)
                continue
            else:
                unigram_embeddings[k] = list(v)  # deep copy of list
        return unigram_embeddings

    @staticmethod
    def generate_unigram_embeddings(tokens_file, idf_file, output_file=None, context_window_size=2,
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
    def generate_unigram_embeddings_batch(tokens_file, idf_file, output_file=None, context_window_size=2,
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


# path = '/home/mayankkejriwal/Downloads/memex-cp4-october/'
# RandomIndexer.generate_unigram_embeddings(path+'readability_tokens-large-corpus.json',
#                         path+'readability_tokens_df-large-corpus.txt', path+'unigram-embeddings-10000docs.json')

