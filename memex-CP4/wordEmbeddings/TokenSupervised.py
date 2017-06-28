import TextPreprocessors
import codecs, json
import kNearestNeighbors
import re
import numpy as np
import warnings
from sklearn.preprocessing import normalize
from sklearn.ensemble import RandomForestClassifier
from sklearn import neighbors
import SimFunctions
from sklearn.model_selection import GridSearchCV
from sklearn.feature_selection import chi2, f_classif, SelectKBest
from sklearn.linear_model import LogisticRegression, LinearRegression
from sklearn.metrics import roc_auc_score, accuracy_score, precision_score, precision_recall_fscore_support
from sklearn.metrics import precision_recall_curve
import ContextVectorGenerators
import FieldAnalyses
import matplotlib.pyplot as plt
from random import shuffle
import math
from sklearn.externals import joblib
from gensim.models.word2vec import Word2Vec
import pickle

class TokenSupervised:
    """
    This class is primarily concerned with token classification tasks in a supervised setting. For example,
    given a few words like 'green', 'blue' and 'brown' for eye color, can the algorithm learn to detect
     'hazel' and 'grey' from words like 'big' and 'sparkling'? We also use it for 'context' supervision tasks, where
     the feature vector of the word depends on its context.
    """
    @staticmethod
    def _compute_label_of_token(index, list_of_tokens, correct_list):
        """
        My testing shows that this method works. Note that the method is case insensitive, although none of the
        original inputs are modified.
        :param index:
        :param list_of_tokens:
        :param annotated_list:
        :param correct_list:
        :return:
        """
        correct_list_lower = TextPreprocessors.TextPreprocessors._preprocess_tokens(correct_list, options=['lower'])
        list_of_tokens_lower = TextPreprocessors.TextPreprocessors._preprocess_tokens(list_of_tokens, options=['lower'])
        global_token = list_of_tokens_lower[index]
        # print list_of_tokens_lower
        # print correct_list_lower
        if global_token in correct_list_lower:
            return True

        else:
            # could still be a multi-match
            for token in correct_list_lower:
                local_tokens = TextPreprocessors.TextPreprocessors.tokenize_string(token)
                if len(local_tokens) <= 1:
                    continue
                elif global_token not in local_tokens:
                    continue
                else:

                    low_index = local_tokens.index(global_token)
                    # print low_index
                    if index-low_index < 0 or index+(len(local_tokens)-low_index) > len(list_of_tokens_lower):
                        # print 'got here'
                        continue
                    elif list_of_tokens_lower[index-low_index:(index+(len(local_tokens)-low_index))] == local_tokens:
                        return True
                    # print list_of_tokens_lower[index-low_index:(index+(len(local_tokens)-low_index))]
        return False


    @staticmethod
    def _convert_string_to_float_list(string):
        return [float(i) for i in re.split(', ', string[1:-1])]

    @staticmethod
    def _compute_majority_label_in_vector(vector):
        """
        If there are multiple labels with the same count, there's no telling which one will get returned.
        :param vector:
        :return:
        """
        label_dict = dict()
        for v in vector:
            if v not in label_dict:
                label_dict[v] = 0
            label_dict[v] += 1
        max = 0
        max_element = -1
        for k,v in label_dict.items():
            if v > max:
                max = v
                max_element = k
        return max_element

    @staticmethod
    def _l2_norm_on_matrix(matrix):
        """
        Takes a np.matrix style object and l2-normalizes it. Will return the normalized object.
        This method has been tested and works.
        :param matrix:
        :return:
        """
        warnings.filterwarnings("ignore")
        return normalize(matrix)

    @staticmethod
    def l2_normalize(list_of_nums):
        """
        l2 normalize a vector. original vector is unchanged. Meant to be used as a process_embedding function
        in Classification.construct_dbpedia_multi_file
        :param list_of_nums: a list of numbers.
        :return:
        """
        k = list()
        k.append(list_of_nums)
        warnings.filterwarnings("ignore")
        return list(normalize(np.matrix(k))[0])

    @staticmethod
    def prepare_pos_neg_dictionaries_file(dictionary_file1, dictionary_file2, embeddings_file, output_file):
        """
        we will assign label 0 to all words in file 1 and label 1 to all words in file 2
        A line in each file only contains a word.
        :param dictionary_file1:
        :param dictionary_file2:
        :param embeddings_file:
        :param output_file: the pos_neg file
        :return:
        """
        full_embeddings = kNearestNeighbors.read_in_embeddings(embeddings_file)
        out = codecs.open(output_file, 'w', 'utf-8')
        with codecs.open(dictionary_file1, 'r', 'utf-8') as f:
            for line in f:
                out.write(line[0:-1]+'\t'+str(full_embeddings[line[0:-1]])+'\t0\n')
        with codecs.open(dictionary_file2, 'r', 'utf-8') as f:
            for line in f:
                out.write(line[0:-1]+'\t'+str(full_embeddings[line[0:-1]])+'\t1\n')
        out.close()

    @staticmethod
    def prep_preprocessed_annotated_file_for_StanfordNER(preprocessed_file, output_file, text_field, correct_field):
        """"""
        out = codecs.open(output_file, 'w', 'utf-8')
        with codecs.open(preprocessed_file, 'r', 'utf-8') as f:
            for line in f:
                obj = json.loads(line)
                for i in range(len(obj[text_field])):
                    if TokenSupervised._compute_label_of_token(i, obj[text_field], obj[correct_field]):
                        out.write(obj[text_field][i] + '\tMISC.\n')
                    else:
                        out.write(obj[text_field][i] + '\t0\n')
                out.write('\n')
        out.close()


    @staticmethod
    def prep_annotated_files_for_word2vec_classification(annotated_file_txt, correct_file_txt, output_file,
        word2vec_file='/Users/mayankkejriwal/datasets/eswc2017/disasters/GoogleNews-vectors-negative300.bin'):
        """
        If a word is not present in word2vec, we print it out and ignore it. Otherwise, for multiple words
        we simply add them up for the moment. The output is similar to the feature vector (pos neg) files
        we have previously encountered.
        :param annotated_file_txt: Must have an item in each line
        :param correct_file_txt: Must have an item in each line
        :param output_file: the feature vectors file
        :param word2vec_file: at present, we use GoogleNews-vectors-negative300.bin
        :return:
        """
        incorrect_items = set()
        correct_items = set()
        with codecs.open(correct_file_txt, 'r', 'utf-8') as f:
            for line in f:
                correct_items.add(line[0:-1])
        with codecs.open(annotated_file_txt, 'r', 'utf-8') as f:
            for line in f:
                if line[0:-1] not in correct_items:
                    incorrect_items.add(line[0:-1])
        print 'finished processing items...'
        model = Word2Vec.load_word2vec_format(word2vec_file, binary=True)
        model.init_sims(replace=True)
        print 'finished reading word2vec...'
        out = codecs.open(output_file, 'w', 'utf-8')
        for item in correct_items.union(incorrect_items):

                h = item.split()
                arr = np.array([0] * 300)
                flag = False
                for i in h:
                    try:
                        j = model[i]
                        arr = np.sum([j, arr], axis=0)
                        flag = True
                    except KeyError:
                        print 'not found...',
                        print item
                        continue
                if flag:
                    k = TokenSupervised.l2_normalize(arr)
                    if item in correct_items:
                        out.write(item + '\t' + str(k) + '\t1\n')
                    else:
                        out.write(item + '\t' + str(k) + '\t0\n')

        out.close()

    @staticmethod
    def prep_preprocessed_annotated_file_for_classification(preprocessed_file, embeddings_file,
                                            output_file, context_generator, text_field, annotated_field, correct_field):
        """
        Meant for prepping a preprocessed annotated tokens file (e.g. a file output by  into something that is
        amenable to the ML experiments such as in supervised-exp-datasets.

        We also support multi-word annotations.
        :param preprocessed_file:
        :param embeddings_file:
        :param output_file:
        :param context_generator: a function in ContextVectorGenerator that will be used for taking a word from
        the text field (e.g.high_recall_readability_text)and generating a context vector based on some notion of context
        :param text_field: e.g. 'high_recall_readability_text'
        :param: annotated_field: e.g. 'annotated_cities'
        :param correct_field: e.g. 'correct_cities'
        :return: None
        """
        full_embeddings = kNearestNeighbors.read_in_embeddings(embeddings_file)
        # embeddings = set(full_embeddings.keys())
        out = codecs.open(output_file, 'w', 'utf-8')
        with codecs.open(preprocessed_file, 'r', 'utf-8') as f:
            for line in f:
                obj = json.loads(line)
                obj[annotated_field] = TextPreprocessors.TextPreprocessors._preprocess_tokens(obj[annotated_field], ['lower'])
                obj[correct_field] = TextPreprocessors.TextPreprocessors._preprocess_tokens(obj[correct_field],
                                                                                              ['lower'])
                obj[text_field] = TextPreprocessors.TextPreprocessors._preprocess_tokens(obj[text_field],
                                                                                              ['lower'])
                for word in set(obj[annotated_field]):
                    word_tokens = TextPreprocessors.TextPreprocessors.tokenize_string(word)
                    if len(word_tokens) <= 1: # we're dealing with a single word
                        if word not in obj[text_field]:
                            print 'skipping word not found in text field: ',
                            print word
                            continue
                        context_vecs = context_generator(word, obj[text_field], full_embeddings)
                    elif TextPreprocessors.TextPreprocessors.is_sublist_in_big_list(obj[text_field], word_tokens):
                        context_vecs = context_generator(word, obj[text_field], full_embeddings, multi=True)
                    else:
                        continue
                    if not context_vecs:
                        print 'context_generator did not return anything for word: ',
                        print word
                        continue
                    for context_vec in context_vecs:
                        if word in obj[correct_field]:
                            out.write(word + '\t' + str(context_vec) + '\t1\n')
                        else:
                            out.write(word + '\t' + str(context_vec) + '\t0\n')
        out.close()

    @staticmethod
    def preprocess_prepped_annotated_cities(annotated_cities_file, embeddings_file, output_file, context_generator):
        """
        Meant for parsing the files in annotated-cities-experiments/prepped-data into something that is
        amenable to the ML experiments such as in supervised-exp-datasets.
        :param annotated_cities_file:
        :param embeddings_file:
        :param output_file:
        :param context_generator: a function in ContextVectorGenerator that will be used for taking a word from
        high_recall_readability_text and generating a context vector based on some notion of context
        :return: None
        """
        full_embeddings = kNearestNeighbors.read_in_embeddings(embeddings_file)
        # embeddings = set(full_embeddings.keys())
        out = codecs.open(output_file, 'w', 'utf-8')
        with codecs.open(annotated_cities_file, 'r', 'utf-8') as f:
            for line in f:
                obj = json.loads(line)
                for word in obj['annotated_cities']:
                    if word not in obj['high_recall_readability_text']:
                        print 'skipping word not found in high_recall: ',
                        print word
                        continue
                    context_vecs = context_generator(word, obj['high_recall_readability_text'], full_embeddings)
                    if not context_vecs:
                        print 'context_generator did not return anything...'
                        continue
                    for context_vec in context_vecs:
                        if word in obj['correct_cities']:
                            out.write(word+'\t'+str(context_vec)+'\t1\n')
                        else:
                            out.write(word+'\t'+str(context_vec)+'\t0\n')
        out.close()

    @staticmethod
    def _prune_dict_by_less_than_count(d, count):
        """
        Modifies d
        :param d: A dictionarity with numbers as values
        :param count: All items with values less than this number will get removed
        :return: None.
        """
        forbidden = set()
        for k, v in d.items():
            if v < count:
                forbidden.add(k)
        for f in forbidden:
            del d[f]



    @staticmethod
    def construct_nyu_pos_neg_files(doc_vec_file, pos_tokens_file, neg_tokens_file, output_file):
        """

        :param doc_vec_file:
        :param pos_tokens_file:
        :param neg_tokens_file:
        :param output_file:
        :return:
        """
        pos_keys = set()
        neg_keys = set()
        with codecs.open(pos_tokens_file, 'r', 'utf-8') as f:
            for line in f:
                pos_keys.add(json.loads(line).keys()[0])
        with codecs.open(neg_tokens_file, 'r', 'utf-8') as f:
            for line in f:
                neg_keys.add(json.loads(line).keys()[0])

        out = codecs.open(output_file, 'w', 'utf-8')
        with codecs.open(doc_vec_file, 'r', 'utf-8') as f:
            for line in f:
                obj = json.loads(line)
                if obj.keys()[0] in pos_keys:
                    out.write(obj.keys()[0] + '\t' + str(obj.values()[0]) + '\t1\n')
                elif obj.keys()[0] in neg_keys:
                    out.write(obj.keys()[0] + '\t' + str(obj.values()[0]) + '\t0\n')
                else:
                    raise Exception

        out.close()

    @staticmethod
    def construct_nationality_pos_neg_files(ground_truth_corpus, embeddings_file, output_dir,
                        context_generator=ContextVectorGenerators.ContextVectorGenerators.tokenize_add_all_generator):
        """
        The pos-neg file(s) generator for our nationality experiments. We will apply a filter of 10 (if a nationality
        occurs in fewer than 10 objects) we do not include it herein.
        :param ground_truth_corpus: the jl file containing the 4000+ ground-truth data
        :param embeddings_file:
        :param output_dir: ...since multiple pos-neg files will be written. Each file will be of the format
        pos-neg-location-<nationality>.txt. This way, we've broken down the problem into multiple binary classification
        problems
        :param context_generator: a function in ContextVectorGenerator that will be used for taking a word from
        high_recall_readability_text and generating a context vector based on some notion of context
        :return: None
        """
        full_embeddings = kNearestNeighbors.read_in_embeddings(embeddings_file)
        stats_dict = FieldAnalyses.FieldAnalyses.field_value_statistics(ground_truth_corpus, 'nationality')
        TokenSupervised._prune_dict_by_less_than_count(stats_dict, count=10)
        valid_nats = stats_dict.keys()
        outs = dict()
        for nat in valid_nats:
            file_name = output_dir+'pos-neg-location-'+nat+'.txt'
            outs[nat] = codecs.open(file_name, 'w', 'utf-8')
        with codecs.open(ground_truth_corpus, 'r', 'utf-8') as f:
            for line in f:
                obj = json.loads(line.lower())
                if 'location' not in obj or not obj['location']:
                    continue # no context for us to use
                if 'nationality' in obj and obj['nationality']:
                    elements = obj['nationality'] # we know nationality is always a list
                    if not set(elements).intersection(valid_nats):
                        continue # not a valid nationality for us to use
                    context_vec = context_generator(None, TextPreprocessors.TextPreprocessors._tokenize_field(obj, 'location'),
                                                    full_embeddings)
                    if not context_vec:
                        continue
                    for element in elements:
                        for k, v in outs.items():
                            if k == element:
                                v.write(element+'\t'+str(context_vec)+'\t1\n')
                            else:
                                v.write(element+'\t'+str(context_vec)+'\t0\n')
        for v in outs.values():
            v.close()

    @staticmethod
    def construct_nationality_multi_file(nationality_pos_neg_file, output_file,
                                         constraint_list = ['american', 'russian', 'turkish', 'swedish', 'indian']):
        """

        :param nationality_pos_neg_file: any of the previously generated pos_neg files will do
        :param output_file:
        :param constraint_list: If None, we will not constrain labels
        :return: None
        """
        out = codecs.open(output_file, 'w', 'utf-8')
        forbidden = set()
        with codecs.open(nationality_pos_neg_file, 'r', 'utf-8') as f:
            for line in f:
                fields = re.split('\t',line)
                if constraint_list:
                    if fields[0] in constraint_list and fields[1] not in forbidden:
                        forbidden.add(fields[1])
                        out.write(fields[0]+'\t'+fields[1]+'\t'+fields[0]+'\n')
                elif fields[1] not in forbidden:
                    forbidden.add(fields[1])
                    out.write(fields[0]+'\t'+fields[1]+'\t'+fields[0]+'\n')

        out.close()

    @staticmethod
    def preprocess_filtered_eyeColor_file(filtered_eyeColor_file, embeddings_file, output_file,
                                          preprocess_function=TextPreprocessors.TextPreprocessors._preprocess_tokens):
        """
        The output file will contain three tab delimited columns (the 'pos-neg' file). The first column contains a token
        that is guaranteed to be in the embeddings file (hence, we will not have to do any preprocessing
        when we read in this file), the second column is the embedding, and the third column is either a 1 or a 0.

        We will only output the vectors from the embeddings directly. Normalization must occur somewhere
        else if at all.
        :param filtered_eyeColor_file:
        :param embeddings_file:
        :param output_file:
        :param preprocess_function:
        :return: None
        """
        full_embeddings = kNearestNeighbors.read_in_embeddings(embeddings_file)
        embeddings = set(full_embeddings.keys())
        last_col_tokens = list()
        first_col_tokens = list()
        with codecs.open(filtered_eyeColor_file, 'r', 'utf-8') as f:
            for line in f:
                line = line[0:-1] # strip out the newline.
                cols = re.split('\t', line)
                fields = re.split(',',cols[-1])
                if preprocess_function:
                    last_col_tokens += (preprocess_function(fields))
                fields = re.split(' ',cols[0])
                if preprocess_function:
                    first_col_tokens += (preprocess_function(fields))
        pos = set(last_col_tokens).intersection(embeddings)
        neg = (set(first_col_tokens).intersection(embeddings)).difference(pos)
        print 'pos samples: '+str(len(pos))
        print 'neg samples: '+str(len(neg))
        out = codecs.open(output_file, 'w', 'utf-8')
        for p in pos:
            out.write(p+'\t'+str(full_embeddings[p])+'\t1\n')
        for n in neg:
            out.write(n+'\t'+str(full_embeddings[n])+'\t0\n')
        out.close()

    @staticmethod
    def _prepare_multi_for_ML_classification(multi_file):
        """
        We don't really know what the labels are, so that's what we need to find out first. Then, we return
        a dictionary that's a generalized verison of the one returned for binary classification. The file
        is closely modeled after the binary case.
        :param multi_file:
        :return: dict
        """
        result = dict()
        with codecs.open(multi_file, 'r', 'utf-8') as f:
            for line in f:
                line = line[0:-1]
                cols = re.split('\t',line)
                # print cols
                if cols[2] not in result:
                    result[cols[2]] = list()
                result[cols[2]].append(TokenSupervised._convert_string_to_float_list(cols[1]))
        for k, v in result.items():
            result[k] = TokenSupervised._l2_norm_on_matrix(np.matrix(v))
        return result

    @staticmethod
    def _prepare_for_ML_classification(pos_neg_file, normalize=False):
        """
        We need to read in embeddings
        :param pos_neg_file: The file generated in one of the preprocess_filtered_* files
        :return: A dictionary where a 0,1 label references a numpy matrix.
        """
        result = dict()
        pos_features = list()
        neg_features = list()
        with codecs.open(pos_neg_file, 'r', 'utf-8') as f:
            for line in f:
                line = line[0:-1]
                cols = re.split('\t',line)
                # print list(cols[1])
                # break
                if int(cols[2]) == 1:
                    pos_features.append(TokenSupervised._convert_string_to_float_list(cols[1]))
                elif int(cols[2]) == 0:
                    neg_features.append(TokenSupervised._convert_string_to_float_list(cols[1]))
                else:
                    print 'error; label not recognized'
        # print np.matrix(pos_features)
        if normalize == True:
            result[0] = TokenSupervised._l2_norm_on_matrix(np.matrix(neg_features))
            result[1] = TokenSupervised._l2_norm_on_matrix(np.matrix(pos_features))
        else:
            if len(pos_features) != 0:
                result[1] = pos_features
            if len(neg_features) != 0:
                result[0] = neg_features
        return result

    @staticmethod
    def _select_k_best_features(data_dict, k=10, test_data_visible=False):
        """
        Do feature selection. Transforms data_dict
        :param data_dict:
        :param k: the number of features to select
        :param test_data_visible: use the complete dataset to do feature selection. Otherwise, use only
        the training data, but then fit_transform the entire dataset.
        :return: None
        """
        if test_data_visible:
            train_len = len(data_dict['train_data'])
            # test_len = len(data_dict['test_data'])
            data_matrix = np.append(data_dict['train_data'], data_dict['test_data'], axis=0)
            # print data_matrix.shape
            label_matrix = np.append(data_dict['train_labels'], data_dict['test_labels'], axis=0)
            new_data_matrix = TokenSupervised._l2_norm_on_matrix(SelectKBest(f_classif, k=k).fit_transform(data_matrix, label_matrix))
            # print len(new_data_matrix[0:train_len])
            data_dict['train_data'] = new_data_matrix[0:train_len]
            data_dict['test_data'] = new_data_matrix[train_len:]
            # print len(data_dict['test_labels'])
            # print new_data_matrix.shape
        else:
            kBest = SelectKBest(f_classif, k=k)
            kBest = kBest.fit(data_dict['train_data'], data_dict['train_labels'])
            # joblib.dump(kBest, '/Users/mayankkejriwal/git-projects/dig-random-indexing-extractor/test/features')
            train_len = len(data_dict['train_data'])
            data_matrix = np.append(data_dict['train_data'], data_dict['test_data'], axis=0)
            # label_matrix = np.append(data_dict['train_labels'], data_dict['test_labels'], axis=0)
            new_data_matrix = TokenSupervised._l2_norm_on_matrix(kBest.transform(data_matrix))
            data_dict['train_data'] = new_data_matrix[0:train_len]
            data_dict['test_data'] = new_data_matrix[train_len:]

    @staticmethod
    def _select_k_best_features_multi(data_dict, k=10, test_data_visible=False):
        """
        Do feature selection. Transforms data_dict
        :param data_dict:
        :param k: the number of features to select
        :param test_data_visible: use the complete dataset to do feature selection. Otherwise, use only
        the training data, but then fit_transform the entire dataset.
        :return: None
        """
        for k1, v1 in data_dict.items():
            for k2, v2 in v1.items():
                TokenSupervised._select_k_best_features(v2, k=k, test_data_visible=test_data_visible)

    @staticmethod
    def _prepare_train_test_data_multi(multi_file, train_percent = 0.3, randomize=True, balanced_training=True):
        """
        :param multi_file:
        :param train_percent:
        :param randomize:
        :param balanced_training: if True, we will equalize positive and negative training samples by oversampling
        the lesser class. For example, if we have 4 positive samples and 7 negative samples, we will randomly re-sample
        3 positive samples from the 4 positive samples, meaning there will be repetition. Use with caution.
        :return:
        """
        data = TokenSupervised._prepare_multi_for_ML_classification(multi_file)
        results = dict()
        labels = data.keys()
        labels.sort()
        for i in range(0, len(labels)-1):
            results[labels[i]] = dict() # this will be the 1 label
            for j in range(i+1, len(labels)):   # this will be the 0 label
                results[labels[i]][labels[j]] = TokenSupervised._prepare_train_test_from_01_vectors(data[labels[j]],
                                                        data[labels[i]], train_percent, randomize, balanced_training)
        return results

    @staticmethod
    def _prepare_train_test_from_01_vectors(vectors_0, vectors_1, train_percent = 0.3, randomize=True,
                                            balanced_training=True):
        """

        :param vectors_0:
        :param vectors_1:
        :param train_percent:
        :param randomize:
        :param balanced_training: if True, we will equalize positive and negative training samples by oversampling
        the lesser class. For example, if we have 4 positive samples and 7 negative samples, we will randomly re-sample
        3 positive samples from the 4 positive samples, meaning there will be repetition. Use with caution.
        :return: a dictionary that is very similar to the one that gets returned by _prepare_train_test_data
        """
        data = dict()
        data[1] = vectors_1
        data[0] = vectors_0
        return TokenSupervised._prepare_train_test_data(pos_neg_file=None, train_percent=train_percent,
                                            randomize=randomize, balanced_training=balanced_training, data_vectors=data)

    @staticmethod
    def _sample_and_extend(list_of_vectors, total_samples):
        """
        Oversampling code for balanced training. We will do deep re-sampling, assuming that the vectors contain
        atoms.
        :param list_of_vectors: the list of vectors that are going to be re-sampled (randomly)
        :param total_samples: The total number of vectors that we want in the list. Make sure that this number
        is higher than the length of list_of_vectors
        :return: the over-sampled list
        """
        if len(list_of_vectors) >= total_samples:
            raise Exception('Check your lengths!')

        indices = range(0, len(list_of_vectors))
        shuffle(indices)
        desired_samples = total_samples-len(list_of_vectors)
        # print desired_samples>len(list_of_vectors)
        while desired_samples > len(indices):
            new_indices = list(indices)
            shuffle(new_indices)
            indices += new_indices
        new_data = [list(list_of_vectors[i]) for i in indices[0:desired_samples]]
        # print new_data
        return np.append(list_of_vectors, new_data, axis=0)

    @staticmethod
    def get_pos_neg_ids(pos_neg_file):
        result = list()
        with codecs.open(pos_neg_file, 'r', 'utf-8') as f:
            for line in f:
                line = line[0:-1]
                result.append(re.split('\t',line)[0])
        return result

    @staticmethod
    def _prepare_train_test_data_separate(pos_neg_train_file, pos_neg_test_file, balanced_training=True, true_test=True):
        """

        :param pos_neg_file:
        :param train_percent:
        :param randomize: If true, we'll randomize the data we're reading in from pos_neg_file. Otherwise, the initial
        train_percent fraction goes into the training data and the rest of it in the test data
        :param balanced_training: if True, we will equalize positive and negative training samples by oversampling
        the lesser class. For example, if we have 4 positive samples and 7 negative samples, we will randomly re-sample
        3 positive samples from the 4 positive samples, meaning there will be repetition. Use with caution.
        :param data_vectors: this should be set if pos_neg_file is None. It is mostly for internal uses, so
        that we can re-use this function by invoking it from some of the other _prepare_ files.
        :return: dictionary containing training/testing data/labels
        """
        train = TokenSupervised._prepare_for_ML_classification(pos_neg_train_file)
        test = TokenSupervised._prepare_for_ML_classification(pos_neg_test_file)
        test_ids = TokenSupervised.get_pos_neg_ids(pos_neg_test_file)
        train_pos_num = len(train[1])
        train_neg_num = len(train[0])

        train_data_pos = train[1][0:train_pos_num]
        train_data_neg = train[0][0:train_neg_num]



        test_pos_num = len(test[1])
        test_neg_num = len(test[0])
        test_data_pos = test[1][0:test_pos_num]
        test_data_neg = test[0][0:test_neg_num]
        test_labels_pos = [[1] * test_pos_num]
        test_labels_neg = [[0] * test_neg_num]

        if balanced_training:
            if train_pos_num < train_neg_num:
                train_labels_pos = [[1] * train_neg_num]
                train_labels_neg = [[0] * train_neg_num]
                train_data_pos = TokenSupervised._sample_and_extend(train_data_pos, total_samples=train_neg_num)
            elif train_pos_num > train_neg_num:
                train_labels_pos = [[1] * train_pos_num]
                train_labels_neg = [[0] * train_pos_num]
                train_data_neg = TokenSupervised._sample_and_extend(train_data_neg, total_samples=train_pos_num)
            else:
                train_labels_pos = [[1] * train_pos_num]
                train_labels_neg = [[0] * train_neg_num]
        else:
            train_labels_pos = [[1] * train_pos_num]
            train_labels_neg = [[0] * train_neg_num]

        # print len(train_data_pos)
        # print len(train_data_neg)
        train_data = np.append(train_data_pos, train_data_neg, axis=0)
        test_data = np.append(test_data_pos, test_data_neg, axis=0)
        train_labels = np.append(train_labels_pos, train_labels_neg)
        test_labels = np.append(test_labels_pos, test_labels_neg)

        results = dict()
        results['train_data'] = train_data
        results['train_labels'] = train_labels
        results['test_data'] = test_data
        results['test_labels'] = test_labels
        results['test_ids'] = test_ids

        return results

    @staticmethod
    def _prepare_train_test_data_separate_unseen(pos_neg_train_file, pos_neg_test_file, balanced_training=True
                                          ):
        train = TokenSupervised._prepare_for_ML_classification(pos_neg_train_file)
        test = TokenSupervised._prepare_for_ML_classification(pos_neg_test_file)
        test_ids = TokenSupervised.get_pos_neg_ids(pos_neg_test_file)
        train_pos_num = len(train[1])
        train_neg_num = len(train[0])

        train_data_pos = train[1][0:train_pos_num]
        train_data_neg = train[0][0:train_neg_num]

        #test_pos_num = len(test[1])
        test_neg_num = len(test[0])
        #test_data_pos = test[1][0:test_pos_num]
        test_data_neg = test[0][0:test_neg_num]
        #test_labels_pos = [[1] * test_pos_num]
        test_labels_neg = [[0] * test_neg_num]

        if balanced_training:
            if train_pos_num < train_neg_num:
                train_labels_pos = [[1] * train_neg_num]
                train_labels_neg = [[0] * train_neg_num]
                train_data_pos = TokenSupervised._sample_and_extend(train_data_pos, total_samples=train_neg_num)
            elif train_pos_num > train_neg_num:
                train_labels_pos = [[1] * train_pos_num]
                train_labels_neg = [[0] * train_pos_num]
                train_data_neg = TokenSupervised._sample_and_extend(train_data_neg, total_samples=train_pos_num)
            else:
                train_labels_pos = [[1] * train_pos_num]
                train_labels_neg = [[0] * train_neg_num]
        else:
            train_labels_pos = [[1] * train_pos_num]
            train_labels_neg = [[0] * train_neg_num]

        # print len(train_data_pos)
        # print len(train_data_neg)
        train_data = np.append(train_data_pos, train_data_neg, axis=0)
        #test_data = np.append(test_data_neg, axis=0)
        train_labels = np.append(train_labels_pos, train_labels_neg)
        #test_labels = np.append(test_labels_neg)

        results = dict()
        results['train_data'] = train_data
        results['train_labels'] = train_labels
        results['test_data'] = test_data_neg
        results['test_labels'] = test_labels_neg
        results['test_ids'] = test_ids

        return results



    @staticmethod
    def _prepare_train_test_data(pos_neg_file, train_percent = 0.3, randomize=True, balanced_training=True,
                                 data_vectors=None):
        """

        :param pos_neg_file:
        :param train_percent:
        :param randomize: If true, we'll randomize the data we're reading in from pos_neg_file. Otherwise, the initial
        train_percent fraction goes into the training data and the rest of it in the test data
        :param balanced_training: if True, we will equalize positive and negative training samples by oversampling
        the lesser class. For example, if we have 4 positive samples and 7 negative samples, we will randomly re-sample
        3 positive samples from the 4 positive samples, meaning there will be repetition. Use with caution.
        :param data_vectors: this should be set if pos_neg_file is None. It is mostly for internal uses, so
        that we can re-use this function by invoking it from some of the other _prepare_ files.
        :return: dictionary containing training/testing data/labels
        """
        if pos_neg_file:
            data = TokenSupervised._prepare_for_ML_classification(pos_neg_file)
        elif data_vectors:
            data = data_vectors
        else:
            raise Exception('Neither pos_neg_file nor data_vectors argument is specified. Exiting.')

        # print len(data[1])
        # print len(data[0])
        train_pos_num = int(math.ceil(len(data[1])*train_percent))
        train_neg_num = int(math.ceil(len(data[0])*train_percent))
        # print train_pos_num
        # print train_neg_num
        test_pos_num = len(data[1])-train_pos_num
        test_neg_num = len(data[0])-train_neg_num
        if test_pos_num == 0:
            test_pos_num = 1
        if test_neg_num == 0:
            test_neg_num = 1

        test_labels_pos = [[1] * test_pos_num]
        test_labels_neg = [[0] * test_neg_num]

        if not randomize:

            train_data_pos = data[1][0:train_pos_num]
            train_data_neg = data[0][0:train_neg_num]
            if train_pos_num < len(data[1]):
                test_data_pos = data[1][train_pos_num:]
            else:
                test_data_pos = [data[1][-1]]

            if train_neg_num < len(data[0]):
                test_data_neg = data[0][train_neg_num:]
            else:
                test_data_neg = [data[0][-1]]

        else:

            all_pos_indices = range(0, len(data[1]))
            all_neg_indices = range(0, len(data[0]))
            shuffle(all_pos_indices)
            shuffle(all_neg_indices)

            train_data_pos = [data[1][i] for i in all_pos_indices[0:train_pos_num]]
            train_data_neg = [data[0][i] for i in all_neg_indices[0:train_neg_num]]

            if train_pos_num < len(data[1]):
                test_data_pos = [data[1][i] for i in all_pos_indices[train_pos_num:]]
            else:
                test_data_pos = [data[1][-1]]

            if train_neg_num < len(data[0]):
                test_data_neg = [data[0][i] for i in all_neg_indices[train_neg_num:]]
            else:
                test_data_neg = [data[0][-1]]

        if balanced_training:
            if train_pos_num < train_neg_num:
                train_labels_pos = [[1] * train_neg_num]
                train_labels_neg = [[0] * train_neg_num]
                train_data_pos = TokenSupervised._sample_and_extend(train_data_pos, total_samples=train_neg_num)
            elif train_pos_num > train_neg_num:
                train_labels_pos = [[1] * train_pos_num]
                train_labels_neg = [[0] * train_pos_num]
                train_data_neg = TokenSupervised._sample_and_extend(train_data_neg, total_samples=train_pos_num)
            else:
                train_labels_pos = [[1] * train_pos_num]
                train_labels_neg = [[0] * train_neg_num]
        else:
            train_labels_pos = [[1] * train_pos_num]
            train_labels_neg = [[0] * train_neg_num]

        # print len(train_data_pos)
        # print len(train_data_neg)
        train_data = np.append(train_data_pos, train_data_neg, axis=0)
        test_data = np.append(test_data_pos, test_data_neg, axis=0)
        train_labels = np.append(train_labels_pos, train_labels_neg)
        test_labels = np.append(test_labels_pos, test_labels_neg)

        results = dict()
        results['train_data'] = train_data
        results['train_labels'] = train_labels
        results['test_data'] = test_data
        results['test_labels'] = test_labels

        return results

    @staticmethod
    def _predict_labels(test_data, model_dict, ranking_mode=False):
        """

        :param test_data: a vector of vectors
        :param model_dict: the double_dict with a model at the final level.
        :param ranking_mode: if true, we will not return a single predicted label per element of test data, but
        instead return a ranked list of labels per test vector.
        :return: a vector of predicted labels (or ranked label lists). labels will typically not be numeric.
        """

        label_scores = [dict()]*len(test_data)
        for k1, v1 in model_dict.items():
            for k2, model in v1.items():
                predicted_labels = model.predict(test_data)
                predicted_probabilities = TokenSupervised._construct_predicted_probabilities_from_labels(predicted_labels)
                for i in range(0,len(predicted_probabilities)):
                    if k1 not in label_scores[i]:
                        label_scores[i][k1] = 0.0
                    if k2 not in label_scores[i]:
                        label_scores[i][k2] = 0.0
                    label_scores[i][k2] += predicted_probabilities[i][0]
                    label_scores[i][k1] += predicted_probabilities[i][1]
        predicted_labels = list()
        for score_dict in label_scores:
            if ranking_mode:
                predicted_labels.append(TokenSupervised._rank_labels_desc(score_dict))
            else:
                predicted_labels.append(TokenSupervised._find_max_label(score_dict))
        return predicted_labels

    @staticmethod
    def _find_max_label(dictionary):
        """

        :param dictionary: labels and scores. The higher the score, the more probable the label.
        :return: the max label.
        """
        max = -1
        max_label = None
        for k, v in dictionary.items():
            if v > max:
                max = v
                max_label = k
        return max_label

    @staticmethod
    def _rank_labels_desc(dictionary):
        """

        :param dictionary: labels and scores. The higher the score, the more probable the label.
        :return: a ranked list of labels.
        """
        # let's reverse the dictionary first.
        reversed_dict = dict()
        for k, v in dictionary.items():
            if v not in reversed_dict:
                reversed_dict[v] = list()
            reversed_dict[v].append(k)
        keys = reversed_dict.keys()
        keys.sort(reverse=True)
        results = list()
        for k in keys:
            results += reversed_dict[k]
        return results

    @staticmethod
    def _train_and_test_allVsAll_classifier(data_labels_dict, classifier_model="linear_regression", ranking_mode=False,
                                            k=None):
        """
        Prints out a bunch of stuff, most importantly accuracy.
        :param data_labels_dict: The double dict that is returned by _prepare_train_test_data_multi
        :param classifier_model: currently only supports a few popular models.
        :param ranking_mode: if False, we will only take 'top' labels and compute accuracy with respect to those.
        Otherwise, we will rank the labels and compute accuracy@k metrics, where k is parameter below.
        :param k: if you set ranking_mode, you also need to set this. We will print accuracy@k.
        :return: None
        """
        model_dict = dict()
        for k1 in data_labels_dict.keys():
            model_dict[k1] = dict()
            for k2 in data_labels_dict[k1].keys():
                model_dict[k1][k2] = TokenSupervised._binary_class_model(data_labels_dict[k1][k2],
                                                                         classifier_model=classifier_model)
        for k1 in data_labels_dict.keys():
            for k2 in data_labels_dict[k1].keys():
                data_labels_dict[k1][k2]['predicted_labels'] = TokenSupervised._predict_labels(
                    data_labels_dict[k1][k2]['test_data'],model_dict,ranking_mode)

        predicted_labels, test_labels = TokenSupervised._numerize_labels(data_labels_dict)
        # print test_labels

        if not ranking_mode:
            print 'Accuracy: ',
            print accuracy_score(test_labels, predicted_labels)
        else:
            correct = 0.0
            print 'Accuracy@'+str(k)+': ',
            for i in range(len(test_labels)):
                if test_labels[i] in predicted_labels[i][0:k]:
                    correct += 1.0
            print str(correct/len(test_labels))

    @staticmethod
    def _numerize_labels(data_labels_dict):
        """
        Assigns each label to an integer. Important for multi-class metrics computations.
        :param data_labels_dict: the inner-most dictionary should contain a test_labels and predicated_labels
        field. We will be numerizing these.
        :return: A tuple (predicted_labels, test_labels)
        """
        # print data_labels_dict
        labels = set(data_labels_dict.keys())
        vals = list()
        for m in data_labels_dict.values():
            vals += m.keys()
        labels = list(labels.union(set(vals)))
        labels.sort()
        print 'num labels : ',
        print len(labels)
        predicted_labels = list()
        test_labels = list()
        for k1, v1 in data_labels_dict.items():
            for k2, v2 in v1.items():
                p_labels = v2['predicted_labels']
                t_labels = v2['test_labels']
                for p_label in p_labels:
                    if type(p_label) == list:
                        tmp_list = list()
                        for l in p_label:
                            tmp_list.append(labels.index(l))
                        predicted_labels.append(tmp_list)
                    else:
                        predicted_labels.append(labels.index(p_label))
                for t_label in t_labels:
                    if t_label == 0:
                        test_labels.append(labels.index(k2))
                    elif t_label == 1:
                        test_labels.append(labels.index(k1))
                    else:
                        raise Exception('t_label is not 0 or 1')

        return predicted_labels, test_labels

    @staticmethod
    def _binary_class_model(data_labels_dict, classifier_model):
        """
        Train a binary classification model. Hyperparameters must be changed manually,
        we do not take them in as input.

        This method is meant to be called by an upstream method like _train_and_test_allVsAll_classifier
        :param data_labels_dict: contains four keys (train/test_data/labels)
        :param classifier_model:
        :return: the trained model
        """
        train_data = data_labels_dict['train_data']
        train_labels = data_labels_dict['train_labels']
        # test_data = data_labels_dict['test_data']

        if classifier_model == 'random_forest':
            model = RandomForestClassifier()
            model.fit(train_data, train_labels)
            # predicted_labels = model.predict(test_data)
            # predicted_probabilities = model.predict_proba(test_data)
            # print predicted_labels[0:10]
            # print predicted_probabilities[0:10]
        elif classifier_model == 'knn':
            k = 1
            model = neighbors.KNeighborsClassifier(n_neighbors=k, weights='uniform')
            model.fit(train_data, train_labels)
            # predicted_labels = model.predict(test_data)
            # predicted_probabilities = model.predict_proba(test_data)
        elif classifier_model == 'logistic_regression':
            model = LogisticRegression()
            model.fit(train_data, train_labels)
            # predicted_labels = model.predict(test_data)
            # predicted_probabilities = model.predict_proba(test_data)
        elif classifier_model == 'linear_regression': # this is a regressor; be careful.
            model = LinearRegression()
            model.fit(train_data, train_labels)
            # predicted_labels = model.predict(test_data)
            # predicted_probabilities = TokenSupervised._construct_predicted_probabilities_from_labels(predicted_labels)

        return model

    @staticmethod
    def _construct_predicted_probabilities_from_labels(predicted_labels):
        """
        BINARY only
        :param predicted_labels: a list of labels/scores
        :return: A 2-d list of predicated probabilities
        """
        predicted_probs = list()
        for label in predicted_labels:
            k = list()
            k.append(1.0-label)
            k.append(label)
            predicted_probs.append(k)
        return predicted_probs

    @staticmethod
    def _train_and_test_classifier(train_data, train_labels, test_data, test_labels, classifier_model, test_ids=None, return_model=False):
        """
        Take three numpy matrices and compute a bunch of metrics. Hyperparameters must be changed manually,
        we do not take them in as input.

        This method is for BINARY CLASSIFICATION only, although there is some support for regression.
        :param train_data:
        :param train_labels:
        :param test_data:
        :param test_labels:
        :param classifier_model:
        :return:
        """
        if classifier_model == 'random_forest':
            reg = RandomForestClassifier(n_estimators=200)
            parameters = {'max_features': ('auto', 0.5, 'log2'), 'criterion': ('entropy', 'gini')}
            clf = GridSearchCV(reg, parameters, scoring='f1')
            model = clf.fit(train_data, train_labels)
            # model = RandomForestClassifier()
            # model.fit(train_data, train_labels)
            # joblib.dump(model, '/Users/mayankkejriwal/git-projects/dig-random-indexing-extractor/test/model')
            predicted_labels = model.predict(test_data)
            print predicted_labels
            predicted_probabilities = model.predict_proba(test_data)
            print predicted_probabilities
            # print predicted_labels[0:10]
            # print predicted_probabilities[0:10]
        elif classifier_model == 'knn':
            k = 9
            model = neighbors.KNeighborsClassifier(n_neighbors=k, weights='uniform')
            model.fit(train_data, train_labels)
            predicted_labels = model.predict(test_data)
            predicted_probabilities = model.predict_proba(test_data)
            print predicted_probabilities
        elif classifier_model == 'manual_knn':
            # this is not an scikit-learn model; does not support predicted_probabilities
            k = 5
            predicted_labels = list()
            # print len(test_data)
            for t in test_data:
                scores_dict = dict()
                for i in range(0, len(train_data)):
                    score = SimFunctions.SimFunctions.abs_dot_product_sim(train_data[i], t)
                    label = train_labels[i]
                    if score not in scores_dict:
                        scores_dict[score] = list()
                    scores_dict[score].append(label)
                results = kNearestNeighbors._extract_top_k(scores_dict, k=k)
                predicted_labels.append(TokenSupervised._compute_majority_label_in_vector(results))
            predicted_labels = np.array(predicted_labels)
        elif classifier_model == 'logistic_regression':
            model = LogisticRegression()
            model.fit(train_data, train_labels)
            predicted_labels = model.predict(test_data)
            predicted_probabilities = model.predict_proba(test_data)
        elif classifier_model == 'linear_regression': # this is a regressor; be careful.
            model = LinearRegression()
            model.fit(train_data, train_labels)
            predicted_labels = model.predict(test_data)

        final_results = list()
        if test_ids is not None and return_model == False:
            final_results.append(test_ids)
            final_results.append(predicted_probabilities)
            return final_results
        else:
            print 'AUC (Area Under Curve): ',
            print roc_auc_score(test_labels, predicted_labels)

        # precision, recall, thresholds = precision_recall_curve(test_labels, predicted_labels)
        # plt.clf()
        # plt.plot(recall, precision, label='precision-recall-curve')
        # plt.xlabel('Recall')
        # plt.ylabel('Precision')
        # plt.ylim([0.0, 1.05])
        # plt.xlim([0.0, 1.0])
        # plt.title('Precision-Recall curve')
        # plt.savefig('/home/mayankkejriwal/Downloads/memex-cp4-october/tmp/fig.png')
        if classifier_model not in ['linear_regression']:
            print 'Accuracy: ',
            print accuracy_score(test_labels, predicted_labels)
            # print precision_score(test_labels, predicted_labels)
            prf = ['Precision: ', 'Recall: ', 'F-score: ', 'Support: ']
            print 'Class 0\tClass 1'
            k = precision_recall_fscore_support(test_labels, predicted_labels)
            for i in range(0, len(k)):
                print prf[i],
                print k[i]
            # return [k[0][1], k[1][1], k[2][1]]
        if return_model:
            return model

    @staticmethod
    def trial_script_multi(multi_file, opt=2):
        if opt == 1:
            #Test Set 1: read in data from pos_neg_file and use classifiers from scikit-learn/manual impl.
            #We do NOT do any kind of feature selection.

            data_dict = TokenSupervised._prepare_train_test_data_multi(multi_file)
            TokenSupervised._train_and_test_allVsAll_classifier(data_dict, classifier_model='linear_regression')
        elif opt == 2:
            #Test Set 2: read in data from pos_neg_file and use classifiers from scikit-learn/manual impl.
            #We do feature selection.
            data_dict = TokenSupervised._prepare_train_test_data_multi(multi_file)
            # TokenSupervised._select_k_best_features_multi(data_dict, k=20)
            TokenSupervised._train_and_test_allVsAll_classifier(data_dict, classifier_model='random_forest',
                                                                ranking_mode=True,k=1)
            # TokenSupervised._train_and_test_allVsAll_classifier(data_dict, classifier_model='random_forest',
            #                                                     ranking_mode=True, k=2)
            # TokenSupervised._train_and_test_allVsAll_classifier(data_dict, classifier_model='linear_regression',
            #                                                     ranking_mode=True, k=3)
            # TokenSupervised._train_and_test_allVsAll_classifier(data_dict, classifier_model='linear_regression',
            #                                                     ranking_mode=True, k=4)
            # TokenSupervised._train_and_test_allVsAll_classifier(data_dict, classifier_model='linear_regression',
            #                                                     ranking_mode=True, k=5)

    @staticmethod
    def trial_script_binary(pos_neg_file, opt=1, train_percent=0.5):
        """

        :param pos_neg_file: e.g. token-supervised/pos-neg-eyeColor.txt
        :param opt:use this to determine which script to run.
        :return:
        """
        if opt == 1:
            #Test Set 1: read in data from pos_neg_file and use classifiers from scikit-learn/manual impl.
            #We do NOT do any kind of feature selection.

            data_dict = TokenSupervised._prepare_train_test_data(pos_neg_file, train_percent=train_percent)
            # print data_dict['train_labels'][0]
            data_dict['classifier_model'] = 'knn'
            results = TokenSupervised._train_and_test_classifier(**data_dict)
        elif opt == 2:
            #Test Set 2: read in data from pos_neg_file and use classifiers from scikit-learn/manual impl.
            #We do feature selection.
            data_dict = TokenSupervised._prepare_train_test_data(pos_neg_file, train_percent=train_percent)
            TokenSupervised._select_k_best_features(data_dict, k=10)
            data_dict['classifier_model'] = 'knn'
            results = TokenSupervised._train_and_test_classifier(**data_dict)

        return results

    @staticmethod
    def trial_script_train_test_binary(pos_neg_train, pos_neg_test, opt=1):
        """

        :param pos_neg_file: e.g. token-supervised/pos-neg-eyeColor.txt
        :param opt:use this to determine which script to run.
        :return:
        """
        if opt == 1:
            # Test Set 1: read in data from pos_neg_file and use classifiers from scikit-learn/manual impl.
            # We do NOT do any kind of feature selection.

            data_dict = TokenSupervised._prepare_train_test_data_separate_unseen(pos_neg_train, pos_neg_test)
            # print data_dict['train_labels'][0]
            data_dict['classifier_model'] = 'logistic_regression'
            results = TokenSupervised._train_and_test_classifier(**data_dict)
        elif opt == 2:
            # Test Set 2: read in data from pos_neg_file and use classifiers from scikit-learn/manual impl.
            # We do feature selection.
            data_dict = TokenSupervised._prepare_train_test_data_separate_unseen(pos_neg_train, pos_neg_test)
            TokenSupervised._select_k_best_features(data_dict, k=20)
            data_dict['classifier_model'] = 'random_forest'
            results = TokenSupervised._train_and_test_classifier(**data_dict)

        return results


    @staticmethod
    def trial_script_www(pos_neg_file, csv_file):
        """
        Computes the .csv result with both 30% and 70% training/testing data. Remember that it's the testing
        percentage that gets recorded in the rows in the file.
        :param pos_neg_file:
        :param csv_file:
        :return:
        """
        out = codecs.open(csv_file, 'w', 'utf-8')
        out.write('Trial,Precision,Recall,F1-Measure\n')
        train_percent = 30
        for i in range(0,10):
            results = TokenSupervised.trial_script_binary(pos_neg_file, train_percent=train_percent/100.00)
            trial_name = str(100-train_percent)+'_'+str(i)
            out.write(trial_name+','+str(results[0])+','+str(results[1])+','+str(results[2])+'\n')
        out.write('\n\n')
        train_percent = 70
        for i in range(0, 10):
            results = TokenSupervised.trial_script_binary(pos_neg_file, train_percent=train_percent / 100.00)
            trial_name = str(100 - train_percent) + '_' + str(i)
            out.write(trial_name + ',' + str(results[0])+','+str(results[1])+','+str(results[2]) + '\n')
        out.close()


    @staticmethod
    def trial_script_1_FEIII17(pos_neg_train, pos_neg_test, return_string=False):
        """

        """

        results = TokenSupervised.trial_script_train_test_binary(pos_neg_train, pos_neg_test)
        if return_string:
            return str(results[0]) + ',' + str(results[1]) + ',' + str(results[2]) + '\n'
        else:
            print 'Precision,Recall,F1-Measure'
            print str(results[0]) + ',' + str(results[1]) + ',' + str(results[2]) + '\n'


    @staticmethod
    def trial_script_2_FEIII17(pos_neg_train_folder, pos_neg_test_folder, output_file, singular_flag=True):
        """

        """
        plural = ['underwriters', 'insurers', 'servicers', 'affiliates', 'guarantors', 'sellers', 'trustees', 'agents',
                  'counterparties', 'issuers']
        singular = ['underwriter', 'insurer', 'servicer', 'affiliate', 'guarantor', 'seller', 'trustee', 'agent',
                    'counterparty', 'issuer']

        if singular_flag:
            file_array = singular
        else:
            file_array = singular + plural

        out = codecs.open(output_file, 'w', 'utf-8')
        out.write('Role,Precision,Recall,F1-Measure\n')

        for f in file_array:
            try:
                val = f+','+TokenSupervised.trial_script_1_FEIII17(pos_neg_train_folder+f+'.tsv',pos_neg_test_folder+f+'.tsv',True)
                out.write(val)
            except Exception as e:
                print f
                print e
                continue

        out.close()
        # print 'Precision,Recall,F1-Measure'
        # results = TokenSupervised.trial_script_train_test_binary(pos_neg_train, pos_neg_test)
        # print str(results[0]) + ',' + str(results[1]) + ',' + str(results[2]) + '\n'

    @staticmethod
    def averaging_score_file(in1, in2, outfile):
        results = dict()
        with codecs.open(in1, 'r', 'utf-8') as f:
            for line in f:
                obj = json.loads(line)
                results[obj['cluster_id']] = obj['score']
        with codecs.open(in2, 'r', 'utf-8') as f:
            for line in f:
                obj = json.loads(line)
                results[obj['cluster_id']] = (results[obj['cluster_id']] + obj['score'])/2

        out = codecs.open(outfile, 'w', 'utf-8')
        for k, v in results.items():
            answer = dict()
            answer['cluster_id'] = k
            answer['score'] = v

            json.dump(answer, out)
            out.write('\n')
        out.close()


    @staticmethod
    def country_classifier_vinay(folder='/Users/mayankkejriwal/git-projects/etk/etk/classifiers/'):
        feature_file = folder+'feature_vectors.csv'
        labels_file = folder+'class_labels.csv'
        data = list()
        data.append(list())
        data.append(list())
        labels = list()
        with codecs.open(labels_file, 'r', 'utf-8') as f:
            for line in f:
                labels.append(float(line[0:-1]))
        count = 0
        with codecs.open(feature_file, 'r', 'utf-8') as f:
            for line in f:
                instance = re.split(',',line[0:-1])
                for i in range(len(instance)):
                    instance[i] = float(instance[i])
                if labels[count] == 0.0:
                    data[0].append(instance)
                elif labels[count] == 1.0:
                    data[1].append(instance)
                else:
                    raise Exception
                count += 1
        print len(data[0]) #1275 instances
        print len(data[1]) #237 instances; adds up!

        data_dict = TokenSupervised._prepare_train_test_data(pos_neg_file=None, train_percent=0.9, balanced_training=False, data_vectors=data)
        data_dict['classifier_model'] = 'random_forest'
        data_dict['return_model'] = True
        # print data_dict['train_data'][0]
        model = TokenSupervised._train_and_test_classifier(**data_dict)
        pickle.dump(model, open(folder+'country_classifier_random_forest','w'))


    @staticmethod
    def test_country_classifier_pickle(folder='/Users/mayankkejriwal/git-projects/etk/etk/classifiers/'):
        model = pickle.load(open(folder+'country_classifier_random_forest', 'r'))
        print model


TokenSupervised.country_classifier_vinay()
# TokenSupervised.test_country_classifier_pickle()

# path = '/Users/mayankkejriwal/Dropbox/memex-mar-17/CP1/'
# TokenSupervised.averaging_score_file(path+'ISI_test_cluster2vec_new.jl',path+'columbia_logr.jl', path+'avg_columbia_isi_200dims.jl')
# results = TokenSupervised.trial_script_train_test_binary(path+'train_pos_neg.tsv', path+'test_pos_neg.tsv')
# print results
# out = codecs.open(path+'test_results_logr.jl', 'w', 'utf-8')
# for i in range(0, len(results[0])):
#     answer = dict()
#     answer['cluster_id'] = str(results[0][i])
#     answer['score'] = results[1][i][1]
#     json.dump(answer, out)
#     out.write('\n')
# out.close()
# TokenSupervised.trial_script_binary(path+'train_pos_neg.tsv')
# print len(results[0])
# print len(results[1])
# path = '/Users/mayankkejriwal/datasets/memex-evaluation-november/annotated-cities/'
# outer_path = '/Users/mayankkejriwal/datasets/FEIII17/dec-16-data/FEIIIY2_csv/'
# path = outer_path+'Training/partitions-by-role-singular/90-10/'
# TokenSupervised.trial_script_2_FEIII17(path+'train-pos-neg/', path+'test-pos-neg/',outer_path+'singular-90-10-training-only.csv',True)
# TokenSupervised.trial_script_1_FEIII17(path+'all-singular_train_raw_training.tsv',path+'all-singular_test_raw_training.tsv')
# TokenSupervised.prep_annotated_files_for_word2vec_classification(path+'annotated_states.txt', path+'correct_states.txt'
#                                                                  , path+'states_pos_neg.txt')
# path='/Users/mayankkejriwal/ubuntu-vm-stuff/home/mayankkejriwal/Downloads/memex-cp4-october/'
# dbpedia_path = '/Users/mayankkejriwal/datasets/eswc2017/LOD-ML-data/'
# TokenSupervised.construct_dbpedia_multi_file(dbpedia_path+'embedding_vecs.jl', dbpedia_path+'aaup/CompleteDataset.tsv',
#                                 dbpedia_path+'aaup/comp-multi-full.tsv', uri_index=-4, label_index=-2, id_index=-1)
# data_path = '/Users/mayankkejriwal/datasets/nyu_data/'
# TokenSupervised.trial_script_binary(path+'all_pos_neg.txt', 2)
# TokenSupervised.construct_nyu_pos_neg_files(data_path+'idf_weighted_combined_doc_embedding.json',data_path+'tokens_pos_ht_onlyLower.json',
#                                             data_path+'tokens_neg_ht_onlyLower.json', data_path+'pos_neg_file.txt')
# TokenSupervised.construct_nationality_multi_file(
#     path+'supervised-exp-datasets/pos-neg-location-american.txt',
#     path+'supervised-exp-datasets/multi-location-nationality-allclasses.txt',None)
# TokenSupervised.construct_nationality_pos_neg_files(path+'corpora/all_extractions_july_2016.jl',
#                             path+'embedding/unigram-embeddings-v2-10000docs.json', path+'supervised-exp-datasets/')
# TokenSupervised.trial_script_multi(dbpedia_path+'aaup/comp-multi-full.tsv')
# www_path='/Users/mayankkejriwal/ubuntu-vm-stuff/home/mayankkejriwal/tmp/www-experiments/embeddings/'
# input_files = ['pos-neg-text-ages.txt', 'pos-neg-text-cities.txt', 'pos-neg-text-names.txt', 'pos-neg-text-states.txt', 'pos-neg-title-cities.txt']
# result_files = ['text-ages.csv', 'text-cities.csv', 'text-names.csv', 'text-states.csv', 'title-cities.csv']
# for i in range(0, len(input_files)):
# TokenSupervised.trial_script_www(www_path+'pos-neg-files-all/pos-neg-ages.txt',
#                                  www_path+'feature-selection-results-ages/ages-allEmbed-allFeatures.csv')
# TokenSupervised.trial_script_binary(www_path+'pos-neg-ages.txt')
# print TokenSupervised._rank_labels_desc({'a':0.23, 'b':0.23, 'c':0.53})
# big_list = ['he', 'is', 'A', 'cat', 'ON', 'thE', 'Roof', 'cat']
# correct = ['a cat on']
# print TokenSupervised._compute_label_of_token(7, big_list, correct)

# TokenSupervised.prep_preprocessed_annotated_file_for_StanfordNER(www_path+'tokens/tokens-ann_city_title_state_26_50.json',
#     www_path + 'stanfordNER-format/tagged-text_states_26_50.json', 'high_recall_readability_text', 'correct_states')
