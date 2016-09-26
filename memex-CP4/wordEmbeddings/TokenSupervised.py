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
from sklearn.feature_selection import chi2, f_classif, SelectKBest
from sklearn.linear_model import LogisticRegression, LinearRegression
from sklearn.metrics import roc_auc_score, accuracy_score, precision_score, precision_recall_fscore_support
from sklearn.metrics import precision_recall_curve
import ContextVectorGenerators
import FieldAnalyses
import matplotlib.pyplot as plt

class TokenSupervised:
    """
    This class is primarily concerned with token classification tasks in a supervised setting. For example,
    given a few words like 'green', 'blue' and 'brown' for eye color, can the algorithm learn to detect
     'hazel' and 'grey' from words like 'big' and 'sparkling'? For 'context' supervision tasks, where we
     use more information than just the word vectors, we design a separate class.
    """

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
    def prepare_pos_neg_dictionaries_file(dictionary_file1, dictionary_file2, embeddings_file, output_file):
        """
        we will assign label 0 to file 1 and label 1 to file 2
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
    def prep_preprocessed_annotated_file_for_classification(preprocessed_file, embeddings_file,
                                            output_file, context_generator, text_field, annotated_field, correct_field):
        """
        Meant for parsing the files in annotated-cities-experiments/prepped-data into something that is
        amenable to the ML experiments such as in supervised-exp-datasets.
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
                for word in obj[annotated_field]:
                    if word not in obj[text_field]:
                        print 'skipping word not found in text field: ',
                        print word
                        continue
                    context_vecs = context_generator(word, obj[text_field], full_embeddings)
                    if not context_vecs:
                        print 'context_generator did not return anything...'
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
                if cols[2] not in result:
                    result[cols[2]] = list()
                result[cols[2]].append(TokenSupervised._convert_string_to_float_list(cols[1]))
        for k, v in result.items():
            result[k] = TokenSupervised._l2_norm_on_matrix(np.matrix(v))
        return result

    @staticmethod
    def _prepare_for_ML_classification(pos_neg_file):
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
        result[0] = TokenSupervised._l2_norm_on_matrix(np.matrix(neg_features))
        result[1] = TokenSupervised._l2_norm_on_matrix(np.matrix(pos_features))
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
            new_data_matrix = SelectKBest(f_classif, k=k).fit_transform(data_matrix, label_matrix)
            # print len(new_data_matrix[0:train_len])
            data_dict['train_data'] = new_data_matrix[0:train_len]
            data_dict['test_data'] = new_data_matrix[train_len:]
            # print len(data_dict['test_labels'])
            # print new_data_matrix.shape
        else:
            kBest = SelectKBest(f_classif, k=k)
            kBest = kBest.fit(data_dict['train_data'], data_dict['train_labels'])
            train_len = len(data_dict['train_data'])
            data_matrix = np.append(data_dict['train_data'], data_dict['test_data'], axis=0)
            label_matrix = np.append(data_dict['train_labels'], data_dict['test_labels'], axis=0)
            new_data_matrix = kBest.fit_transform(data_matrix, label_matrix)
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
    def _prepare_train_test_data_multi(multi_file, train_percent = 0.3, randomize=False):
        """
        randomize is not currently implemented.
        :param multi_file:
        :param train_percent:
        :param randomize:
        :return:
        """
        data = TokenSupervised._prepare_multi_for_ML_classification(multi_file)
        results = dict()
        labels = data.keys()
        labels.sort()
        for i in range(0, len(labels)-1):
            results[labels[i]] = dict() # this will be the 1 label
            for j in range(i+1, len(labels)):   # this will be the 0 label
                results[labels[i]][labels[j]] = TokenSupervised._prepare_train_test_from_01_vectors(data[labels[j]], data[labels[i]],
                                                                                    train_percent, randomize)
        return results

    @staticmethod
    def _prepare_train_test_from_01_vectors(vectors_0, vectors_1, train_percent = 0.3, randomize = False):
        """
        randomize is not currently implemented.
        :param vectors_0:
        :param vectors_1:
        :param train_percent:
        :param randomize:
        :return: a dictionary that is very similar to the one that gets returned by _prepare_train_test_data
        """
        data = dict()
        data[1] = vectors_1
        data[0] = vectors_0
        train_pos_num = int(len(data[1])*train_percent)
        train_neg_num = int(len(data[0])*train_percent)
        test_pos_num = len(data[1])-train_pos_num
        test_neg_num = len(data[0])-train_neg_num

        train_data_pos = data[1][0:train_pos_num]
        train_data_neg = data[0][0:train_neg_num]
        train_data = np.append(train_data_pos, train_data_neg, axis=0)

        test_data_pos = data[1][train_pos_num:]
        test_data_neg = data[0][train_neg_num:]
        test_data = np.append(test_data_pos, test_data_neg, axis=0)

        train_labels_pos = [[1]*train_pos_num]
        train_labels_neg = [[0]*train_neg_num]
        train_labels = np.append(train_labels_pos, train_labels_neg)

        test_labels_pos = [[1]*test_pos_num]
        test_labels_neg = [[0]*test_neg_num]
        test_labels = np.append(test_labels_pos, test_labels_neg)

        results = dict()
        results['train_data'] = train_data
        results['train_labels'] = train_labels
        results['test_data'] = test_data
        results['test_labels'] = test_labels

        return results

    @staticmethod
    def _prepare_train_test_data(pos_neg_file, train_percent = 0.3, randomize=False):
        """
        Warning: randomize is not implemented at present.
        :param pos_neg_file:
        :param train_percent:
        :param randomize: If true, we'll randomize the data we're reading in from pos_neg_file.
        :return: dictionary containing training/testing data/labels
        """
        data = TokenSupervised._prepare_for_ML_classification(pos_neg_file)
        train_pos_num = int(len(data[1])*train_percent)
        train_neg_num = int(len(data[0])*train_percent)
        test_pos_num = len(data[1])-train_pos_num
        test_neg_num = len(data[0])-train_neg_num

        train_data_pos = data[1][0:train_pos_num]
        train_data_neg = data[0][0:train_neg_num]
        train_data = np.append(train_data_pos, train_data_neg, axis=0)

        test_data_pos = data[1][train_pos_num:]
        test_data_neg = data[0][train_neg_num:]
        test_data = np.append(test_data_pos, test_data_neg, axis=0)

        train_labels_pos = [[1]*train_pos_num]
        train_labels_neg = [[0]*train_neg_num]
        train_labels = np.append(train_labels_pos, train_labels_neg)

        test_labels_pos = [[1]*test_pos_num]
        test_labels_neg = [[0]*test_neg_num]
        test_labels = np.append(test_labels_pos, test_labels_neg)

        results = dict()
        results['train_data'] = train_data
        results['train_labels'] = train_labels
        results['test_data'] = test_data
        results['test_labels'] = test_labels

        return results

    @staticmethod
    def _predict_labels(test_data, model_dict):
        """

        :param test_data: a vector of vectors
        :param model_dict: the double_dict with a model at the final level.
        :return: a vector of predicted labels. labels will typically not be numeric.
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
            predicted_labels.append(TokenSupervised._find_max_label(score_dict))
        return predicted_labels

    @staticmethod
    def _find_max_label(dictionary):
        """

        :param dictionary:
        :return:
        """
        max = -1
        max_label = None
        for k, v in dictionary.items():
            if v > max:
                max = v
                max_label = k
        return max_label

    @staticmethod
    def _train_and_test_allVsAll_classifier(data_labels_dict, classifier_model="linear_regression"):
        """
        Prints out a bunch of stuff, most importantly accuracy.
        :param data_labels_dict: The double dict that is returned by _prepare_train_test_data_multi
        :param classifier_model: currently only supports a few popular models.
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
                    data_labels_dict[k1][k2]['test_data'],model_dict)

        predicted_labels, test_labels = TokenSupervised._numerize_labels(data_labels_dict)
        print accuracy_score(test_labels, predicted_labels)

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
    def _train_and_test_classifier(train_data, train_labels, test_data, test_labels, classifier_model):
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
            model = RandomForestClassifier()
            model.fit(train_data, train_labels)
            predicted_labels = model.predict(test_data)
            predicted_probabilities = model.predict_proba(test_data)
            # print predicted_labels[0:10]
            # print predicted_probabilities[0:10]
        elif classifier_model == 'knn':
            k = 1
            model = neighbors.KNeighborsClassifier(n_neighbors=k, weights='uniform')
            model.fit(train_data, train_labels)
            predicted_labels = model.predict(test_data)
            predicted_probabilities = model.predict_proba(test_data)
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
            TokenSupervised._select_k_best_features_multi(data_dict, k=20)
            TokenSupervised._train_and_test_allVsAll_classifier(data_dict, classifier_model='random_forest')

    @staticmethod
    def trial_script_binary(pos_neg_file, opt=2):
        """

        :param pos_neg_file: e.g. token-supervised/pos-neg-eyeColor.txt
        :param opt:use this to determine which script to run.
        :return:
        """
        if opt == 1:
            #Test Set 1: read in data from pos_neg_file and use classifiers from scikit-learn/manual impl.
            #We do NOT do any kind of feature selection.

            data_dict = TokenSupervised._prepare_train_test_data(pos_neg_file)
            # print data_dict['train_labels'][0]
            data_dict['classifier_model'] = 'manual_knn'
            TokenSupervised._train_and_test_classifier(**data_dict)
        elif opt == 2:
            #Test Set 2: read in data from pos_neg_file and use classifiers from scikit-learn/manual impl.
            #We do feature selection.
            data_dict = TokenSupervised._prepare_train_test_data(pos_neg_file)
            TokenSupervised._select_k_best_features(data_dict, k=20)
            data_dict['classifier_model'] = 'random_forest'
            TokenSupervised._train_and_test_classifier(**data_dict)


# path='/home/mayankkejriwal/Downloads/memex-cp4-october/'
# TokenSupervised.construct_nationality_multi_file(
#     path+'supervised-exp-datasets/pos-neg-location-american.txt',
#     path+'supervised-exp-datasets/multi-location-nationality-5class.txt')
# TokenSupervised.construct_nationality_pos_neg_files(path+'corpora/all_extractions_july_2016.jl',
#                             path+'embedding/unigram-embeddings-v2-10000docs.json', path+'supervised-exp-datasets/')
# TokenSupervised.trial_script_multi(path+'supervised-exp-datasets/multi-location-nationality-5class.txt')
# TokenSupervised.trial_script_binary(path+'supervised-exp-datasets/pos-neg-location-turkish.txt')
