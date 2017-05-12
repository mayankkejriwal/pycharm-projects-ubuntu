import json
import codecs
import re
import random
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegressionCV, LogisticRegression
from sklearn.metrics import average_precision_score, precision_recall_fscore_support, f1_score, accuracy_score
from sklearn.metrics import precision_recall_curve, SCORERS
import matplotlib.pyplot as plt


def _replace_pairs_with_feature_vectors(list_of_pairs, pos_dict_str):
    pos_dict = json.load(codecs.open(pos_dict_str))
    answer = list()
    for i in range(len(list_of_pairs)):
        k = list_of_pairs[i]
        t = str(tuple(k))
        answer.append(pos_dict[t])
    return np.array(answer)



def prepare_train_test_data_matrix_1(path, all_pos_files, neg_file, pos_dict_str, test_pos_index, balanced=True):
    """
    Designed for training on all pos files in all_pos_files and also neg_file except that indicated by test_pos_index
    which we use for testing. For a one vs. one test, see the other function.
    :param path:
    :param all_pos_files:
    :param neg_file:
    :param pos_dict_str:
    :param test_pos_index:
    :param balanced:
    :return:
    """
    # print all_pos_files
    test_pos = json.load(codecs.open(path+all_pos_files[test_pos_index]))
    train_pos = None
    for i in range(len(all_pos_files)):
        if test_pos_index == i:
            continue
        else:
            obj = json.load(codecs.open(path+all_pos_files[i]))
            if train_pos is None:
                train_pos = np.array(obj)
            else:
                train_pos = np.append(train_pos, np.array(obj), axis=0)

    train_pos = _replace_pairs_with_feature_vectors(train_pos, path+pos_dict_str)
    test_pos = _replace_pairs_with_feature_vectors(test_pos, path+pos_dict_str)

    neg = json.load(codecs.open(path+neg_file)).values()
    # print neg[0]
    random.shuffle(neg)
    neg_train = np.array(neg[0:len(train_pos)])
    neg_test = np.array(neg[len(train_pos):])
    train_labels = ([1]*len(train_pos))+([0]*len(neg_train))
    test_labels = ([1] * len(test_pos)) + ([0] * len(neg_test))
    train_data = np.append(train_pos, neg_train, axis=0)
    # print train_data.shape
    # print test_pos.shape
    # print neg_test.shape
    test_data = np.append(test_pos, neg_test, axis=0)
    results = dict()
    results['train_data'] = train_data
    results['train_labels'] = train_labels
    results['test_data'] = test_data
    results['test_labels'] = test_labels
    return results


def prepare_train_test_data_matrix_2(path, train_pos_file, neg_file, pos_dict_str, test_pos_file, balanced=True):
    """

    :param path:
    :param all_pos_files:
    :param neg_file:
    :param pos_dict_str:
    :param test_pos_index:
    :param balanced:
    :return:
    """
    # print all_pos_files
    test_pos = json.load(codecs.open(path+test_pos_file))
    train_pos = json.load(codecs.open(path+train_pos_file))

    train_pos = _replace_pairs_with_feature_vectors(train_pos, path+pos_dict_str)
    test_pos = _replace_pairs_with_feature_vectors(test_pos, path+pos_dict_str)

    neg = json.load(codecs.open(path+neg_file)).values()
    # print neg[0]
    random.shuffle(neg)
    neg_train = np.array(neg[0:len(train_pos)])
    neg_test = np.array(neg[len(train_pos):])
    train_labels = ([1]*len(train_pos))+([0]*len(neg_train))
    test_labels = ([1] * len(test_pos)) + ([0] * len(neg_test))
    train_data = np.append(train_pos, neg_train, axis=0)
    # print train_data.shape
    # print test_pos.shape
    # print neg_test.shape
    test_data = np.append(test_pos, neg_test, axis=0)
    results = dict()
    results['train_data'] = train_data
    results['train_labels'] = train_labels
    results['test_data'] = test_data
    results['test_labels'] = test_labels
    return results


def train_and_evaluate_logregCV(data_dict, print_curve=True):
    # model = RandomForestClassifier()
    # print SCORERS
    model = LogisticRegressionCV(scoring='f1')
    # print model
    model.fit(data_dict['train_data'], data_dict['train_labels'])
    if print_curve is True:
        predicted_probabilities = model.predict_proba(data_dict['test_data'])[:,1]
        precision, recall, thresholds = precision_recall_curve(y_true = data_dict['test_labels'],
                                                               probas_pred=predicted_probabilities)

        plt.plot(recall, precision, 'ro')
        plt.show()
    else:
        predictions = model.predict(data_dict['test_data'])
        results = precision_recall_fscore_support(y_pred=predictions, y_true=data_dict['test_labels'])
        labels = ['precision', 'recall', 'f_score', 'support']
        print 'for labels: 0\t1'
        for i in range(len(labels)):
            print labels[i]+'\t'+str(results[i])


path = '/Users/mayankkejriwal/Dropbox/lorelei/all_results_zhang/17_4_24/supervised_data/'
all_pos_files = ['part1PosTuples.json','part2PosTuples.json','part3PosTuples.json',
                 'part4PosTuples.json','part5PosTuples.json']
neg_file = 'negDict_402620.json'
pos_dict_str = 'positiveDictStr.json'
# data = prepare_train_test_data_matrix_2(path, all_pos_files[2], neg_file, pos_dict_str, all_pos_files[1])
data = prepare_train_test_data_matrix_1(path, all_pos_files, neg_file, pos_dict_str, 1)
train_and_evaluate_logregCV(data)