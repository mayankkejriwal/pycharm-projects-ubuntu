import codecs, re, json
from sklearn.metrics import precision_recall_fscore_support, precision_recall_curve, accuracy_score
import numpy as np
from scipy.stats import pearsonr
# import rltk

file_path = '/Users/mayankkejriwal/Dropbox/memex-private-nebraska/'
# tk = rltk.init()
#
# def produce_sim_data_v3(in_file=file_path+'training_set_name_pairs_and_similarities-supp-v2.csv', sim_file_v3=
#     file_path + 'training_set_name_pairs_and_similarities-supp-v3.csv'):
#     """
#     careful, it reads v2 and outputs v3, which only contains phonetic similarities
#     :param in_file:
#     :param sim_file_v3:
#     :return:
#     """
#     pass

def read_in_sim_data_as_table(sim_file=file_path+'training_set_name_pairs_and_similarities-supp-v2.csv'):
    header = True
    tab_strings = list()
    tab_values = list()
    ground_truth = list()
    with codecs.open(sim_file, 'r', 'utf-8') as f:
        for line in f:
            fields = re.split(',',line[0:-1])
            if len(fields) <= 1:
                continue
            if header is True:
                header = False
                # print 'dimensions of table correspond to sims.'
                # for i in fields[2:-3]:
                #     print i
                continue
            if fields[-2] != fields[1] or fields[0] != fields[-3]:

                print 'error!'
                print fields
                break
            if len(fields[-1]) == 0:
                print fields
                continue
            tab_values.append([float(i) for i in fields[2:-3]])
            tab_strings.append(fields[0:2])
            # print fields
            ground_truth.append(int(fields[-1]))
    return (tab_strings, tab_values, ground_truth)


def compute_precision_recall(dim=4):
    tab_strings, tab_values, ground_truth = read_in_sim_data_as_table()
    y_true = list()
    y_pred = list()
    for i in range(len(tab_values)):
        y_true.append(ground_truth[i])
        y_pred.append(tab_values[i][dim])
    precision, recall, thresholds = precision_recall_curve(np.array(y_true), np.array(y_pred))

    print precision
    print recall

def compute_precision_recall_accuracy_thresholded_v2(threshold=0.5, sim_column=4):
    """
    starting from 0 the sims (for supp-v2) are:
        tri_gram_jaccard_similarity
    	jaro_winkler_similarity
    	levenshtein_similarity
    	needleman_wunsch_similarity
    	metaphone_similarity
    :param threshold:
    :param sim_column:
    :return:
    """
    tab_strings, tab_values, ground_truth = read_in_sim_data_as_table()
    y_true = list()
    y_pred = list()
    num_pos = 0
    output_pos = 0
    for i in range(len(tab_values)):
        y_true.append(ground_truth[i])
        if ground_truth[i] == 1:
            num_pos += 1
        if tab_values[i][sim_column] >= threshold:
            y_pred.append(1)
            output_pos += 1
        else:
            y_pred.append(0)
    # precision, recall, thresholds = precision_recall_curve(np.array(y_true), np.array(y_pred))
    print precision_recall_fscore_support(np.array(y_true), np.array(y_pred), average='binary')
    print 'accuracy score: ',accuracy_score(np.array(y_true), np.array(y_pred))
    # print precision
    # print recall
    print num_pos
    print output_pos

def compute_precision_recall_accuracy_thresholded_v3(threshold=0.5, sim_column=4):
        """
        starting from 0 the sims (for supp-v2) are:
            soundex
        	nysiis
        	metaphone
        :param threshold:
        :param sim_column:
        :return:
        """
        tab_strings, tab_values, ground_truth = read_in_sim_data_as_table()
        y_true = list()
        y_pred = list()
        num_pos = 0
        output_pos = 0
        for i in range(len(tab_values)):
            y_true.append(ground_truth[i])
            if ground_truth[i] == 1:
                num_pos += 1
            if tab_values[i][sim_column] >= threshold:
                y_pred.append(1)
                output_pos += 1
            else:
                y_pred.append(0)
        # precision, recall, thresholds = precision_recall_curve(np.array(y_true), np.array(y_pred))
        print 'printing precision, recall, fscore, support:',
        print precision_recall_fscore_support(np.array(y_true), np.array(y_pred), average='binary')
        print 'accuracy score: ', accuracy_score(np.array(y_true), np.array(y_pred))
        # print precision
        # print recall
        print 'number of positives output: ',
        print num_pos
        print 'number of positives in ground truth: ',
        print output_pos

def pearson_r():
    tab_strings, tab_values, ground_truth = read_in_sim_data_as_table()
    tab_values = np.array(tab_values).transpose()
    print tab_values.shape
    tab_values = list(tab_values)
    for i in range(0, len(tab_values)):
        print pearsonr(tab_values[i], ground_truth)


# read_in_sim_data_as_table()
# compute_precision_recall()
# pearson_r()
for t in range(5, 9, 1):
    print 'threshold: ',t*0.1
    compute_precision_recall_accuracy_thresholded_v2(threshold=t*0.1)