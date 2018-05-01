import codecs, re, json
from sklearn.metrics import precision_recall_fscore_support, precision_recall_curve
import numpy as np
from scipy.stats import pearsonr

file_path = '/Users/mayankkejriwal/Dropbox/memex-private-nebraska/'

def read_in_sim_data_as_table(sim_file=file_path+'training_set_name_pairs_and_similarities-supp.csv'):
    header = True
    tab_strings = list()
    tab_values = list()
    ground_truth = list()
    with codecs.open(sim_file, 'r', 'utf-8') as f:
        for line in f:
            fields = re.split(',',line[0:-1])
            if header is True:
                header = False
                print 'dimensions of table correspond to sims.'
                for i in fields[2:-3]:
                    print i
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

def compute_precision_recall_levenstein_thresholded(threshold=0.7):
    tab_strings, tab_values, ground_truth = read_in_sim_data_as_table()
    y_true = list()
    y_pred = list()
    num_pos = 0
    output_pos = 0
    for i in range(len(tab_values)):
        y_true.append(ground_truth[i])
        if ground_truth[i] == 1:
            num_pos += 1
        if tab_values[i][2] >= threshold:
            y_pred.append(1)
            output_pos += 1
        else:
            y_pred.append(0)
    # precision, recall, thresholds = precision_recall_curve(np.array(y_true), np.array(y_pred))
    print precision_recall_fscore_support(np.array(y_true), np.array(y_pred), average='binary')
    # print precision
    # print recall
    print num_pos
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
compute_precision_recall_levenstein_thresholded()