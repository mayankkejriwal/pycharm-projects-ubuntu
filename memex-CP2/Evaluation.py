import re, codecs, numpy
from sklearn.metrics import precision_recall_curve
import matplotlib.pyplot as plt

class Evaluation:

    @staticmethod
    def scorefile_recPrec_graphs(pairs_file, score_file, output_file, prec_rec = True, print_metrics = False):
        """

        :param score_file:
        :param pairs_file:
        :param output_file:
        :return: None
        """

        true_pos = set()
        scores = []
        labels = []
        total = 0
        with codecs.open(pairs_file, 'r', 'utf-8') as f:
            for line in f:
                tokens = re.split('\t|\n', line)
                string = tokens[0]+'\t'+tokens[1]
                true_pos.add(string)
                total = total + 1
        print("Finished reading pairwise_true_positives file. Total lines read is ",total)

        count = 0
        with codecs.open(score_file, 'r', 'utf-8') as f:
            for line in f:
                 tmp = re.split('\t|\n', line)

                 if (tmp[0]+'\t'+tmp[1]) in true_pos:
                     labels.append(1)
                     count = count+1
                 else:
                     labels.append(0)
                 scores.append(float(tmp[2]))
        print("count of true positives in score_file is ",count)
        y_true = numpy.array(labels)
        y_scores = numpy.array(scores)
        precision, recall, thresholds = precision_recall_curve(y_true, y_scores, 1)
        if not len(precision)==len(recall):
            print("Precision and recall num elements differ!")
            print("precision size : ",len(precision))
            print("recall size : ",len(recall))
        fMeasure = []
        for i in range(len(precision)-1):   #we're excluding the last element since there is no threshold for it.
            if (precision[i] + recall[i])!=0:
                fMeasure.append(2 * precision[i] * recall[i] / (precision[i] + recall[i]))
            else:
                fMeasure.append(0.0)

        if print_metrics:
            print("Precision is : ", precision)
            print("Recall is : ", recall)
            print("F-Measure is :", fMeasure)
            print("Thresholds are :", thresholds)

        print("plotting curve...")

        if prec_rec:
            plt.plot(recall, precision)
            plt.xlabel('Recall')
            plt.ylabel('Precision')
        else:
            plt.plot(thresholds, fMeasure)
            plt.xlabel('Threshold')
            plt.ylabel('F-Measure')
        if output_file:
            plt.savefig(output_file)
        else:
            plt.show()
        plt.clf()

upper_path = '/home/mayankkejriwal/Downloads/memex-cp2/'
Evaluation.scorefile_recPrec_graphs(upper_path+'pairs-ground-truth.txt',upper_path+'userName-score-file.txt',
                                    upper_path+'recPrec-basic-userName.png', print_metrics=True)