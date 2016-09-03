import TextPreprocessors
import codecs
import kNearestNeighbors
import re
import numpy as np
import warnings
from sklearn.preprocessing import normalize
from sklearn.ensemble import RandomForestClassifier

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
    def preprocess_filtered_eyeColor_file(filtered_eyeColor_file, embeddings_file, output_file,
                                          preprocess_function=TextPreprocessors.TextPreprocessors._preprocess_tokens):
        """
        The output file will contain three tab delimited columns. The first column contains a token
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
    def _prepare_train_test_data(pos_neg_file, train_percent = 0.5, randomize=False):
        """
        Warning: randomize is not implemented at present.
        :param pos_neg_file:
        :param train_percent:
        :param randomize: If true, we'll randomize the data we're reading in from pos_neg_file.
        :return:
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
    def _train_and_test_classifier(train_data, train_labels, test_data, test_labels, classifier_model):
        """
        Take three matrices and compute a bunch of metrics
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
        print len(predicted_labels)

    @staticmethod
    def script_1(pos_neg_file):
        """

        :param pos_neg_file:
        :return:
        """
        data_dict = TokenSupervised._prepare_train_test_data(pos_neg_file)
        data_dict['classifier_model'] = 'random_forest'
        TokenSupervised._train_and_test_classifier(**data_dict)


# path='/home/mayankkejriwal/Downloads/memex-cp4-october/'
# TokenSupervised.script_1(path+'token-supervised/pos-neg-eyeColor.txt')
