import codecs
import json
import pprint
import TextPreprocessors
import kNearestNeighbors
from decimal import *
import math
from sklearn.preprocessing import normalize
import numpy as np
import warnings
import SimFunctions
import kNearestNeighbors


class FieldAnalyses:
    """
    Use static methods in this class to analyze the structure of the data. Things like, what fields
    are present in the file, and what are their data types?

    We will also use this class to develop methods for measuring field coherence, once we have our unigram embeds.
    """
    @staticmethod
    def read_in_ground_truth_file(ground_truth_file):
        results = list()
        with codecs.open(ground_truth_file, 'r', 'utf-8') as f:
            for line in f:
                results.append(json.loads(line))
        return results

    @staticmethod
    def _l2_norm_on_vec(vec):
        warnings.filterwarnings("ignore")
        k = np.reshape(vec, (1,-1))
        return normalize(k)[0]

    @staticmethod
    @DeprecationWarning
    def _l2_norm_on_vec_old(vec):
        """
        This is the earlier, more primitive implementation. I believe its still correct, but let's not use it.
        :param vec:
        :return: L2-normalized vector
        """
        total = Decimal(0.0)
        new_vec = [0.0]*len(vec)
        for element in vec:
            total = total + Decimal(math.pow(element, 2))
        print total
        if total == 0.0:
            return new_vec
        total = Decimal(math.sqrt(total))
        for i in range(0, len(vec)):
            el = Decimal(vec[i]*1.0)/total
            new_vec[i] = float(el)
        return new_vec

    @staticmethod
    def _l2_norm(vec):
        """
        print l2_norm of vec
        :param vec:
        :return:
        """
        l2_norm = 0.0
        for element in vec:
            l2_norm += math.pow(element, 2)
        print 'l2_norm : ',
        print math.sqrt(l2_norm)

    @staticmethod
    def _find_normalized_centroid_of_vectors(vectors_dict):
        """
        The first step is to build another dictionary that has l2-normalized vectors. Then we average that
        and return the result.
        :param vectors_dict: the key will typically be an attribute value. The value will be an embedded vector
        :return: A normalized vector representing the centroid of the vectors_dict
        """
        normalized_vectors_dict = dict()
        centroid =[0.0]*len(vectors_dict.values()[0])
        num = len(vectors_dict)
        for k, vec in vectors_dict.items():
            new_vec = FieldAnalyses._l2_norm_on_vec(vec)
            normalized_vectors_dict[k] = new_vec
            for i in range(0, len(new_vec)):
                centroid[i] += (new_vec[i]/num)
        return FieldAnalyses._l2_norm_on_vec(centroid)  # this step caused me more headache than it should have

    @staticmethod
    def _build_vector_set_for_attribute(embeddings_file, ground_truth_file, attribute):
        """

        :param embeddings_file:
        :param ground_truth_file:
        :param attribute:
        :return: A dictionary with keys that are values
        """
        ground_truth_list = FieldAnalyses.read_in_ground_truth_file(ground_truth_file)
        embeddings = kNearestNeighbors.read_in_embeddings(embeddings_file)
        attribute_vectors = dict()
        for obj in ground_truth_list:
            if attribute in obj:
                tokens_list = TextPreprocessors.TextPreprocessors.tokenize_field(obj, attribute)
                processed_tokens_list = TextPreprocessors.TextPreprocessors.preprocess_tokens(tokens_list)
                for token in processed_tokens_list:
                    if token in embeddings:
                        attribute_vectors[token] = embeddings[token]
                    # else:
                    #     print 'token in file, but not in embeddings: ',
                    #     print token
        # print len(attribute_vectors)
        return attribute_vectors

    @staticmethod
    def print_fields_data_types(input_file):
        """
        We are only considering 'upper-level' fields.
        :param input_file: A json lines file
        :return: None
        """
        fields = dict()  # the key is the field name, the value will be the data type
        pp = pprint.PrettyPrinter(indent=4)
        with codecs.open(input_file, 'r', 'utf-8') as f:
            for line in f:
                obj = json.loads(line)
                for k, v in obj.items():
                    if k not in fields:
                        fields[k] = set()
                    fields[k].add(type(v))
        print pp.pprint(fields)

    @staticmethod
    def _k_centroid(score_dict, k, nearest):
        """
        :param score_dict: A key is a sim. score, and values are lists of items.
        :param k:
        :param nearest: If True, return k nearest, otherwise return k farthest
        :return: a list of (at most) k values
        """
        return kNearestNeighbors._extract_top_k(scored_results_dict=score_dict, k=k, disable_k=False, reverse=nearest)

    @staticmethod
    def _reverse_dict(dictionary):
        """
        Turn keys into (lists of) values, and values into keys. Values must originally be primitive.
        :param dictionary:
        :return: Another dictionary
        """
        new_dict = dict()
        for k, v in dictionary.items():
            if v not in new_dict:
                new_dict[v] = list()
            new_dict[v].append(k)
        return new_dict

    @staticmethod
    def centroid_analysis_on_attribute_cluster(embeddings_file, ground_truth_file, attribute, k=10):
        """
        Mean, std. dev and k-nearest and k-farthest vectors from centroid (if cluster is small, may overlap)
        :param embeddings_file:
        :param ground_truth_file:
        :param attribute:
        :param k:
        :return: None
        """
        warnings.filterwarnings("ignore")
        attribute_vecs = FieldAnalyses._build_vector_set_for_attribute(embeddings_file, ground_truth_file, attribute)
        centroid = FieldAnalyses._find_normalized_centroid_of_vectors(attribute_vecs)
        sim_dict = dict()
        for key, val in attribute_vecs.items():
            sim_dict[key] = SimFunctions.SimFunctions.abs_cosine_sim(val, centroid)
        # print sim_dict.values()
        print 'mean: ',
        print np.mean(sim_dict.values())
        print 'std. dev: ',
        print np.std(sim_dict.values())
        score_dict = FieldAnalyses._reverse_dict(sim_dict)
        print 'k nearest values: ',
        # print sim_dict['z']
        # print sim_dict['vietnamese']
        print FieldAnalyses._k_centroid(score_dict=score_dict, k=k, nearest=True)
        print 'k farthest values: ',
        print FieldAnalyses._k_centroid(score_dict=score_dict, k=k, nearest=False)


# path='/home/mayankkejriwal/Downloads/memex-cp4-october/'
# FieldAnalyses.print_fields_data_types(path+'part-00000.json')
# FieldAnalyses.centroid_analysis_on_attribute_cluster(path+'unigram-embeddings.json', path+'ground-truth-corpus.json', 'price')
# attribute_vecs = FieldAnalyses._build_vector_set_for_attribute(path+'unigram-embeddings.json',
#                                                               path+'ground-truth-corpus.json', 'ethnicity')
# centroid = FieldAnalyses._find_normalized_centroid_of_vectors(attribute_vecs)
# FieldAnalyses._l2_norm(centroid)

# p = [1, 4, -5, 8]
# (FieldAnalyses._l2_norm(FieldAnalyses._l2_norm_on_vec(p)))
# print p
