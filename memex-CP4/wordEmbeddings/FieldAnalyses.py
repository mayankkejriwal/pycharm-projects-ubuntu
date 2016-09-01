import codecs
import json
import pprint
import TextPreprocessors
import kNearestNeighbors
import math
from decimal import *

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
        """

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
        l2_norm = Decimal(0.0)
        for element in vec:
            l2_norm = l2_norm + Decimal(math.pow(element, 2))
        print 'l2_norm : ',
        print Decimal(math.sqrt(l2_norm))


    @staticmethod
    def find_normalized_centroid_of_vectors(vectors_dict):
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
        return centroid

    @staticmethod
    def build_vector_set_for_attribute(embeddings_file, ground_truth_file, attribute):
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


path='/home/mayankkejriwal/Downloads/memex-cp4-october/'
attribute_vecs = FieldAnalyses.build_vector_set_for_attribute(path+'unigram-embeddings.json',
                                                              path+'ground-truth-corpus.json', 'ethnicity')
centroid = FieldAnalyses.find_normalized_centroid_of_vectors(attribute_vecs)
FieldAnalyses._l2_norm(centroid)

# p = [1, 4, -5, 8]
# FieldAnalyses._l2_norm(FieldAnalyses._l2_norm_on_vec(p))
