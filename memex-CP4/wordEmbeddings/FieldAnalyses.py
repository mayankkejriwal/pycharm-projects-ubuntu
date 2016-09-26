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
from random import shuffle
from nltk.tokenize import sent_tokenize,word_tokenize
import pprint


class FieldAnalyses:
    """
    Use static methods in this class to analyze the structure of the data. Things like, what fields
    are present in the file, and what are their data types?

    We will also use this class to develop methods for measuring field coherence, once we have our unigram embeds.
    """
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
    def read_in_ground_truth_file(ground_truth_file):
        results = list()
        with codecs.open(ground_truth_file, 'r', 'utf-8') as f:
            for line in f:
                results.append(json.loads(line))
        return results

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
    def centroid_analysis_on_attribute_cluster(embeddings_file, ground_truth_file, attribute, k=10):
        """
        Mean, std. dev and k-nearest and k-farthest vectors from centroid (if cluster is small, may overlap)
        :param embeddings_file:
        :param ground_truth_file: a misnomer. This is just a 'corpus' file.
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

    @staticmethod
    def sample_n_values_from_field(text_corpus, attribute, n=10, output_file=None):
        """
        Will sample the values and write them out to file/print them. Note that we treat all values independently,
        regardless of whether they originated in the same document. We do tokenize each value, using word/sent_tokenize,
        then concatenate all tokens using space. This way, we're assured of some rudimentary normalization.
        :param text_corpus:
        :param output_file:
        :return: None
        """
        results = set()
        with codecs.open(text_corpus, 'r', 'utf-8') as f:
            for line in f:
                obj = json.loads(line)
                if attribute not in obj:
                    continue
                if type(obj[attribute]) == list:
                    results = results.union(set(obj[attribute]))
                else:
                    results.add(obj[attribute])
        if len(results) <= n:
            print 'Warning: the number of values in attribute is not greater than what you\'ve requested'
        else:
            results = list(results)
            shuffle(results)
            results = results[0:n]

        if output_file:
            out = codecs.open(output_file, 'w', 'utf-8')
            for result in results:
                word_tokens = list()
                for s in sent_tokenize(result):
                    word_tokens += word_tokenize(s)
                out.write(' '.join(word_tokens))
                out.write('\n')
            out.close()
        else:
            pp = pprint.PrettyPrinter(indent=4)
            pp.pprint(results)

    @staticmethod
    def find_intersecting_values(corpus, attribute1, attribute2, num=10000):
        """
        We will use lower-case tokens preprocessing only.
        :param corpus:
        :param attribute1:
        :param attribute2:
        :param num: the (first) number of lines to consider
        :return: A set of intersecting values
        """
        values1 = set()
        values2 = set()
        count = 1
        with codecs.open(corpus, 'r', 'utf-8') as f:
            for line in f:
                print count
                obj = json.loads(line)
                if attribute1 in obj:
                    tokens_list = TextPreprocessors.TextPreprocessors._tokenize_field(obj, attribute1)
                    vals = set(TextPreprocessors.TextPreprocessors._preprocess_tokens(tokens_list, options=['lower']))
                    values1 = values1.union(vals)
                if attribute2 in obj:
                    tokens_list = TextPreprocessors.TextPreprocessors._tokenize_field(obj, attribute2)
                    vals = set(TextPreprocessors.TextPreprocessors._preprocess_tokens(tokens_list, options=['lower']))
                    values2 = values2.union(vals)
                if count == num:
                    break
                count += 1
        results = values1.intersection(values2)
        print results
        print len(results)
        return results

    @staticmethod
    def find_missing_present_field_statistics(corpus, field):
        """
        We are doing this for upper-level fields only. We will print out the number of objects containing that
        field and the number of objects missing that field.
        :param corpus: A jl file
        :param field: An upper level field
        :return: None
        """
        present = 0
        absent = 0
        with codecs.open(corpus, 'r', 'utf-8') as f:
            for line in f:
                obj = json.loads(line)
                if field in obj:
                    if obj[field]:
                        present += 1
                        continue

                absent += 1
        print 'num. objects in which field is present: ',
        print present
        print 'num. objects in which field is absent: ',
        print absent

    @staticmethod
    def field_value_statistics(corpus, field):
        """
        For each value, will print count of objects it occurs in. Note that due to the multi/missing-value problem,
        numbers may not add up to the total number of objects.

        We will convert the line and field to lower-case to avoid conflation problems.

        Prints out and returns the statistics dictionary
        :param corpus:
        :param field:
        :return: the statistics dictionary
        """
        field = field.lower()
        stats_dict = dict()
        pp = pprint.PrettyPrinter(indent =4)
        with codecs.open(corpus, 'r', 'utf-8') as f:
            for line in f:
                l = line.lower()
                obj = json.loads(l)
                if field in obj:
                    if obj[field]:
                        elements = list()
                        if type(obj[field]) is not list:
                            elements.append(obj[field])
                        else:
                            elements = obj[field]
                        for element in elements:
                            if element not in stats_dict:
                                stats_dict[element] = 0
                            stats_dict[element] += 1
        pp.pprint(stats_dict)
        return stats_dict



# path='/Users/mayankkejriwal/ubuntu-vm-stuff/home/mayankkejriwal/tmp/'
# FieldAnalyses.field_value_statistics(path+'all_extractions_july_2016.jl', 'nationality')
# FieldAnalyses.find_intersecting_values(path+'corpora/part-00000.json','addressLocality','name')
# FieldAnalyses.sample_n_values_from_field(path+'part-00000.json', attribute='eyeColor', n=100,
#                                          output_file=path+'100-sampled-eyeColor-vals-2.txt')
# FieldAnalyses.print_fields_data_types(path+'part-00000.json')
# FieldAnalyses.centroid_analysis_on_attribute_cluster(path+'unigram-embeddings.json', path+'ground-truth-corpus.json', 'price')
# attribute_vecs = FieldAnalyses._build_vector_set_for_attribute(path+'unigram-embeddings.json',
#                                                               path+'ground-truth-corpus.json', 'ethnicity')
# centroid = FieldAnalyses._find_normalized_centroid_of_vectors(attribute_vecs)
# FieldAnalyses._l2_norm(centroid)

# p = [1, 4, -5, 8]
# (FieldAnalyses._l2_norm(FieldAnalyses._l2_norm_on_vec(p)))
# print p
