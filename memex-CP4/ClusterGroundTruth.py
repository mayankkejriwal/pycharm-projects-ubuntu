import codecs
import json


class ClusterGroundTruth:
    """
    Contains static methods, mostly exploratory, for building a ground-truth of seller clusters starting
    from the file memex-cp4-october/ground-truth-corpus.json
    """

    @staticmethod
    def cluster_by_addressLocality(input_file, output_file=None):
        """
        Take the addressLocality field in each object, tokenize it by space and comma, lower case it
        and convert to set of words. use each token in that set as a 'key' for the cluster. We'll start
        by analyzing those.
        :param input_file: the ground-truth-corpus file
        :param output_file:
        :return:
        """
        with codecs.open(input_file, 'r', 'utf-8') as f:
            for line in f:
                pass
