import json
import codecs


def build_random_index_vectors(input_file, output_word_file, output_context_file, d=3000, non_zero_ratio=0.05):
    """
    At present, I've designed this for the condensed file in reliefWebProcessed. We use the random
    indexing procedure outlined in  http://eprints.sics.se/221/1/RI_intro.pdf
    :param input_file: the condensed file. we will use the loreleiJSONMapping.wordcloud field, and
    lowercase all words in the field.
    :param output_word_file: The output file will be a json where the key is a (lower-case) word, and the object
     is a list representing the d dimensional vector. Note that if new contexts are introduced in the future,
     these words should be updated!
     :param output_context_file: contains a json per line, with each json containing uuid and wordcloud_context_vector
     keys. The values should be obvious.
    :param d: the dimensionality of the random index vectors
    :param non_zero_ratio: the ratio of +1s and also -1s, that we distribute randomly through each vector.
    The fraction of 0's in each vector will be (1-2*ratio)
    :return: None
    """