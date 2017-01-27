from Utilities import *
from gensim.models.word2vec import *

def word2vec_on_random_walks(rw_file, serializer=serialize_text_file_as_sentences, output_file=None):
    sentences = serializer(rw_file)
    model = Word2Vec(sentences=sentences, size=100, window=5, sg=1)
    model.init_sims(replace=True)
    # print model.most_similar(positive=['1', '2', '3'])
    if output_file:
        model.save(output_file)

def word2vec_custom_tester(word2vec_model_file):
    """
    Custom file for putting in random stuff.
    :param word2vec_model_file:
    :return:
    """
    model = Word2Vec.load(word2vec_model_file)
    print model.most_similar(positive=['4275033'])


# path = '/Users/mayankkejriwal/datasets/lorelei/KB-CIA/'
# word2vec_on_random_walks(path+'walks_trial3_adj_file_1.txt', output_file=path+'word2vec_trial3_adj_file_1')
# word2vec_custom_tester(path+'word2vec_trial3_adj_file_1')
# word2vec_on_random_walks(path+'CIA-undir-RW10.jl', path+'CIA-undir-RW10-word2vec')
