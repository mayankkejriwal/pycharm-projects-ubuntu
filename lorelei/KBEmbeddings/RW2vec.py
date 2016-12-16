from Utilities import *
from gensim.models.word2vec import *

def word2vec_on_random_walks(rw_file, output_file=None):
    sentences = read_in_random_walks_as_sentences(rw_file)
    model = Word2Vec(sentences=sentences, size=20, window=2, sg=1)
    model.init_sims(replace=True)
    print model.most_similar(positive=['1', '2', '3'])
    if output_file:
        model.save(output_file)


# path = '/Users/mayankkejriwal/datasets/lorelei/KB-CIA/'
# word2vec_on_random_walks(path+'CIA-undir-RW10.jl', path+'CIA-undir-RW10-word2vec')
