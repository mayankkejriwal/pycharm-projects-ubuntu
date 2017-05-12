from Utilities import *
from gensim.models.word2vec import *
import numpy as np


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
    print model.most_similar(positive=['22', '17395'])

def word2vec_serialize_model_as_uuids_json(word2vec_model_file, sorted_network_mapping_file,
                                           json_output):
    """
    Custom file for putting in random stuff.
    :param word2vec_model_file:
    :return:
    """
    model = Word2Vec.load(word2vec_model_file)
    key_dict = dict()
    with codecs.open(sorted_network_mapping_file, 'r', 'utf-8') as f:
        for line in f:
            elements = re.split('\t', line[0:-1])
            key_dict[elements[0]] = elements[1]
    out = codecs.open(json_output, 'w', 'utf-8')
    # print type(model.wv)
    for k, v in key_dict.items():
        answer = dict()
        # print k
        # print model[v]
        answer[k] = model[v].tolist()
        json.dump(answer, out)
        out.write('\n')
    out.close()



path = '/Users/mayankkejriwal/datasets/lorelei/RWP/entity-graphs/'
# word2vec_on_random_walks(path+'graph1RandomWalks-5-10.txt', output_file=path+'graph1DeepWalk-5-10')
# word2vec_custom_tester(path+'graph1DeepWalk-5-10')
word2vec_serialize_model_as_uuids_json(path+'graph1DeepWalk-5-10',
                                       path+'sortedEntityNetworkMapping.txt', path+'graph1DeepWalk-5-10.json')
# word2vec_on_random_walks(path+'CIA-undir-RW10.jl', path+'CIA-undir-RW10-word2vec')
