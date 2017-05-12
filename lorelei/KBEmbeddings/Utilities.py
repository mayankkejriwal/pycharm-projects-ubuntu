from rdflib import Graph
from rdflib.term import Literal, URIRef
import codecs
import json
import re
from gensim.models.word2vec import *

def parse_line_into_triple(line):
        """
        Borrowed verbatim from eswc-2017.EmbeddingGenerator
        Convert a line into subject, predicate, object, and also a flag on whether object is a literal or URI.
        At present we assume all objects are URIs. Later this will have to be changed.
        :param line:
        :return:
        """
        # fields = re.split('> <', line[1:-2])
        # print fields
        answer = dict()
        g = Graph().parse(data=line, format='nt')
        for s,p,o in g:
            answer['subject'] = s
            answer['predicate'] = p
            answer['object'] = o

        if 'subject' not in answer:
            return None
        else:
            answer['isObjectURI'] = (type(answer['object']) != Literal)
            return answer

def read_in_random_walks_as_sentences(random_walk_jl):
    sentences = list()
    with codecs.open(random_walk_jl, 'r', 'utf-8') as f:
        for line in f:
            sentence = list()
            for i in json.loads(line).values()[0]:
                sentence.append(unicode(i))
            sentences.append(sentence)
    return sentences

def serialize_text_file_as_sentences(random_walk_txt):
    """
    Each line contains space delimited 'words'.
    :param random_walk_txt:
    :return:
    """
    sentences = list()
    with codecs.open(random_walk_txt, 'r', 'utf-8') as f:
        for line in f:
            sentence = re.split(' ',line[0:-1])
            sentences.append(sentence)
    return sentences

def serialize_word2vec_model_as_jlines(word2vec_file, mapped_places, output_file):
    mapped_dict = dict()
    with codecs.open(mapped_places, 'r', 'utf-8') as f:
        for line in f:
            fields = re.split('\t', line[0:-1])
            mapped_dict[fields[1]] = fields[0]
    print 'finished reading in mapped dict...'
    model = Word2Vec.load(word2vec_file)
    print 'finished reading in word2vec model file...'
    out = codecs.open(output_file, 'w', 'utf-8')
    for k, v in mapped_dict.items():
        try:
            answer = dict()
            answer[v] = model[k].tolist()
            json.dump(answer, out)
            out.write('\n')
        except:
            continue
    out.close()

def count_edges_in_weighted_adjacency_list(adjacency_list):
    """
    makes sense for weighted graph
    :param adjacency_list:
    :return:
    """
    total = 0
    with codecs.open(adjacency_list, 'r', 'utf-8') as f:
        for line in f:
            total += ((len(re.split('\t',line[0:-1]))-1)/2)

    print total




path = '/Users/mayankkejriwal/datasets/lorelei/KB-CIA/'
count_edges_in_weighted_adjacency_list(path+'prob_adjacency_file_1.tsv')
# serialize_word2vec_model_as_jlines(path+'word2vec_trial3_adj_file_1', path+'mapped_populated_places.txt', path+'word2vec_trial3_adj_file_1.jl')



