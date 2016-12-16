from rdflib import Graph
from rdflib.term import Literal, URIRef
import codecs
import json

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

