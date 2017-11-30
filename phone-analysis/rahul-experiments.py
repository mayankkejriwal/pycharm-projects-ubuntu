import codecs, re, json
import math
import networkx as nx
from networkx.drawing import nx_agraph
from networkx import algorithms, assortativity
from networkx import info, density, degree_histogram
import matplotlib.pyplot as plt
import glob
from sklearn import linear_model
import numpy as np
import pygraphviz as pgv

nebraska_path = '/Users/mayankkejriwal/datasets/memex/nebraska-data/data-sharing/'
rahul_path = '/Users/mayankkejriwal/datasets/memex/rahul-experiments/'

def check_url_intersection(rahul_file=rahul_path+'all_urls1.txt', data_for_memex=nebraska_path+'data_for_memex.json', output=rahul_path+'intersecting-urls.jsonl'):
    # rahul_count = 0
    rahul_urls = set()
    with codecs.open(rahul_file, 'r', 'utf-8') as f:
        for line in f:
            line = line[0:-1]
            rahul_urls.add(line)
    print 'length of rahul_urls: ',str(len(rahul_urls))
    out = codecs.open(output, 'w', 'utf-8')
    nebraska_count = 0
    intersection = 0
    with codecs.open(data_for_memex, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])  # exclude newline
            url = obj['url']
            nebraska_count += 1
            if url in rahul_urls:
                intersection += 1
                out.write(line)

    print 'number of urls in nebraska: ',str(nebraska_count)
    print 'intersection: ',str(intersection)
    out.close()


check_url_intersection()
