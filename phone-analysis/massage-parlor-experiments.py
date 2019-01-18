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
from datetime import datetime, timedelta
from dateutil.parser import parse
import math

path = '/Users/mayankkejriwal/datasets/memex/nebraska-data/data-sharing/'

def get_ids_with_more_than_n_names(data_for_memex=path+'data_for_memex.json', n=3, out_file=path+'massage-parlor-experiments/ids_with_more_than_3_names.json'):
    out = codecs.open(out_file, 'w', 'utf-8')
    # prev_line='['
    ids_set = set()
    count = 0
    with codecs.open(data_for_memex, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            count += 1
            if count % 500000 == 0:
                print count
            if 'name' in obj and len(obj['name']) > n:
                ids_set.add(obj['_id'])
    # out.write(prev_line+'\n]\n')
    json.dump(list(ids_set),open(out_file, 'w'))
    out.close()


def randomly_sample_print_n_ads(id_list=path+'massage-parlor-experiments/ids_with_more_than_3_names.json',n=100,
                                data_for_memex_txt=path + 'data_for_memex_txt.json',
                out_file=path+'massage-parlor-experiments/ids_with_more_than_3_names_100_randomly_sampled_ads.jsonl',
        spreadsheet=path+'massage-parlor-experiments/ids_with_more_than_3_names_100_randomly_sampled_ads_labels.csv'):

    ids = list(json.load(open(id_list, 'r')))
    out = codecs.open(out_file, 'w', 'utf-8')
    sp = codecs.open(spreadsheet, 'w', 'utf-8')
    sp.write('ID,label\n')
    np.random.shuffle(ids)
    samples = set(ids[0:n]) # will return error if there are not enough samples
    with codecs.open(data_for_memex_txt, 'r', 'utf-8') as f:
        for line in f:
            if json.loads(line[0:-1])['_id'] in samples:
                out.write(line)
                sp.write(json.loads(line[0:-1])['_id']+',\n')
    out.close()
    sp.close()



# get_ids_with_more_than_n_names()
randomly_sample_print_n_ads()
