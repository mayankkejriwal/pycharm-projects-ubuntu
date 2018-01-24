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


path = '/Users/mayankkejriwal/datasets/memex/nebraska-data/data-sharing/'

def is_data_for_memex_sorted_by_date(input_file=path+'data_for_memex.json'):
    with codecs.open(input_file, 'r', 'utf-8') as f:
        current = None
        for line in f:
            obj = json.loads(line[0:-1])
            if current is None:
                current = parse(str(obj['day']))
            else:
                day = parse(str(obj['day']))
                if day < current:
                    print 'False'
                    exit(-1)
                else:
                    current = day
    print 'True'





# is_data_for_memex_sorted_by_date() # we ran the code on data_for_memex.json. Answer printed was True
# d = timedelta(days=7)
# k = parse(str(20170327))
# m = k+d
# print(m.year)
# print(m.day)
# print(m.month)