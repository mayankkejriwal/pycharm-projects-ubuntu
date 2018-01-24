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

def parse_connected_component_days_file(cc_day=path+'adj_lists/connected-component-day-range.tsv',
                                        singletons=path+'adj_lists/phone-postid-all-singleton-workers.txt',
                                        output_file=path+'adj_lists/connected-component-day-range-processed.csv',
                                        delimiter=','):
    singleton_set = set()
    with codecs.open(singletons, 'r', 'utf-8') as f:
        for line in f:
            singleton_set.add(line[0:-1])
    print 'finished reading singletons file'
    out = codecs.open(output_file, 'w', 'utf-8')
    count = 0
    out.write('worker-designation'+delimiter+'day-range'+delimiter+'is singleton worker with phone-postid edges?'+'\n')
    with codecs.open(cc_day, 'r', 'utf-8') as f:
        for line in f:
            fields = re.split('\t', line[0:-1])
            days = re.split('day', fields[1])[0]
            if fields[0] in singleton_set:
                out.write(fields[0]+delimiter+str(int(days))+delimiter+'YES\n')
                count += 1
            else:
                out.write(fields[0] + delimiter + str(int(days)) +delimiter+ 'NO\n')

    print count
    out.close()

def analyze_connected_component_days_file(cc_day=path+'adj_lists/connected-component-day-range.tsv',
                                        singletons=path+'adj_lists/phone-postid-all-singleton-workers.txt'
                                        ):
    singleton_set = set()
    singleton_zero = 0
    singleton_non_zero = 0
    non_singleton_zero = 0
    non_singleton_non_zero = 0
    with codecs.open(singletons, 'r', 'utf-8') as f:
        for line in f:
            singleton_set.add(line[0:-1])
    print 'finished reading singletons file...',str(len(singleton_set))
    # count = 0
    days_dict = dict()
    total = 0
    with codecs.open(cc_day, 'r', 'utf-8') as f:
        for line in f:
            fields = re.split('\t', line[0:-1])
            days = re.split('day', fields[1])[0]
            if int(days) not in days_dict:
                days_dict[int(days)] = 0
            days_dict[int(days)] += 1
            total += 1
            if fields[0] in singleton_set:
                if (int(days)) == 0:
                    singleton_zero += 1
                else:
                    singleton_non_zero += 1
            else:
                if (int(days)) == 0:
                    non_singleton_zero += 1
                else:
                    non_singleton_non_zero += 1

    print 'num singletons with zero day range...',str(singleton_zero)
    print 'num singletons with non-zero day range...', str(singleton_non_zero)
    print 'num non-singletons with zero day range...', str(non_singleton_zero)
    print 'num non-singletons with non-zero day range...', str(non_singleton_non_zero)

    print 'num zero day range connected workers...',str(non_singleton_zero+singleton_zero)
    print 'num non-zero day range workers...', str(non_singleton_non_zero + singleton_non_zero)

    x = list()
    y = list()
    print total
    for k,v in days_dict.items():
        x.append(k+1)
        y.append(v*1.0/total)
        if k == 0:
            print v*1.0/total



    plt.loglog(x, y, 'ro')

    plt.title("Worker Days-range distribution plot")
    plt.ylabel("Prob(days-range)")
    plt.xlabel("days-range+1")
    plt.show()


analyze_connected_component_days_file()