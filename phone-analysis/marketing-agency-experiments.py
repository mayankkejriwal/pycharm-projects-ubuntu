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

def isolate_ads(data_for_memex=path+'data_for_memex.json', file=path+'marketing-agency-experiments/PostIDs_HSI.csv',
                out_file=path+'marketing-agency-experiments/PostIDs_HSI_ads.json'):
    header = True
    city_set = set()
    postid_num_set = set()
    with codecs.open(file, 'r', 'utf-8') as f:
        for line in f:
            if header == True:
                header = False
                continue
            fields = re.split(',',line[0:-1])
            postid_num_set.add(fields[0])
            city_set.add(fields[1])

    out = codecs.open(out_file, 'w', 'utf-8')

    count = 0
    with codecs.open(data_for_memex, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            count += 1
            if count % 500000 == 0:
                print count
            if 'city' in obj and obj['city'] and obj['city'] in city_set:
                for n in postid_num_set:
                    if obj['post_id'] and n in obj['post_id']:
                        out.write(line)
                        print line
                        break

    out.close()


def isolate_ad_text_from_ads(ad_file=path+'marketing-agency-experiments/PostIDs_HSI_ads.json',
                             ad_txt_file=path+'data_for_memex_txt.json',
                             out_file=path+'marketing-agency-experiments/PostIDs_HSI_ads_txt.json'):
    ids_set = set()
    with codecs.open(ad_file, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            ids_set.add(obj['_id'])
    out = codecs.open(out_file, 'w', 'utf-8')
    with codecs.open(ad_txt_file, 'r', 'utf-8') as f:
        for line in f:
            if json.loads(line[0:-1])['_id'] in ids_set:
                out.write(line)

    out.close()


def output_ground_truth_ad_count_stats(ad_file=path+'marketing-agency-experiments/PostIDs_HSI_ads.json',
                                    id_int_file=path+'adj_lists/id-int-mapping.tsv',
                                    worker_int_file=path+'connected-component-analysis-round2/'
                                                'network-profiling-data/cid6_analysis/worker-ads-int-dict.json',
                                        workers_set_file=path+'marketing-agency-experiments/PostIDs_HSI_ccr2_workers.json'):
    ids_int = dict()
    workers_set = set()
    int_set = set()
    with codecs.open(id_int_file, 'r') as f:
        for line in f:
            fields = re.split('\t', line[0:-1])
            ids_int[fields[0]] = fields[1]
    print 'finished reading in ids_int dictionary'
    with codecs.open(ad_file, 'r') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            int_set.add(ids_int[obj['_id']])

    worker_ints = json.load(open(worker_int_file, 'r'))
    for k, v in worker_ints.items():
        if len(set(v).intersection(int_set)) != 0:
            workers_set.add(k)

    json.dump(list(workers_set), open(workers_set_file, 'w'))


def analyze_ground_truth_workers_ad_count(workers_set_file=path+'marketing-agency-experiments/PostIDs_HSI_ccr2_workers.json',
                                          worker_int_file=path + 'connected-component-analysis-round2/'
                                                                 'network-profiling-data/cid6_analysis/worker-ads-int-dict.json'
                                          ):
    worker_ints = json.load(open(worker_int_file, 'r'))
    workers_set = set(json.load(open(workers_set_file, 'r')))
    total = 0.0
    for w in workers_set:
        total += len(set(worker_ints[w]))*1.0
        print len(set(worker_ints[w]))*1.0
    print str(total/len(workers_set))



# isolate_ad_text_from_ads()
# output_ground_truth_ad_count_stats()
analyze_ground_truth_workers_ad_count()