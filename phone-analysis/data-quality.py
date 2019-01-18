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
import sys


path = '/Users/mayankkejriwal/datasets/memex/nebraska-data/data-sharing/'


def sample_edges(edge_list=path+'connected-component-analysis-round4/network-profiling-data/phone-postid-edge-list-merged-names',
                 sampled_edges=path+'connected-component-analysis-round4/network-profiling-data/data-quality/sampled-edges',
                 num_samples=10):
    seed = 1
    edges = list()
    with codecs.open(edge_list, 'r', 'utf-8') as f:
        for line in f:
            edges.append(line[0:-1])
    np.random.seed(seed)
    print edges[0:num_samples]
    np.random.shuffle(edges)
    samples = edges[0:num_samples]
    print samples
    out = codecs.open(sampled_edges, 'w')
    for e in samples:
        out.write(e+'\n')
    out.close()


def sampled_edges_ints(sampled_edges=path+'connected-component-analysis-round4/network-profiling-data/data-quality/sampled-edges',
                       merged_names=path+'connected-component-analysis-round4/network-profiling-data/merged-names-map.json',
                       cc_folder=path+'connected-component-analysis-round4/connected-component-workers-old/',
                       out_file=path+'connected-component-analysis-round4/network-profiling-data/data-quality/sampled-edges-ints.json'):
    sampled_edge_list_map = dict()
    with codecs.open(sampled_edges, 'r', 'utf-8') as f:
        for line in f:
            if line[0:-1] in sampled_edge_list_map:
                raise Exception
            else:
                sampled_edge_list_map[line[0:-1]] = set(re.split('\t',line[0:-1]))
    merged_names_map = json.load(open(merged_names, 'r'))
    for k in sampled_edge_list_map.keys():
        new_set = set()
        for item in sampled_edge_list_map[k]:
            if 'PREF' not in item:
                new_set.add(item)
            else:
                new_set = new_set.union(set(merged_names_map[item]))
        sampled_edge_list_map[k] = new_set

    for k in sampled_edge_list_map.keys():
        new_set = set()
        for item in sampled_edge_list_map[k]:
            with codecs.open(cc_folder+item+'.txt','r', 'utf-8') as f:
                for line in f:
                    ints = set(re.split(' ',line[0:-1]))
                    new_set = new_set.union(ints)

        sampled_edge_list_map[k] = list(new_set)

    json.dump(sampled_edge_list_map, open(out_file, 'w'))


def sampled_edges_ids(sampled_edges_ints=path+'connected-component-analysis-round4/network-profiling-data/data-quality/sampled-edges-ints.json',
                      int_ids=path+'adj_lists/int-id-mapping.tsv',
                      out_file=path+'connected-component-analysis-round4/network-profiling-data/data-quality/sampled-edges-ids.json'):
    sampled_edges_ints_map = json.load(open(sampled_edges_ints, 'r'))
    int_set = set()
    int_id_map = dict()
    for l in sampled_edges_ints_map.values():
        int_set = int_set.union(set(l))
    # print int_set
    with codecs.open(int_ids, 'r', 'utf-8') as f:
        for line in f:
            fields = re.split('\t',line[0:-1])
            # print fields
            # break
            if str(fields[0]) in int_set:
                int_id_map[str(fields[0])] = fields[1]
    # print int_id_map
    sampled_edges_ids = dict()
    for k in sampled_edges_ints_map.keys():
        new_list = list()
        for item in sampled_edges_ints_map[k]:
            new_list.append(int_id_map[item])
        sampled_edges_ids[k] = new_list

    json.dump(sampled_edges_ids, open(out_file, 'w'))


def sampled_edges_map(sampled_edges_ints=path+'connected-component-analysis-round4/network-profiling-data/data-quality/sampled-edges-ints.json',
                      out_file=path+'connected-component-analysis-round4/network-profiling-data/data-quality/sampled-edges-map.json',
                      out_script = path+'connected-component-analysis-round4/network-profiling-data/data-quality/make_edge_dirs_script.sh'):
    sampled_edges_ints_map = json.load(open(sampled_edges_ints, 'r'))
    count = 1
    new_map = dict()
    out = codecs.open(out_script, 'w')
    for k in sampled_edges_ints_map.keys():
        new_map[k] = 'edge-'+str(count)
        out.write('mkdir edge-'+str(count)+'\n')
        out.write('mkdir edge-'+str(count)+'/ad_txt\n')
        out.write('mkdir edge-' + str(count) + '/ad_attributes\n')
        count += 1
    json.dump(new_map, open(out_file, 'w'))
    out.close()

def collect_text_ads(sampled_edges_map_file=path+'connected-component-analysis-round4/network-profiling-data/data-quality/sampled-edges-map.json',
                     sampled_edges_ids_file=path+'connected-component-analysis-round4/network-profiling-data/data-quality/sampled-edges-ids.json',
                     text_file=path+'data_for_memex_txt.json'):
    sampled_edges_map = json.load(open(sampled_edges_map_file, 'r'))
    sampled_edges_ids = json.load(open(sampled_edges_ids_file, 'r'))
    all_ids = set()
    id_txt = dict()
    count = 0
    line_count = 0
    for k, v in sampled_edges_ids.items():
        all_ids = all_ids.union(set(v))
    # print all_ids
    with codecs.open(text_file, 'r', 'utf-8') as f:
        for line in f:
            if line_count % 1000000 == 0:
                print line_count
            line_count += 1
            obj = json.loads(line[0:-1])
            id = obj['_id']
            if id not in all_ids:
                continue
            # print id
            # break
            count += 1
            id_txt[id] =  obj
    print count
    print len(all_ids)
    if count != len(all_ids):
        print all_ids.difference(set(id_txt.keys()))
        print set(id_txt.keys()).difference(all_ids)
    for k, v in sampled_edges_map.items():
        out_path = path+'connected-component-analysis-round4/network-profiling-data/data-quality/'+v+'/ad_txt/'
        id_list = sampled_edges_ids[k]
        for id in id_list:
            json.dump(id_txt[id],codecs.open(out_path+id+'.json','w'))

def collect_attribute_ads(
        sampled_edges_map_file=path + 'connected-component-analysis-round4/network-profiling-data/data-quality/sampled-edges-map.json',
        sampled_edges_ids_file=path + 'connected-component-analysis-round4/network-profiling-data/data-quality/sampled-edges-ids.json',
        text_file=path + 'data_for_memex.json'):
    sampled_edges_map = json.load(open(sampled_edges_map_file, 'r'))
    sampled_edges_ids = json.load(open(sampled_edges_ids_file, 'r'))
    all_ids = set()
    id_txt = dict()
    count = 0
    line_count = 0
    for k, v in sampled_edges_ids.items():
        all_ids = all_ids.union(set(v))
    # print all_ids
    with codecs.open(text_file, 'r', 'utf-8') as f:
        for line in f:
            if line_count % 1000000 == 0:
                print line_count
            line_count += 1
            obj = json.loads(line[0:-1])
            id = obj['_id']
            if id not in all_ids:
                continue
            # print id
            # break
            count += 1
            id_txt[id] = obj
    print count
    print len(all_ids)
    if count != len(all_ids):
        print all_ids.difference(set(id_txt.keys()))
        print set(id_txt.keys()).difference(all_ids)
    for k, v in sampled_edges_map.items():
        out_path = path + 'connected-component-analysis-round4/network-profiling-data/data-quality/' + v + '/ad_attributes/'
        id_list = sampled_edges_ids[k]
        for id in id_list:
            json.dump(id_txt[id], codecs.open(out_path + id + '.json', 'w'))

def collect_all_workers_from_edge_ads(worker1='windsor-76', worker2='paloma-3'):
    cc_path = path + 'connected-component-analysis-round4/connected-component-workers-old/'
    ids1 = set()
    ids2 = set()
    with codecs.open(cc_path+worker1+'.txt', 'r', 'utf-8') as f:
        for line in f:
            ids1 = set(re.split(' ',line[0:-1]))

    with codecs.open(cc_path + worker2 + '.txt', 'r', 'utf-8') as f:
        for line in f:
            ids2 = set(re.split(' ', line[0:-1]))

    # print ids1
    # print ids2
    # print len(ids1.union(ids2))
    all_ids = ids1.union(ids2)
    workers_list = set()
    with codecs.open(path + 'connected-component-analysis-round4/workers-int.jl', 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            k = obj.keys()[0]
            if len(set(obj[k]).intersection(all_ids)) > 0:
                workers_list.add(k)
    print workers_list


# sample_edges()
# sampled_edges_ints()
# sampled_edges_ids()
# sampled_edges_map()
# collect_text_ads()
# collect_attribute_ads()
collect_all_workers_from_edge_ads()
