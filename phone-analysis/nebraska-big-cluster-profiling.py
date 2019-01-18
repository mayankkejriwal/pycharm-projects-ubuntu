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

def build_worker_int_dict(cluster_edge_list=path+'connected-component-analysis-round3/network-profiling-data/phone-postid-edge-list-merged-names',
                      merged_names=path + 'connected-component-analysis-round3/network-profiling-data/merged-names-map.json',

                      conn_comp_folder=path + 'connected-component-analysis-round3/connected-component-workers-old/',
                      out_file=path+'connected-component-analysis-round3/network-profiling-data/worker-ads-int-dict.json'
                      ):
    G = nx.read_edgelist(cluster_edge_list, delimiter='\t')
    print 'finished reading edge list'
    merged_names_map = json.load(open(merged_names, 'r'))
    print 'finished reading in merged names map'
    worker_int_dict = dict()
    for n in G.nodes():
        if str(n) in worker_int_dict:
            print str(n)
            continue
        set_of_ints = set()
        file_list = list()
        if 'PREF' not in n:
            file_list.append(n)
        else:
            file_list = merged_names_map[n]
        for f in file_list:
            with codecs.open(conn_comp_folder + f + '.txt', 'r', 'utf-8') as m:
                counter = 0
                for line in m:
                    set_of_ints = set_of_ints.union(set(re.split(' ', line[0:-1])))
                    counter += 1
                if counter != 1:
                    print 'problems in file.' + f + '...more than one line...'
                    raise Exception
        worker_int_dict[str(n)] = list(set_of_ints)

    # for k, v in worker_int_dict.items():
    #     print k, v
    #     break
    json.dump(worker_int_dict, open(out_file, 'w'))


def build_worker_id_dict(cluster_edge_list=path+'connected-component-analysis-round3/network-profiling-data/phone-postid-edge-list-merged-names',
                      merged_names=path + 'connected-component-analysis-round3/network-profiling-data/merged-names-map.json',
                        int_id_mapping=path+'adj_lists/int-id-mapping.tsv',
                      conn_comp_folder=path + 'connected-component-analysis-round3/connected-component-workers-old/',
                      out_file=path+'connected-component-analysis-round3/network-profiling-data/worker-ads-id-dict.json'
                      ):
    G = nx.read_edgelist(cluster_edge_list, delimiter='\t')
    print 'finished reading edge list'
    merged_names_map = json.load(open(merged_names, 'r'))
    print 'finished reading in merged names map'
    worker_int_dict = dict()
    for n in G.nodes():
        if str(n) in worker_int_dict:
            print str(n)
            continue
        set_of_ints = set()
        file_list = list()
        if 'PREF' not in n:
            file_list.append(n)
        else:
            file_list = merged_names_map[n]
        for f in file_list:
            with codecs.open(conn_comp_folder + f + '.txt', 'r', 'utf-8') as m:
                counter = 0
                for line in m:
                    set_of_ints = set_of_ints.union(set(re.split(' ', line[0:-1])))
                    counter += 1
                if counter != 1:
                    print 'problems in file.' + f + '...more than one line...'
                    raise Exception
        worker_int_dict[str(n)] = list(set_of_ints)

    int_id_dict = dict()
    with codecs.open(int_id_mapping, 'r', 'utf-8') as f:
        for line in f:
            fields = re.split('\t', line[0:-1])
            int_id_dict[fields[0]] = fields[1]

    worker_id_dict = dict()
    for k, v in worker_int_dict.items():
        worker_id_dict[k] = list()
        for i in v:
            worker_id_dict[k].append(int_id_dict[i])

    json.dump(worker_id_dict, open(out_file, 'w'))


def _network_profile(cluster_edge_list=path+'connected-component-analysis-round2/network-profiling-data/cid6_analysis/cid6-edge-list'):
    G = nx.read_edgelist(cluster_edge_list, delimiter='\t')
    print 'finished reading edge list'
    print len(G.nodes())


def isolate_ads(worker_ads_dict_file=path+'connected-component-analysis-round2/network-profiling-data/cid6_analysis/worker-ads-int-dict.json',
                       int_id_mapping=path+'adj_lists/int-id-mapping.tsv',
                       data_file=path+'data_for_memex.json',
                out_file=path+'connected-component-analysis-round2/network-profiling-data/cid6_analysis/ads.json'):
    worker_ads_dict = json.load(open(worker_ads_dict_file, 'r'))
    ad_ints = set()
    for k in worker_ads_dict.values():
        for m in k:
            ad_ints.add(m)

    print len(ad_ints)
    ad_ids = set()
    int_id_dict = dict()
    with codecs.open(int_id_mapping, 'r', 'utf-8') as f:
        for line in f:
            fields = re.split('\t', line[0:-1])
            if fields[0] not in ad_ints:
                continue
            else:
                int_id_dict[int(fields[0])] = fields[1]
                ad_ids.add(fields[1])

    print len(int_id_dict.keys())


    out = codecs.open(out_file, 'w', 'utf-8')
    count = 0
    with codecs.open(data_file, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            if obj['_id'] in ad_ids:
                out.write(line)
                count += 1

    out.close()
    print count


def ethnicity_profiling(worker_ads=path+'connected-component-analysis-round2/network-profiling-data/cid6_analysis/ads.json',
                        profiling_output_whole=path+'connected-component-analysis-round2/network-profiling-data/cid6_analysis/ethnicity-non-fractional.tsv',
                        profiling_output_frac=path + 'connected-component-analysis-round2/network-profiling-data/cid6_analysis/ethnicity-fractional.tsv'):
    """
    we do count missing ethnicities/empty ethnicity lists (equivalently) also
    :param worker_ads:
    :param profiling_output_whole:
    :param profiling_output_frac:
    :return:
    """
    ethnicity_whole_dict = dict()
    ethnicity_frac_dict = dict()
    with codecs.open(worker_ads, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            if 'ethnicity' not in obj or len(obj['ethnicity']) == 0:
                eth = ['None']
            else:
                eth = list(set(obj['ethnicity'])) # this will be a list. we do list set just in case there are dups.
            length = len(eth)
            for e in eth:
                if e not in ethnicity_frac_dict:
                    ethnicity_frac_dict[e] = 0.0
                ethnicity_frac_dict[e] += 1.0/length
                if e not in ethnicity_whole_dict:
                    ethnicity_whole_dict[e] = 0
                ethnicity_whole_dict[e] += 1

    w = codecs.open(profiling_output_whole, 'w', 'utf-8')
    w.write('ethnicity\tcount\n')
    for k, v in ethnicity_whole_dict.items():
        w.write(k+'\t'+str(v)+'\n')
    w.close()

    w = codecs.open(profiling_output_frac, 'w', 'utf-8')
    w.write('ethnicity\tfractional count\n')
    for k, v in ethnicity_frac_dict.items():
        w.write(k + '\t' + str(v) + '\n')
    w.close()


def postid_profiling(worker_ads=path+'connected-component-analysis-round2/network-profiling-data/cid6_analysis/ads.json',
                        profiling_output=path+'connected-component-analysis-round2/network-profiling-data/cid6_analysis/postid.tsv',
                        ):
    """
    we do count missing ethnicities/empty ethnicity lists (equivalently) also
    :param worker_ads:
    :param profiling_output_whole:
    :param profiling_output_frac:
    :return:
    """
    postid_dict = dict()
    with codecs.open(worker_ads, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            if 'post_id' not in obj or not obj['post_id']:
                eth = 'None'
            else:
                eth = obj['post_id']
            # length = len(eth)
            if eth not in postid_dict:
                postid_dict[eth] = 0
            postid_dict[eth] += 1

    w = codecs.open(profiling_output, 'w', 'utf-8')
    w.write('postid\tcount\n')
    for k, v in postid_dict.items():
        w.write(k+'\t'+str(v)+'\n')
    w.close()



# build_worker_int_dict()
# build_worker_id_dict()
# isolate_ads()
# _network_profile()


# ethnicity_profiling()
# postid_profiling()