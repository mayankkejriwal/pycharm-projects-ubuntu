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

def tag_edge_list_with_phones(edge_list=path+'connected-component-analysis/network-profiling-data/cid6_analysis/cid6-edge-list',
                              merged_names=path+'connected-component-analysis/network-profiling-data/merged-names-map.json',
                              int_phones=path+'adj_lists/int-phones.jl',
                              conn_comp_folder=path+'connected-component-analysis/connected-component-workers-old/',
                output_file=path+'connected-component-analysis/network-profiling-data/cid6_analysis/cid6-edge-list-tagged-phone'):
    """
    output edge list will not contain edges that do not have common phone semantics
    :param edge_list:
    :param merged_names:
    :param int_phones:
    :param conn_comp_folder:
    :param output_file:
    :return:
    """
    G = nx.read_edgelist(edge_list, delimiter='\t')
    print 'finished reading edge list'
    merged_names_map = json.load(open(merged_names, 'r'))
    print 'finished reading in merged names map'
    int_phones_dict = dict()

    big_int_list = list()
    conn_comp_int_dict = dict()
    for n in G.nodes():
        if n in conn_comp_int_dict:
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
        conn_comp_int_dict[n] = list(set_of_ints)
        big_int_list += conn_comp_int_dict[n]

    big_int_list = set(big_int_list)

    print 'finished populating connected component dict'

    with codecs.open(int_phones, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            ph = obj.keys()[0]
            if ph in big_int_list:
                int_phones_dict[ph] = set(obj[ph])


    print 'finished populating int phones dict'

    out = codecs.open(output_file, 'w', 'utf-8')
    for e in G.edges():
        phones0 = set()
        phones1 = set()
        for i in conn_comp_int_dict[e[0]]:
            if i not in int_phones_dict:
                continue
            phones0 = phones0.union(int_phones_dict[i])
        for i in conn_comp_int_dict[e[1]]:
            if i not in int_phones_dict:
                continue
            phones1 = phones1.union(int_phones_dict[i])
        intersecting_phones = list(phones0.intersection(phones1))
        if len(intersecting_phones) >= 1:
            out.write(e[0]+'\t'+e[1]+'\t'+'\t'.join(intersecting_phones)+'\n')
    out.close()

def tag_edge_list_with_postids(edge_list=path+'connected-component-analysis/network-profiling-data/cid6_analysis/cid6-edge-list',
                              merged_names=path+'connected-component-analysis/network-profiling-data/merged-names-map.json',
                              int_phones=path+'adj_lists/int-postid.jl',
                              conn_comp_folder=path+'connected-component-analysis/connected-component-workers-old/',
                              output_file=path+'connected-component-analysis/network-profiling-data/cid6_analysis/cid6-edge-list-tagged-postid'):
    """
    output edge list will not contain edges that do not have common postid semantics
    :param edge_list:
    :param merged_names:
    :param int_phones:
    :param conn_comp_folder:
    :param output_file:
    :return:
    """
    G = nx.read_edgelist(edge_list, delimiter='\t')
    print 'finished reading edge list'
    merged_names_map = json.load(open(merged_names, 'r'))
    print 'finished reading in merged names map'
    int_postid_dict = dict()

    big_int_list = list()
    conn_comp_int_dict = dict()
    for n in G.nodes():
        if n in conn_comp_int_dict:
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
        conn_comp_int_dict[n] = list(set_of_ints)
        big_int_list += conn_comp_int_dict[n]

    big_int_list = set(big_int_list)

    print 'finished populating connected component dict'

    with codecs.open(int_phones, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            ph = obj.keys()[0]
            if ph in big_int_list:
                if obj[ph] is None:
                    continue
                int_postid_dict[ph] = obj[ph]
                if int_postid_dict[ph] is None:
                    print ph


    print 'finished populating int postid dict'

    out = codecs.open(output_file, 'w', 'utf-8')
    for e in G.edges():
        phones0 = set()
        phones1 = set()
        for i in conn_comp_int_dict[e[0]]:
            if i not in int_postid_dict:
                continue
            phones0.add(int_postid_dict[i])
        for i in conn_comp_int_dict[e[1]]:
            if i not in int_postid_dict:
                continue
            phones1.add(int_postid_dict[i])
        intersecting_phones = list(phones0.intersection(phones1))
        if None in intersecting_phones:
            print phones1
            print phones0
            print e
            print conn_comp_int_dict[e[0]]
            print conn_comp_int_dict[e[1]]
            sys.exit(-1)
        if len(intersecting_phones) >= 1:
                out.write(e[0]+'\t'+e[1]+'\t'+'\t'.join(intersecting_phones)+'\n')

    out.close()

# tag_edge_list_with_phones()
def edge_counts_phone(phone_edge_tagged=path+'connected-component-analysis/network-profiling-data/cid6_analysis/cid6-edge-list-tagged-phone',
                      out_file=path+'connected-component-analysis/network-profiling-data/cid6_analysis/edge-count-phone.jl'
                      ):
    phone_edge_dict = dict()
    with codecs.open(phone_edge_tagged, 'r', 'utf-8') as f:
        for line in f:
            fields = re.split('\t',line[0:-1])
            for p in fields[2:]:
                if p not in phone_edge_dict:
                    phone_edge_dict[p] = 0
                phone_edge_dict[p]+=1

    reversed_dict = dict()
    for k, v in phone_edge_dict.items():
        # if v > 1000:
        #     print k,
        #     print v
        if v not in reversed_dict:
            reversed_dict[v] = set()
        reversed_dict[v].add(k)

    g = reversed_dict.keys()
    g.sort(reverse=True)
    # print g[0]
    # print len(reversed_dict[g[0]])
    out = codecs.open(out_file, 'w', 'utf-8')
    for k in g:
        answer = dict()
        answer[k] = list(reversed_dict[k])
        json.dump(answer, out)
        out.write('\n')
    out.close()


def edge_counts_postid(phone_edge_tagged=path+'connected-component-analysis/network-profiling-data/cid6_analysis/cid6-edge-list-tagged-postid',
                out_file=path+'connected-component-analysis/network-profiling-data/cid6_analysis/edge-count-postid.jl'
                      ):
    phone_edge_dict = dict()
    with codecs.open(phone_edge_tagged, 'r', 'utf-8') as f:
        for line in f:
            fields = re.split('\t',line[0:-1])
            for p in fields[2:]:
                if p not in phone_edge_dict:
                    phone_edge_dict[p] = 0
                phone_edge_dict[p]+=1

    reversed_dict = dict()
    for k, v in phone_edge_dict.items():
        # if v > 1000:
        #     print k,
        #     print v
        if v not in reversed_dict:
            reversed_dict[v] = set()
        reversed_dict[v].add(k)

    g = reversed_dict.keys()
    g.sort(reverse=True)
    # print g[0]
    # print reversed_dict[g[0]]
    # print '' in reversed_dict[g[0]]
    # print len(reversed_dict[g[0]])
    out = codecs.open(out_file, 'w', 'utf-8')
    for k in g:
        answer = dict()
        answer[k] = list(reversed_dict[k])
        json.dump(answer, out)
        out.write('\n')
    out.close()



def conn_comp_analyses_after_phone_filtering(phone_edge_tagged=path+'network-profiling-data/cid5_analysis/cid5-edge-list-phone-tagged',
                                             orig_edge_list=path+'network-profiling-data/cid5_analysis/cid5-edge-list'):
    H = nx.read_edgelist(orig_edge_list, delimiter='\t')
    print len(H.edges())
    conn_comp_orig = sorted(nx.connected_components(H), key=len, reverse=True)
    print len(conn_comp_orig)
    forbidden_phones = set()
    G = nx.Graph()
    # forbidden_phones.add('3535353535')
    # forbidden_phones.add('3472862353')
    with codecs.open(phone_edge_tagged, 'r', 'utf-8') as f:
        for line in f:
            fields = re.split('\t',line[0:-1])
            if len(forbidden_phones.intersection(set(fields[2:]))) > 0:
                continue
            else:
                G.add_edge(fields[0], fields[1])

    conn_comp = sorted(nx.connected_components(G), key=len, reverse=True)
    print len(G.edges())
    print len(conn_comp)
    print len(conn_comp[0])

def conn_comp_analyses_postid_phone_composition(phone_edge_tagged=path+'network-profiling-data/cid5_analysis/cid5-edge-list-phone-tagged',
                                                postid_edge_tagged=path+'network-profiling-data/cid5_analysis/cid5-edge-list-postid-tagged',
                                             orig_edge_list=path+'network-profiling-data/cid5_analysis/cid5-edge-list'):
    forbidden_phones = set()
    G = nx.Graph()
    forbidden_phones.add('3535353535')
    forbidden_phones.add('3472862353')
    with codecs.open(phone_edge_tagged, 'r', 'utf-8') as f:
       for line in f:
           fields = re.split('\t', line[0:-1])
           if len(forbidden_phones.intersection(set(fields[2:]))) > 0:
               continue
           else:
               G.add_edge(fields[0], fields[1])


    with codecs.open(postid_edge_tagged, 'r', 'utf-8') as f:
       for line in f:
           fields = re.split('\t', line[0:-1])
           if '' not in fields[2:]:
                G.add_edge(fields[0], fields[1])


    conn_comp = sorted(nx.connected_components(G), key=len, reverse=True)
    print len(G.edges())
    print len(G.nodes())
    print len(conn_comp)
    print len(conn_comp[0])


def phone_statistics_from_int_phone(int_phones=path+'adj_lists/int-phones.jl', output=path+'connected-component-analysis/phone-stats-sorted.jl'):
    phone_counts = dict()
    with codecs.open(int_phones, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            phones = obj[obj.keys()[0]]
            for p in phones:
                if p not in phone_counts:
                    phone_counts[p] = 0
                phone_counts[p] += 1
    print 'finished populating phone counts'
    count_phone = dict()
    for k, v in phone_counts.items():
        if v not in count_phone:
            count_phone[v] = list()
        count_phone[v].append(k)
    print 'finished reversing dict'
    count_list = count_phone.keys()
    print len(count_list)
    count_list.sort(reverse=True)
    print 'finished sorting'
    out = codecs.open(output, 'w', 'utf-8')
    for k in count_list:
        answer = dict()
        answer[k] = count_phone[k]
        json.dump(answer, out)
        out.write('\n')
    out.close()


def postid_statistics_from_int_postid(int_phones=path+'adj_lists/int-postid.jl', output=path+'connected-component-analysis/postid-stats-sorted.jl'):
    phone_counts = dict()
    with codecs.open(int_phones, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            p = obj[obj.keys()[0]]
            if p not in phone_counts:
                phone_counts[p] = 0
            phone_counts[p] += 1
    print 'finished populating postid counts'
    count_phone = dict()
    for k, v in phone_counts.items():
        if v not in count_phone:
            count_phone[v] = list()
        count_phone[v].append(k)
    print 'finished reversing dict'
    count_list = count_phone.keys()
    count_list.sort(reverse=True)
    print 'finished sorting'
    out = codecs.open(output, 'w', 'utf-8')
    for k in count_list:
        answer = dict()
        answer[k] = count_phone[k]
        json.dump(answer, out)
        out.write('\n')
    out.close()


def phone_pruning_connected_components(phone_stats=path+'connected-component-analysis/network-profiling-data/cid6_analysis/edge-count-phone.jl',
                        tagged_edge_list=path+'connected-component-analysis/network-profiling-data/cid6_analysis/cid6-edge-list-tagged-phone',
                                       lower_bound=1):
    phone_counts = dict() # its actually counts indexing phones
    with codecs.open(phone_stats, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            p = obj[obj.keys()[0]]
            phone_counts[int(obj.keys()[0])] = set(p)
    phone_edge_list_dict = dict()
    with codecs.open(tagged_edge_list, 'r', 'utf-8') as f:
        for line in f:
            fields = re.split('\t',line[0:-1])
            edge = (fields[0], fields[1])
            for p in fields[2:]:
                if p not in phone_edge_list_dict:
                    phone_edge_list_dict[p] = set()
                phone_edge_list_dict[p].add(edge)
    x = list()
    y = list()



    counts = phone_counts.keys()
    counts.sort(reverse=True)
    print counts
    # print x
    # print y
    forbidden_phones = set()
    for i in range(0, len(counts)):
        if lower_bound is not None:
            if counts[i] < lower_bound:
                break

        x.append(counts[i])
        forbidden_phones = forbidden_phones.union(phone_counts[counts[i]])
        # print forbidden_phones
        G = nx.Graph()
        for p, edge_set in phone_edge_list_dict.items():
            if p in forbidden_phones:
                continue
            else:
               for u in edge_set:
                   G.add_edge(u[0], u[1])
        # print nx.info(G)
        ccs = sorted(nx.connected_components(G), key=len, reverse=True)
        if not ccs or len(ccs) == 0:
            y.append(0)
        else:
            y.append(len(ccs[0]))

    x = [x[0]+1]+x
    G = nx.Graph()

    for e in phone_edge_list_dict.values():
        for edge in e:
            G.add_edge(edge[0], edge[1])
    print nx.info(G)
    ccs = sorted(nx.connected_components(G), key=len, reverse=True)
    print len(ccs[0])
    y= [len(ccs[0])]+y

    print x
    print y

def exhaustive_phone_pruning_connected_components(
                        tagged_edge_list=path+'connected-component-analysis/network-profiling-data/cid6_analysis/cid6-edge-list-tagged-phone',
        out_file = path+'connected-component-analysis/network-profiling-data/cid6_analysis/cid6-exhaustive-phone-removal.tsv'
                                       ):

    phone_edge_list_dict = dict()
    with codecs.open(tagged_edge_list, 'r', 'utf-8') as f:
        for line in f:
            fields = re.split('\t',line[0:-1])
            edge = (fields[0], fields[1])
            for p in fields[2:]:
                if p not in phone_edge_list_dict:
                    phone_edge_list_dict[p] = set()
                phone_edge_list_dict[p].add(edge)
    phones = phone_edge_list_dict.keys()
    out = codecs.open(out_file, 'w', 'utf-8')
    print 'trying over ',str(len(phones)),' phones'
    out.write('phone\tnum. connected components\tlen. largest cc\n')
    for p in phones:
        G = nx.Graph()
        for ph, edge_set in phone_edge_list_dict.items():

            if p == ph:
                continue
            else:
                for u in edge_set:
                    G.add_edge(u[0], u[1])
    # print nx.info(G)
        ccs = sorted(nx.connected_components(G), key=len, reverse=True)
        out.write(p+'\t'+str(len(ccs))+'\t'+str(len(ccs[0]))+'\n')
        if len(ccs[0]) < 15000:
            print p+'\t'+str(len(ccs))+'\t'+str(len(ccs[0]))


    out.close()


def cid6_phone_edges_not_in_postid(tagged_phone_edge_list=path + 'connected-component-analysis/network-profiling-data/cid6_analysis/cid6-edge-list-tagged-phone',
        tagged_postid_edge_list=path + 'connected-component-analysis/network-profiling-data/cid6_analysis/cid6-edge-list-tagged-postid',
        out_file=path + 'connected-component-analysis/network-profiling-data/cid6_analysis/cid6-edge-list-tagged-phone-minus-postid'
):

    postid_edges = set()
    G = nx.Graph()
    with codecs.open(tagged_postid_edge_list, 'r', 'utf-8') as f:
        for line in f:
            fields = re.split('\t', line[0:-1])
            postid_edges.add(tuple(sorted([fields[0], fields[1]])))
    # ccs = sorted(nx.connected_components(G), key=len, reverse=True)
    # print len(ccs)
    # print len(ccs[0])
    # print nx.info(G)
    out = codecs.open(out_file, 'w', 'utf-8')
    with codecs.open(tagged_phone_edge_list, 'r', 'utf-8') as f:
        for line in f:
            fields = re.split('\t', line[0:-1])
            edge = sorted([fields[0], fields[1]])
            if tuple(edge) not in postid_edges:
                out.write(line)
                G.add_edge(fields[0], fields[1])

    out.close()
    print nx.info(G)
    ccs = sorted(nx.connected_components(G), key=len, reverse=True)
    print len(ccs)
    print len(ccs[0])


def cid6_incremental_edge_construction(edge_list=path+'connected-component-analysis/network-profiling-data/cid6_analysis/cid6-edge-list',
                                       out_file=path + 'connected-component-analysis/network-profiling-data/cid6_analysis/cid6-edge-list-incremental-stats.tsv'):
    G = nx.Graph()
    out = codecs.open(out_file, 'w', 'utf-8')
    out.write('edge_1\tedge_2\tnum.conn. comp.\tlen. largest cc\n')
    with codecs.open(edge_list, 'r', 'utf-8') as f:
        for line in f:
            fields = re.split('\t', line[0:-1])
            G.add_edge(fields[0], fields[1])
            ccs = sorted(nx.connected_components(G), key=len, reverse=True)
            out.write(fields[0]+'\t'+fields[1]+'\t'+str(len(ccs))+'\t'+str(len(ccs[0]))+'\n')

    out.close()

def ads_with_wrong_postids(ads_file = path+'data_for_memex.json', out_file_blank = path+'ads_with_blank_postids.tsv',
                           out_file_null=path + 'ads_with_null_postids.tsv'):
    out_blank = codecs.open(out_file_blank, 'w', 'utf-8')
    out_null = codecs.open(out_file_null, 'w', 'utf-8')
    out_blank.write('ad ID\tday\n')
    out_null.write('ad ID\tday\n')
    count1 = 0
    count2 = 0
    count3 = 0

    with codecs.open(ads_file, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            if not obj['post_id']:
                out_null.write(obj['_id'] + '\t' + str(obj['day']) + '\n')
                count2 += 1
            elif len(obj['post_id'])==0:
                out_blank.write(obj['_id']+'\t'+str(obj['day'])+'\n')
                count1 += 1

            else:
                count3 += 1
                continue

    out_blank.close()
    out_null.close()

    print 'ads with blank postids...',str(count1)
    print 'ads with missing postids...', str(count2)
    print 'ads with normal postids...',str(count3)


def analysis_of_incremental_edge_results(incremental_edge=path +
            'connected-component-analysis/network-profiling-data/cid6_analysis/cid6-edge-list-incremental-stats.tsv'):
    header = True
    x = list()
    y = list()
    z = list()
    count = 0
    with codecs.open(incremental_edge, 'r', 'utf-8') as f:
        for line in f:
            if header:
                header = False
                continue
            fields = re.split('\t', line[0:-1])
            x.append(count)
            count += 1
            y.append(int(fields[2]))
            z.append(int(fields[3]))
    print 'printing num. conn. comps. vs. edge index'
    plt.plot(x, y)
    plt.show()
    print 'printing largest conn. comp. size vs. edge index'
    plt.plot(x, z)
    plt.show()

def ablation_analysis_incremental_edge_results(incremental_edge=path +
            'connected-component-analysis/network-profiling-data/cid6_analysis/cid6-edge-list-incremental-stats.tsv'):
    header = True
    # count = 0
    prev = 0
    G = nx.Graph()
    with codecs.open(incremental_edge, 'r', 'utf-8') as f:
        for line in f:
            if header:
                header = False
                continue
            fields = re.split('\t', line[0:-1])
            if int(fields[3]) - prev > 1000:
                print prev
                print line
            else:
                G.add_edge(fields[0], fields[1])
            prev = int(fields[3])

    print nx.info(G)
    ccs = sorted(nx.connected_components(G), key=len, reverse=True)
    print len(ccs)
    print len(ccs[0])


def ablation_analysis_pref_nodes(incremental_edge=path +
            'connected-component-analysis/network-profiling-data/cid6_analysis/cid6-edge-list-incremental-stats.tsv'):
    header = True
    # count = 0
    # prev = 0
    G = nx.Graph()
    with codecs.open(incremental_edge, 'r', 'utf-8') as f:
        for line in f:
            if header:
                header = False
                continue
            if 'PREF' in line:
                continue
            fields = re.split('\t', line[0:-1])
            G.add_edge(fields[0], fields[1])
            # prev = int(fields[3])

    print nx.info(G)
    ccs = sorted(nx.connected_components(G), key=len, reverse=True)
    print len(ccs)
    print len(ccs[0])

def community_detection(edge_list=path+'connected-component-analysis/network-profiling-data/cid6_analysis/cid6-edge-list'):
    # too slow...
    from networkx.algorithms import community
    G = nx.read_edgelist(edge_list, delimiter='\t')
    communities_generator = community.girvan_newman(G)
    top_level_communities = next(communities_generator)
    next_level_communities = next(communities_generator)
    print len(sorted(map(sorted, next_level_communities)))

def edge_betweenness(edge_list=path+'connected-component-analysis/network-profiling-data/cid6_analysis/cid6-edge-list',
                     out_file=path+'connected-component-analysis/network-profiling-data/cid6_analysis/cid6-edge-betweenness-10000.json'):
    G = nx.read_edgelist(edge_list, delimiter='\t')
    M = nx.edge_betweenness_centrality(G, k=10000)
    M_new = dict()
    for k, v in M.items():
        M_new[k[0]+'\t'+k[1]] = v
    json.dump(M_new, open(out_file, 'w'))

def edge_betweenness_analysis(edge_bn=path+'connected-component-analysis/network-profiling-data/cid6_analysis/cid6-edge-betweenness-100.json'):
    edge_bn_dict = json.load(open(edge_bn, 'r'))
    reverse_dict = dict()
    for k, v in edge_bn_dict.items():
        if v not in reverse_dict:
            reverse_dict[v] = list()
        reverse_dict[v].append(k)

    vals = sorted(list(reverse_dict.keys()), reverse=True)
    # print vals[0:20]
    return reverse_dict

def ablation_edge_betweenness(edge_list=path+'connected-component-analysis/network-profiling-data/cid6_analysis/cid6-edge-list',
                        edge_bn=path + 'connected-component-analysis/network-profiling-data/cid6_analysis/cid6-edge-betweenness-10000.json'):
    # header = True
    count = 0
    # prev = 0

    # threshold = 0.0001
    edge_bn_dict = edge_betweenness_analysis(edge_bn)
    edge_bn_orig_dict = json.load(open(edge_bn, 'r'))
    vals = sorted(list(edge_bn_dict.keys()), reverse=True)
    sorted_list = list()
    x = list()
    y = list()
    z = list()
    for v in vals:
        sorted_list += edge_bn_dict[v]
        if count % 500 == 0:
            print count
            G = nx.read_edgelist(edge_list, delimiter='\t')
            for s in sorted_list:
                fields = re.split('\t', s)
                G.remove_edge(fields[0], fields[1])
            ccs = sorted(nx.connected_components(G), key=len, reverse=True)
            x.append(len(sorted_list))
            y.append(len(ccs))
            z.append(len(ccs[0]))
            if len(ccs[0]) < 20000:
                print [edge_bn_orig_dict[i] for i in edge_bn_dict[v]]
        count += 1
        if count > 100000:
            break

    print 'printing num. conn. comps. vs. number of edges removed'
    plt.plot(x, y)
    plt.show()
    print 'printing largest conn. comp. size vs. number of edges removed'
    plt.plot(x, z)
    plt.show()

    # print nx.info(G)


def phone_or_postid_pruning_network_construction(edge_list=
                path+'connected-component-analysis/network-profiling-data/cid6_analysis/cid6-edge-list',
            edge_phone_count=path+'connected-component-analysis/network-profiling-data/cid6_analysis/edge-count-phone.jl',
            phone_edge_list=path+'connected-component-analysis/network-profiling-data/cid6_analysis/cid6-edge-list-tagged-phone'):
    G = nx.read_edgelist(edge_list, delimiter='\t')
    print nx.info(G)
    threshold = 50
    count = 0
    forbidden_phones = set()
    with codecs.open(edge_phone_count, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            if int(obj.keys()[0]) >= threshold:
                forbidden_phones = forbidden_phones.union(set(obj[obj.keys()[0]]))
    with codecs.open(phone_edge_list, 'r', 'utf-8') as f:
        for line in f:
            fields = re.split('\t', line[0:-1])
            phones = set(fields[2:])
            if len(phones.intersection(forbidden_phones)) != 0:
                count += 1
                G.remove_edge(fields[0], fields[1])
    print str(count),' edges pruned from graph'
    ccs = sorted(nx.connected_components(G), key=len, reverse=True)
    print len(ccs)
    print len(ccs[0])


def common_ad_jaccard_pruning(edge_list=
                path+'connected-component-analysis-round2/network-profiling-data/cid6_analysis/cid6-edge-list',
            worker_ads_file = path+'connected-component-analysis-round2/network-profiling-data/cid6_analysis/worker-ads-int-dict.json'):
    """
    Remove edges if there is high edge jaccard similarity in terms of shared ads. The idea is that these edges
    will most likely collapse into singletons.
    :param edge_list:
    :param worker_ads_file:
    :return:
    """
    G = nx.read_edgelist(edge_list, delimiter='\t')
    worker_ints = json.load(open(worker_ads_file, 'r'))
    print nx.info(G)
    threshold = 0.0
    count = 0
    forbidden_phones = set()
    # with codecs.open(edge_phone_count, 'r', 'utf-8') as f:
    #     for line in f:
    #         obj = json.loads(line[0:-1])
    #         if int(obj.keys()[0]) >= threshold:
    #             forbidden_phones = forbidden_phones.union(set(obj[obj.keys()[0]]))
    # with codecs.open(phone_edge_list, 'r', 'utf-8') as f:
    #     for line in f:
    #         fields = re.split('\t', line[0:-1])
    #         phones = set(fields[2:])
    #         if len(phones.intersection(forbidden_phones)) != 0:
    #             count += 1
    #             G.remove_edge(fields[0], fields[1])
    H = nx.Graph()
    for e in G.edges:
        if e[0] not in worker_ints or e[1] not in worker_ints:
            raise Exception
        else:
            w1 = set(worker_ints[e[0]])
            w2 = set(worker_ints[e[1]])
            j = len(w1.intersection(w2)) * 1.0 / len(w1.union(w2))
            if j <= threshold:
                H.add_edge(e[0], e[1])
            else:
                count += 1
    print str(count),' edges pruned from graph'
    print nx.info(H)
    ccs = sorted(nx.connected_components(H), key=len, reverse=True)
    print len(ccs)
    print len(ccs[0])

def num_ad_pruning(edge_list=
                path+'connected-component-analysis-round2/network-profiling-data/cid6_analysis/cid6-edge-list',
            worker_ads_file = path+'connected-component-analysis-round2/network-profiling-data/cid6_analysis/worker-ads-int-dict.json'):
    """
    Remove an edge if either worker in the edge has more than 'threshold' ads
    :param edge_list:
    :param worker_ads_file:
    :return:
    """
    G = nx.read_edgelist(edge_list, delimiter='\t')
    worker_ints = json.load(open(worker_ads_file, 'r'))
    print nx.info(G)
    threshold = 16
    count = 0
    forbidden_phones = set()
    # with codecs.open(edge_phone_count, 'r', 'utf-8') as f:
    #     for line in f:
    #         obj = json.loads(line[0:-1])
    #         if int(obj.keys()[0]) >= threshold:
    #             forbidden_phones = forbidden_phones.union(set(obj[obj.keys()[0]]))
    # with codecs.open(phone_edge_list, 'r', 'utf-8') as f:
    #     for line in f:
    #         fields = re.split('\t', line[0:-1])
    #         phones = set(fields[2:])
    #         if len(phones.intersection(forbidden_phones)) != 0:
    #             count += 1
    #             G.remove_edge(fields[0], fields[1])
    H = nx.Graph()
    for e in G.edges:
        if e[0] not in worker_ints or e[1] not in worker_ints:
            raise Exception
        else:
            if len(worker_ints[e[0]]) < threshold and len(worker_ints[e[1]]) < threshold:
                H.add_edge(e[0], e[1])
            else:
                count += 1
    print str(count),' edges pruned from graph'
    print nx.info(H)
    ccs = sorted(nx.connected_components(H), key=len, reverse=True)
    print len(ccs)
    print len(ccs[0])


def worker_ads_count_stats(cc_folder=path+'connected-component-analysis-round2/connected-component-workers-old/'):
    files = glob.glob(cc_folder+'*.txt')
    count_dict = dict()
    total = 0
    count = 0
    for fi in files:
        if count % 50000 == 0:
            print count
        count += 1
        with codecs.open(fi, 'r') as f:
            counter = 0
            for line in f:
                l = len(set(re.split(' ', line[0:-1])))
                counter += 1
                if l not in count_dict:
                    count_dict[l] = 0
                count_dict[l] += 1
                total += l
            if counter != 1:
                print 'problems in file.' + f + '...more than one line...'
                raise Exception

    X = sorted(count_dict.keys())
    Y = list()
    for x in X:
        Y.append(count_dict[x])
    plt.loglog(X, Y)
    print 'average num. ads per worker is ',str(total*1.0/len(files))
    # print np.std(Y)
    plt.show()


def cid6_ads_count_stats(edge_list=
                path+'connected-component-analysis-round2/network-profiling-data/cid6_analysis/cid6-edge-list',
            worker_ads_file = path+'connected-component-analysis-round2/network-profiling-data/cid6_analysis/worker-ads-int-dict.json'):
    N = nx.read_edgelist(edge_list, delimiter='\t').nodes
    worker_ints = json.load(open(worker_ads_file, 'r'))
    count_dict = dict()
    total = 0
    count = 0

    for n in N:

        l = len(set(worker_ints[n]))
        if 'PREF' in n:
            count+= 1
            continue

        if l not in count_dict:
            count_dict[l] = 0
        count_dict[l] += 1
        total += l


    X = sorted(count_dict.keys())
    Y = list()
    for x in X:
        Y.append(count_dict[x])
    plt.loglog(X, Y)
    print 'average num. ads per worker in cid6 is ',str(total*1.0/(len(N)-count))
    # print np.std(Y)
    plt.show()


# ads_with_wrong_postids()
# phone_or_postid_pruning_network_construction()
# edge_betweenness()
# edge_betweenness_analysis()
# ablation_edge_betweenness()
# cid6_incremental_edge_construction()
# G = nx.read_edgelist(path+'connected-component-analysis/network-profiling-data/cid6_analysis/cid6-edge-list', delimiter='\t')
# print(len(sorted(nx.connected_components(G), key=len, reverse=True)))
# cid6_phone_edges_not_in_postid()
# conn_comp_analyses_after_phone_filtering()
# postid_statistics_from_int_postid()
# tag_edge_list_with_phones()
# tag_edge_list_with_postids()
# edge_counts_postid()
# postid_statistics_from_int_postid()
# conn_comp_analyses_postid_phone_composition()
# common_ad_jaccard_pruning()
# num_ad_pruning()
# worker_ads_count_stats()
cid6_ads_count_stats()