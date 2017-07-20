import codecs, re
import networkx as nx
from networkx import algorithms, assortativity
from networkx import info, density, degree_histogram
import matplotlib.pyplot as plt
from intersectPhones import compute_url_intersection_metrics
import math
from sklearn import linear_model
import numpy as np
import fasttext

def build_phone_network(input_file, phone_mapping_file=None):
    """
    Use for small-ish datasets. Will read in one of the phone network files (see strict-phones-network.txt as
    an example, add the edges to an undirected graph and return the graph.

    if phone mapping file is specified, we will use it to add phone nodes not in the input file (which only
    contains edges)
    :param input_file:
    :return:
    """
    edges = list()
    G = nx.Graph()
    if phone_mapping_file:
        with codecs.open(phone_mapping_file, 'r', 'utf-8') as f:
            first_line = True
            for line in f:
                if first_line:
                    first_line = False
                    continue # ignore header
                phone = re.split(' ',line[0:-1])[0]
                G.add_node(phone)

    with codecs.open(input_file, 'r', 'utf-8') as f:
        for line in f:
            fields = re.split('\t', line[0:-1])
            for i in range(0, len(fields)-1):
                for j in range(i+1, len(fields)):
                    edges.append((fields[i], fields[j]))

    # print len(edges)
    G.add_edges_from(edges)
    print len(G.nodes())
    print len(G.edges())
    return G


def output_undirected_node_centrality_statistics(input_file, output_file):
    G = build_phone_network(input_file)
    out = codecs.open(output_file, 'w')
    out.write('degree centrality '+str(algorithms.centrality.degree_centrality(G)))
    out.close()


def _tester():
    # think of this like a networkx scratchpad
    G = nx.Graph()  # this is an undirected graph
    G.add_edge(1, 2)
    G.add_edge(2, 3)
    G.add_node(4)
    print nx.degree(G)
    print nx.info(G)
    # print G.edges()


def read_phone_mapping_file(phone_mapping, header=True):
    phone_dict = dict()
    with codecs.open(phone_mapping, 'r', 'utf-8') as f:
        for line in f:
            if header:
                header = False
                continue
            else:
                fields = re.split(' ',line[0:-1])
                phone_dict[fields[0]] = int(fields[1])
    return phone_dict


def build_edge_list_mtx(input_file, mtx_file, node_mapping_file, write_node_mapping=False):
    """
    If write_node_mapping is True, then we will construct a node mapping and write it out to file. Otherwise,
    we'll read in the node mapping file.
    :param input_file:
    :param mtx_file:
    :param node_mapping_file:
    :param write_node_mapping:
    :return:
    """
    g = build_phone_network(input_file)
    node_mapping = dict()

    if write_node_mapping:
        count = 1
        for node in g.nodes():
            node_mapping[node] = count
            count += 1
    else:
        node_mapping=read_phone_mapping_file(node_mapping_file)
    out = codecs.open(mtx_file, 'w')
    out.write(str(len(g.nodes()))+' '+str(len(g.nodes()))+' '+str(len(g.edges()))+'\n')
    for edge in g.edges():
        out.write(str(node_mapping[edge[0]]) + ' ' + str(node_mapping[edge[1]]) + '\n')
    out.close()

    if write_node_mapping:
        out = codecs.open(node_mapping_file, 'w')
        out.write('phone integer-mapping \n')
        for k, v in node_mapping.items():
            out.write(str(k)+' '+str(v)+'\n')
        out.close()


def build_phone_integer_mapping_with_islet(old_phone_mapping_file, nebraska_phones, memex_phones, new_phone_mapping_file):
    answer = compute_url_intersection_metrics(nebraska_phones, memex_phones, True)
    out = codecs.open(new_phone_mapping_file, 'w', 'utf-8')
    count = -1 # to account for the header
    old_phone_map = read_phone_mapping_file(old_phone_mapping_file)
    with codecs.open(old_phone_mapping_file, 'r', 'utf-8') as f:
        for line in f:
            out.write(line)
            count += 1
    print 'old phone mapping file has num. nodes...',str(count)

    with codecs.open(memex_phones, 'r', 'utf-8') as f:
        for line in f:
            elements = re.split('\t', line[0:-1])
            if elements[0] not in answer:
                continue
            elif len(elements) > 2:
                continue
            else:
                if elements[1] not in old_phone_map:
                    count += 1
                    old_phone_map[elements[1]] = count
                    out.write(elements[1]+' '+str(count)+'\n')


    print 'new phone mapping file has num. nodes...', str(count)
    out.close()


def get_local_clustering_coefficient(phone_network_file, phone_mapping_file, output_graph=None):
    G = build_phone_network(phone_network_file, phone_mapping_file)
    cluster_coefficients = reverse_degree_dict(nx.clustering(G), output_file=
    '/media/mayankkejriwal/ExtraDrive1/Dropbox/memex-shared/networks/strict-phones-network-properties/cluster-coefficients.txt')
    ks = cluster_coefficients.keys()
    ks.sort(reverse=True)
    count = 0
    for k in ks:
        print k, cluster_coefficients[k]
        count += 1
        if count > 10:
            break


def get_degree_rank_graph(phone_network_file, phone_mapping_file, output_graph=None):
    G = build_phone_network(phone_network_file, phone_mapping_file)
    degree_sequence = sorted(nx.degree(G).values(), reverse=True)  # degree sequence
    # print "Degree sequence", degree_sequence
    # dmax = max(degree_sequence)

    plt.loglog(degree_sequence, 'b-', marker='o')
    plt.title("Degree rank plot")
    plt.ylabel("degree")
    plt.xlabel("rank")

    # draw graph in inset
    # plt.axes([0.45, 0.45, 0.45, 0.45])
    # Gcc = sorted(nx.connected_component_subgraphs(G), key=len, reverse=True)[0]
    # pos = nx.spring_layout(Gcc)
    # plt.axis('off')
    # nx.draw_networkx_nodes(Gcc, pos, node_size=20)
    # nx.draw_networkx_edges(Gcc, pos, alpha=0.4)

    if output_graph is not None:
        plt.savefig(output_graph)
    plt.show()


def reverse_degree_dict(node_degree_dict, output_file=None):
    reversed_dict = dict()
    for k, v in node_degree_dict.items():
        if v not in reversed_dict:
            reversed_dict[v] = list()
        reversed_dict[v].append(str(k))

    if output_file is not None:
        ks = reversed_dict.keys()
        ks.sort(reverse=True)
        out = codecs.open(output_file, 'w', 'utf-8')

        print 'highest degree in graph...',str(ks[0])
        for k in ks:
            out.write(str(k)+'\t')
            out.write('\t'.join(reversed_dict[k])+'\n')
        out.close()
    return reversed_dict


def get_degree_distribution_graph(phone_network_file, phone_mapping_file, output_graph=None):
    """

    :param phone_network_file:
    :param phone_mapping_file:
    :param output_graph:
    :return:
    """
    G = build_phone_network(phone_network_file, phone_mapping_file)
    # degree_sequence = sorted(nx.degree(G).values(), reverse=True)  # degree sequence
    # print "Degree sequence", degree_sequence
    # dmax = max(degree_sequence)
    degree_hist = nx.classes.function.degree_histogram(G)
    log_k = list()
    log_freq = list()
    total_sum = np.sum(degree_hist)
    print 'total sum of degree hist. list is...',str(total_sum)
    # degree_dict = dict()
    if degree_hist[0] > 0:
        log_k.append(0)
        log_freq.append(math.log(degree_hist[0]*1.0/total_sum))
    for i in range(1,len(degree_hist)):
        if degree_hist[i] == 0:
            continue
        else:
            log_k.append(math.log(i))
            log_freq.append(math.log(degree_hist[i]*1.0/total_sum))
    # plt.loglog(degree_sequence, 'b-', marker='o')
    regr = linear_model.LinearRegression()
    print len(log_k)
    print len(log_freq)
    # Train the model using the training sets
    regr.fit(np.array(log_k).reshape(-1, 1), np.array(log_freq).reshape(-1, 1))

    # The coefficients
    print('Coefficients: \n', regr.coef_)
    plt.plot(log_k, log_freq, 'ro')
    # plt.show()
    plt.title("Degree distribution plot")
    plt.ylabel("ln(Prob(k))")
    plt.xlabel("ln(k)")

    # draw graph in inset
    # plt.axes([0.45, 0.45, 0.45, 0.45])
    # Gcc = sorted(nx.connected_component_subgraphs(G), key=len, reverse=True)[0]
    # pos = nx.spring_layout(Gcc)
    # plt.axis('off')
    # nx.draw_networkx_nodes(Gcc, pos, node_size=20)
    # nx.draw_networkx_edges(Gcc, pos, alpha=0.4)

    if output_graph is not None:
        plt.savefig(output_graph)
    plt.show()


# _tester()
# path = '/Users/mayankkejriwal/datasets/memex/experiments-analysis/networks/'
root = '/media/mayankkejriwal/ExtraDrive1/Dropbox/memex-shared/networks/'
get_local_clustering_coefficient(root+'raw-data/strict-phones-network.txt',
                                 root+'strict-phones-network-properties/phone-integer-mappings-v2.txt')
# nebraska_path = '/Users/mayankkejriwal/datasets/memex/nebraska-data/memex_comm/'
# path = '/Users/mayankkejriwal/datasets/memex/experiments-analysis/'
# build_phone_integer_mapping_with_islet(
#     path+'networks/strict-phones-network-properties/phone-integer-mappings-v1.txt',
#         nebraska_path + 'phones-urls-2017.txt', path + '2017-strict-phones-urls.txt',
#     path+'networks/strict-phones-network-properties/phone-integer-mappings-v2.txt'       )
# compute_phone_graph_for_common_urls(nebraska_path+'phones-urls-2017.txt', memex_path+'2017-strict-phones-urls.txt', memex_path+'strict-phones-network.txt')
# build_edge_list_mtx(path+'raw-data/strict-phones-network.txt', path+'strict-phones-network-properties/edge-list.mtx',
#                     path+'strict-phones-network-properties/phone-integer-mappings-v1.txt')
# g = build_phone_network(path+'networks/raw-data/strict-phones-network.txt',path+'networks/strict-phones-network-properties/phone-integer-mappings-v2.txt')
# reverse_degree_dict(g.degree(), path+'networks/strict-phones-network-properties/node-degrees-v2.tsv')
# get_degree_distribution_graph(path+'networks/raw-data/strict-phones-network.txt',path+'networks/strict-phones-network-properties/phone-integer-mappings-v2.txt',
#                       path+'networks/strict-phones-network-properties/degree-distribution-v2.png')
# print assortativity.degree_assortativity_coefficient(g)
# print info(g)
# print density(g)
# out = codecs.open(path+'strict-phones-network-properties/edge-list.txt', 'w')
# for i in graph.edges():
#     out.write(str(i[0])+' '+str(i[1])+'\n')
# out.close()
# nx.draw(graph)
# plt.show()
# output_undirected_node_centrality_statistics(path+'raw-data/strict-phones-network.txt',
#                                         path+'strict-phones-network-properties/centrality.txt')

