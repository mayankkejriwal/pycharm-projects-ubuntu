import codecs
import re
import json
import random

def map_tab_nt_nodes_to_integers(tab_nt_file, output_file):
    """
    The 'nt' file is actually a 3-element tsv file. The output file is also a tsv with only two fields:
    a node, and an integer. We start integers from 1, and increment them as we encounter new nodes. This way
    the mapping is well defined
    :param tab_nt_file:
    :param output_file:
    :return:
    """
    count = 1
    current_node = 1
    mapping_dict = dict()
    with codecs.open(tab_nt_file, 'r', 'utf-8') as f:
        for line in f:
            count += 1
            # if count > 100:
            #     break
            fields = re.split('\t',line[0:-1])
            subject = fields[0]
            object = fields[2]
            if subject not in mapping_dict:
                mapping_dict[subject] = current_node
                current_node += 1
            if object not in mapping_dict:
                mapping_dict[object] = current_node
                current_node += 1
    out = codecs.open(output_file, 'w', 'utf-8')
    for k, v in mapping_dict.items():
        out.write(k+'\t'+str(v)+'\n')
    out.close()

def build_adjacency_list(mapped_file, tab_nt_file, output_file, directed=False):
    """
    Note that although we do not consider edge labels we will include a node mapping twice if it links
    to another node with two different edges, since the goal is to perform random walks.
    :param mapped_file: The output from one of the map nodes file
    :param tab_nt_file: the tab input file
    :param output_file: Simply a json. Each value is a list of mapped integers.
    :return:
    """
    mapping_dict = read_in_mapped_file(mapped_file)
    adjacency_list = dict()
    with codecs.open(tab_nt_file, 'r', 'utf-8') as f:
        for line in f:
            # count += 1
            # if count > 100:
            #     break
            fields = re.split('\t',line[0:-1])
            subject_int = mapping_dict[fields[0]]
            object_int = mapping_dict[fields[2]]
            if subject_int not in adjacency_list:
                adjacency_list[subject_int] = list()
            adjacency_list[subject_int].append(object_int)
            if not directed:
                if object_int not in adjacency_list:
                    adjacency_list[object_int] = list()
                adjacency_list[object_int].append(subject_int)
    json.dump(adjacency_list, codecs.open(output_file, 'w'))


def read_in_mapped_file(mapped_file):
    mapping_dict = dict()
    with codecs.open(mapped_file, 'r', 'utf-8') as f:
        for line in f:
            line = line[0:-1]
            mapping_dict[re.split('\t', line)[0]] = int(re.split('\t', line)[1])
    return mapping_dict


def random_walks_on_adjacency_list(adjacency_list_file, output_file, num_walks=100, random_walk_len=10):
    adjacency_dict = json.load(codecs.open(adjacency_list_file, 'r'))
    nodes = adjacency_dict.keys()
    out = codecs.open(output_file, 'w', 'utf-8')
    count = 1
    for node in nodes:

        # node = nodes[random.randint(0, len(nodes)-1)]
        for i in range(num_walks):
            rwalk = dict()
            rwalk['r'+str(count)+'-'+str(i)] = perform_random_walk_from_node(node, adjacency_dict, random_walk_len)
            json.dump(rwalk, out)
            out.write('\n')
        count += 1
        if count % 1000==0:
            print 'completed random walk...',str(count)
    out.close()


def perform_random_walk_from_node(node, adjacency_dict, random_walk_len):
    """
    Walks will only be UP TO len. If the walk terminates in a node that has no outgoing nodes, the walk
    terminates with that node.
    :param node:
    :param adjacency_dict:
    :param random_walk_len:
    :return:
    """
    answer = list()
    new_node = int(node) # this is (or should be) guaranteed to exist in adjacency_dict
    answer.append(new_node)
    for i in range(random_walk_len):
        new_node = str(new_node)
        if new_node not in adjacency_dict:
            break
        k = random.randint(0, len(adjacency_dict[new_node])-1)
        new_node = adjacency_dict[new_node][k]
        answer.append(new_node)
    return answer



# path = '/Users/mayankkejriwal/datasets/lorelei/KB-CIA/'
# random_walks_on_adjacency_list(path+'CIA-undir-adj-list.json', path+'CIA-undir-RW10.jl')
# build_adjacency_list(path+'CIA-node-mapping.tsv',path+'KB_CIA.nt',path+'CIA-dir-adj-list.json',True)
# map_tab_nt_nodes_to_integers(path+'KB_CIA.nt', path+'CIA-node-mapping.tsv')