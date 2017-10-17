import networkx as nx
import json

path='/Users/mayankkejriwal/datasets/schemaOrg/Hospital2014/'

def construct_netloc_graph(max_index_blank=10510, max_index_id=13480, max_index_netloc=13539,
                           edge_list=path+'schema_Hospital-2014-netloc.edgelist',
                           dictionary=path+'schema_Hospital-2014-netloc.dictionary'):
    G = nx.read_edgelist(edge_list, nodetype=int)
    G.add_nodes_from(range(0, max_index_netloc))
    id_blank_dict = dict() # id node references set of blank nodes
    blank_netloc_dict = dict() # blank node references set of netloc nodes


    #USE FOR DEBUGGING

    # node_dict = json.load(open(dictionary, 'r'))
    # reversed_dict = dict()
    # for k, v in node_dict.items():
    #     reversed_dict[v] = k
    #
    # print reversed_dict[10509]
    # print len(node_dict.keys())


    print nx.info(G)
    for e in G.edges():
        l = list()
        l.append(int(e[0]))
        l.append(int(e[1]))
        l.sort()  # in ascending order
        el1 = l[0]
        el2 = l[1]
        if el1 < max_index_blank and (el2 >= max_index_blank and el2 < max_index_id): # el1 is a blank and el2 is an id
            if el2 not in id_blank_dict:
                id_blank_dict[el2] = set()
            id_blank_dict[el2].add(el1) # tricky. I think I've got the order right.
        if el1 < max_index_blank and (el2 >= max_index_id and el2 < max_index_netloc): # el1 is a blank and el2 is a netloc
            if el1 not in blank_netloc_dict:
                blank_netloc_dict[el1] = set()
            blank_netloc_dict[el1].add(el2) # tricky. I think I've got the order right.

    H = nx.Graph()
    for node_id, set_blank in id_blank_dict.items():
        set_netloc = set()
        for b in set_blank:
            if b in blank_netloc_dict:
                set_netloc = set_netloc.union(blank_netloc_dict[b])
        if len(set_netloc) > 0:
            list_netloc = list(set_netloc)
            list_netloc.sort()
            H.add_nodes_from(list_netloc)
            for i in range(0, len(list_netloc)-1):
                for j in range(i+1, len(list_netloc)):
                    H.add_edge(list_netloc[i], list_netloc[j])
    print nx.info(H)


def construct_id_graph(max_index_blank=10510, max_index_id=13480, max_index_netloc=13539,
                           edge_list=path + 'schema_Hospital-2014-netloc.edgelist',
                           dictionary=path + 'schema_Hospital-2014-netloc.dictionary'):
    G = nx.read_edgelist(edge_list, nodetype=int)
    G.add_nodes_from(range(0, max_index_netloc))
    blank_id_dict = dict()  # blank node references set of id nodes
    netloc_id_dict = dict()  # netloc node references set of id nodes

    # USE FOR DEBUGGING

    # node_dict = json.load(open(dictionary, 'r'))
    # reversed_dict = dict()
    # for k, v in node_dict.items():
    #     reversed_dict[v] = k
    #
    # print reversed_dict[10509]
    # print len(node_dict.keys())


    print nx.info(G)
    for e in G.edges():
        l = list()
        l.append(int(e[0]))
        l.append(int(e[1]))
        l.sort()  # in ascending order
        el1 = l[0]
        el2 = l[1]
        if el1 < max_index_blank and (el2 >= max_index_blank and el2 < max_index_id):  # el1 is a blank and el2 is an id
            if el1 not in blank_id_dict:
                blank_id_dict[el1] = set()
            blank_id_dict[el1].add(el2)  # tricky. I think I've got the order right.

    for e in G.edges():
        l = list()
        l.append(int(e[0]))
        l.append(int(e[1]))
        l.sort()  # in ascending order
        el1 = l[0]
        el2 = l[1]
        if el1 < max_index_blank and (el2 >= max_index_id and el2 < max_index_netloc):  # el1 is a blank and el2 is a netloc
            if el1 not in blank_id_dict:
                continue
            ids = blank_id_dict[el1]
            if el2 not in netloc_id_dict:
                netloc_id_dict[el2] = set()
            netloc_id_dict[el2] = netloc_id_dict[el2].union(ids)

    H = nx.Graph()
    for netloc, ids in netloc_id_dict.items():
        # set_netloc = set()
        # for b in set_blank:
        #     if b in blank_netloc_dict:
        #         set_netloc = set_netloc.union(blank_netloc_dict[b])
        if len(ids) > 0:
            list_ids = list(ids)
            list_ids.sort()
            H.add_nodes_from(list_ids)
            for i in range(0, len(list_ids) - 1):
                for j in range(i + 1, len(list_ids)):
                    H.add_edge(list_ids[i], list_ids[j])
    print nx.info(H)


# construct_netloc_graph()
construct_id_graph()
