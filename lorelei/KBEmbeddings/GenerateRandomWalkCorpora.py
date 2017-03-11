from Geonames import *
from numpy.random import *

def generate_truncated_random_walks(integer_nodes_set, weighted_adj_graph_file, output_file,
                                    samples_per_node=5, depth_per_node=10):
    """
    The samples will always be with replacement. Specifically, for each node in integer_nodes_set, we
    generate 'samples_per_node' samples by sampling according to the probability distribution with repl. The depth
    per node dictates how far we are willing to traverse the graph, up to that depth (may be shorter).
    :param integer_nodes_set:
    :param weighted_adj_graph:
    :param output_file:
    :param samples_per_node:
    :param depth_per_node: must be at least 2 to 'walk' the graph. We don't cover quirky base cases here.
    :return: output will be written out as a space-delimited sequence of nodes. This is to make it easy for
    word2vec.
    """
    graph = read_weighted_adj_graph(weighted_adj_graph_file)
    print 'finished reading graph.'
    nodes_in_graph = set(graph.keys())
    out = codecs.open(output_file, 'w', 'utf-8')
    count = 1
    for node in integer_nodes_set:
        if count % 100 == 0:
            print count
        # if count % 10000 == 0:
        #     break

        if node not in nodes_in_graph:
            pass
            # print 'node not in graph with outdegree at least 1...',str(node)
        else:
            samples = choice(graph[node][0], samples_per_node, True, graph[node][1]).tolist()
            for sample in samples:
                walk = list()
                walk.append(node)
                walk.append(sample)
                generate_random_walk(walk, graph, depth_per_node-1)
                write_string = ''
                for i in range(0, len(walk)):
                    write_string += (str(walk[i])+' ')
                write_string = write_string[0:-1]+'\n'
                out.write(write_string)
        count += 1
    out.close()


def generate_random_walk(list_of_nodes, graph, depth):
    if depth <= 1:
        return
    node = list_of_nodes[-1]
    if node not in graph:
        return
    else:
        sample = choice(graph[node][0], 1, True, graph[node][1]).tolist()
        list_of_nodes += sample
        generate_random_walk(list_of_nodes, graph, depth-1)


# path = '/Users/mayankkejriwal/datasets/lorelei/KB-CIA/'
# integer_nodes = get_country_integer_nodes(path+'populated_places_countries.tsv',path+'mapped_populated_places.txt', None)
# generate_truncated_random_walks(integer_nodes, path+'prob_adjacency_file_1.tsv', path+'walks_trial3_adj_file_1.txt')