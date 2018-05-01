import codecs, json, re
import networkx as nx
from dateutil.parser import parse

path = '/Users/mayankkejriwal/Dropbox/lorelei/'

def topic_AND_location_clustering(input_file=path+'datasets/haiti_reproc.json',
                              output_file_edgelist=path+'situation-clustering/haiti_reproc_topic_AND_location_edgelist.tsv'):
    """
    currently, I just write out the edge list
    :param input_file:
    :param output_file_edgelist:
    :return:
    """
    G = nx.Graph()
    location_id = dict()
    id_topics = dict()
    with codecs.open(input_file, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            node_id = obj['_id']
            G.add_node(node_id)
            obj = obj['_source']

            # both locations and topics must exist, otherwise this node will always be in a singleton cluster
            if 'topics' in obj and len(obj['topics']) > 0:
                topics = obj['topics']
                id_topics[node_id] = set(topics)
            else:
                continue

            if 'LOC' in obj and len(obj['LOC']) > 0:
                loc = obj['LOC']
                for l in loc:
                    if l not in location_id:
                        location_id[l] = list()
                    location_id[l].append(node_id)
            else:
                continue

    #let's construct graph edges
    for loc, id_list in location_id.items():
        id_list.sort()
        for i in range(0, len(id_list)-1):
            id_i = id_list[i]
            if id_i not in id_topics:
                continue
            for j in range(i+1, len(id_list)):
                id_j = id_list[j]
                if id_j not in id_topics:
                    continue
                if len(id_topics[id_i].intersection(id_topics[id_j])) > 0:
                    G.add_edge(id_i, id_j) # id_i and id_j share both a topic and location
    nx.write_edgelist(G, output_file_edgelist, delimiter='\t', data=False)


def topic_OR_location_clustering(input_file=path+'datasets/haiti_reproc.json',
                              output_file_edgelist=path+'situation-clustering/haiti_reproc_topic_OR_location_edgelist.tsv'):
    """
    currently, I just write out the edge list
    :param input_file:
    :param output_file_edgelist:
    :return:
    """
    G = nx.Graph()
    location_id = dict()
    topics_id = dict()
    with codecs.open(input_file, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            node_id = obj['_id']
            G.add_node(node_id)
            obj = obj['_source']

            # both locations and topics must exist, otherwise this node will always be in a singleton cluster
            if 'topics' in obj and len(obj['topics']) > 0:
                topics = obj['topics']
                for l in topics:
                    if l not in topics_id:
                        topics_id[l] = list()
                    topics_id[l].append(node_id)


            if 'LOC' in obj and len(obj['LOC']) > 0:
                loc = obj['LOC']
                for l in loc:
                    if l not in location_id:
                        location_id[l] = list()
                    location_id[l].append(node_id)


    #let's construct graph edges
    for loc, id_list in location_id.items():
        id_list.sort()
        for i in range(0, len(id_list)-1):
            id_i = id_list[i]

            for j in range(i+1, len(id_list)):
                id_j = id_list[j]
                G.add_edge(id_i, id_j) # id_i and id_j share both a topic and location

    for topic, id_list in topics_id.items():
        id_list.sort()
        for i in range(0, len(id_list) - 1):
            id_i = id_list[i]

            for j in range(i + 1, len(id_list)):
                id_j = id_list[j]
                G.add_edge(id_i, id_j)  # id_i and id_j share both a topic and location


    nx.write_edgelist(G, output_file_edgelist, delimiter='\t', data=False)


def connected_component_from_edge_list_file(input_file = path+'situation-clustering/haiti_reproc_topic_OR_location_edgelist.tsv',
                                            output_file = path+'situation-clustering/haiti_reproc_topic_OR_location_clusters.jsonl'):
    G = nx.read_edgelist(input_file)
    conn = sorted(nx.connected_components(G), key=len, reverse=True)
    count = 0
    out = codecs.open(output_file, 'w', 'utf-8')
    for c in conn:
        obj = dict()
        obj[str(count)] = list(c)
        json.dump(obj, out)
        out.write('\n')
        count += 1
        print len(c)

    out.close()

def topic_window_clustering(input_file=path+'datasets/haiti_reproc.json',
                              output_file=path+'situation-clustering/haiti_reproc_topic_window_clusters.jsonl'):
    """
    currently, window size is set at 20 documents or a single time stamp.
    :param input_file:
    :param output_file_edgelist:
    :return:
    """

    location_id = dict()
    topics_id = dict()
    count = 0
    time_dict = dict()
    count_time = 0
    with codecs.open(input_file, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            node_id = obj['_id']
            # G.add_node(node_id)
            obj = obj['_source']
            # print obj['originalText']
            # break

            # both locations and topics must exist, otherwise this node will always be in a singleton cluster
            if 'topics' in obj and len(obj['topics']) > 0:
                count += 1
                # topics = obj['topics']
                if parse(obj['createdAt']) not in time_dict:
                    time_dict[parse(obj['createdAt'])] = list()
                time_dict[parse(obj['createdAt'])].append(node_id)
                count_time += 1
                # print topics
                # for l in topics:
                #     if l not in topics_id:
                #         topics_id[l] = list()
                #     topics_id[l].append(node_id)
            # break


            # if 'LOC' in obj and len(obj['LOC']) > 0:
            #     loc = obj['LOC']
            #     for l in loc:
            #         if l not in location_id:
            #             location_id[l] = list()
            #         location_id[l].append(node_id)


    print count
    print count_time
    times = time_dict.keys()
    times.sort()
    clusters = dict()
    count = 0
    forbidden = set()
    for t in range(0, len(times)):
        if t in forbidden:
            continue
        if len(time_dict[times[t]]) >= 20:
            clusters[str(count)] = time_dict[times[t]]
            count += 1
            continue
        else:
            # tmp_count = 0
            new_list = list()
            new_list += time_dict[times[t]]
            # tmp_count = len(new_list)
            flag = False
            for k in range(t+1, len(times)):
                new_list += time_dict[times[k]]
                tmp_count = len(new_list)
                forbidden.add(k)
                if tmp_count >= 20:
                    clusters[str(count)] = new_list
                    count += 1
                    flag = True
                    break
            if not flag:
                print 'entered exit flag gate'
                clusters[str(count)] = new_list
                count += 1

    print len(clusters.keys())
    out = codecs.open(output_file, 'w', 'utf-8')
    for c in clusters:
        obj = dict()
        obj[c] = clusters[c]
        json.dump(obj, out)
        out.write('\n')


    out.close()


def sanity_check_window_clusters(input_file=path+'situation-clustering/haiti_reproc_topic_window_clusters.jsonl'):
    occurred = set()
    count = 0
    with codecs.open(input_file, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])

            for i in obj[obj.keys()[0]]:
                if i in occurred:
                    print i
                    raise Exception
                else:
                    count += 1
                    occurred.add(i)
    print count
    print len(occurred)


def serialize_clusters_human_readable(input_file=path+'situation-clustering/haiti_reproc_topic_window_clusters.jsonl',
                                      original_file=path+'datasets/haiti_reproc.json',
                                      output_folder=path+'situation-clustering/jsonl-clusters/'):
    node_dict = dict()
    with codecs.open(original_file, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            node_id = obj['_id']
            node_dict[node_id] = obj
    print 'finished reading in original file...'
    with codecs.open(input_file, 'r', 'utf-8') as f:

        for line in f:
            obj = json.loads(line[0:-1])
            cluster_id = obj.keys()[0]
            doc_ids = obj[cluster_id]
            out = codecs.open(output_folder+cluster_id+".jsonl", 'w', 'utf-8')
            for docid in doc_ids:
                json.dump(node_dict[docid], out)
                out.write('\n')
            out.close()


# topic_OR_location_clustering()
serialize_clusters_human_readable()
# topic_window_clustering()
# sanity_check_window_clusters()
