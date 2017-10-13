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



path = '/Users/mayankkejriwal/datasets/memex/nebraska-data/data-sharing/'

def generate_worker_ids(data_for_memex=path+'data_for_memex.json', output_file=path+'adj_lists/worker-ids.jl'):
    out = codecs.open(output_file, 'w', 'utf-8')
    postid_none = 0
    noname = 0
    nophone = 0
    with codecs.open(data_for_memex, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])  # exclude newline
            id = obj['_id']
            if 'name' not in obj or len(obj['name']) == 0:
                names = ['no_name']
                noname += 1
            else:
                names = obj['name']


            if obj['post_id'] is None:
                postid_none += 1
                post_id = ['NA']
            else:
                post_id = [obj['post_id']]

            # if type(post_id) is None:
            #     print obj

            if 'phone' not in obj or len(obj['phone']) == 0:
                phones = ['NA']
                nophone += 1
            else:
                phones = obj['phone']
            worker_ids = dict()
            try:
                worker_ids[id] = form_cross_product(names, phones, post_id)
            except:
                # print names
                # print phones
                # print post_id
                print obj
            json.dump(worker_ids, out)
            out.write('\n')

    out.close()
    print 'number of objects with no post ids...',str(postid_none)
    print 'number of objects with no phone...', str(nophone)
    print 'number of objects with no name...', str(noname)


def generate_name_postid_file(input_file=path+'adj_lists/worker-ids.jl', output_file=path+'adj_lists/name_postid.jl',
                              ignore_noname='True'):
    answer_dict = dict()
    count = 0
    with codecs.open(input_file, 'r', 'utf-8') as f:
        for line in f:
            count += 1
            if count % 100000 == 0:
                print 'processed line...',str(count)
            obj = json.loads(line[0:-1])  # exclude newline
            k = obj.keys()[0]
            v = obj.values()[0]
            for n in v:
                items = re.split('-',n)
                if items[0] == 'no_name' or items[2] == 'NA':
                    continue
                else:
                    if items[0] not in answer_dict:
                        answer_dict[items[0]] = dict()
                    if items[2] not in answer_dict[items[0]]:
                        answer_dict[items[0]][items[2]] = list()
                    answer_dict[items[0]][items[2]].append(k)
    out = codecs.open(output_file, 'w', 'utf-8')
    for k, v in answer_dict.items():
        d = dict()
        d[k] = v
        json.dump(d, out)
        out.write('\n')
    out.close()

def generate_name_phone_file(input_file=path+'adj_lists/worker-ids.jl', output_file=path+'adj_lists/name_phone.jl',
                              ignore_noname='True'):
    answer_dict = dict()
    count = 0
    with codecs.open(input_file, 'r', 'utf-8') as f:
        for line in f:
            count += 1
            if count % 100000 == 0:
                print 'processed line...',str(count)
            obj = json.loads(line[0:-1])  # exclude newline
            k = obj.keys()[0]
            v = obj.values()[0]
            for n in v:
                items = re.split('-',n)
                if items[0] == 'no_name' or items[1] == 'NA':
                    continue
                else:
                    if items[0] not in answer_dict:
                        answer_dict[items[0]] = dict()
                    if items[1] not in answer_dict[items[0]]:
                        answer_dict[items[0]][items[1]] = list()
                    answer_dict[items[0]][items[1]].append(k)
    out = codecs.open(output_file, 'w', 'utf-8')
    for k, v in answer_dict.items():
        d = dict()
        d[k] = v
        json.dump(d, out)
        out.write('\n')
    out.close()


def form_cross_product(names, phones, post_id):
    answer = list()
    for n in names:
        for p in phones:
            for i in post_id:
                answer.append(n+'-'+p+'-'+i)
    return answer


def construct_adj_list_phone_postid(input_file_phone=path+'adj_lists/name_phone.jl',
                            input_file_postid=path+'adj_lists/name_postid.jl',
                            id_int_file = path+'adj_lists/id-int-mapping.tsv',
                             output_file_phone=path+'adj_lists/adj_list_phone.txt',
                            output_file_postid=path + 'adj_lists/adj_list_postid.txt'):
    """
    Careful, this is not a true adj list file. we just want to make sure we generate a file such that we
    recover the connected components.
    :param input_file_phone:
    :param input_file_postid:
    :param id_int_file:
    :param output_file_phone:
    :param output_file_postid:
    :return:
    """

    # with codecs.open(input_file, 'r') as f:
    #     for line in f:
    #         obj = json.loads(line[0:-1])
    #         # if count % 100000 == 0:
    #         #     print 'processed line...',str(count)
    #         print obj.values()[0]
    #         exit(-1)

    id_int_dict = dict()
    with codecs.open(id_int_file, 'r') as f:
        for line in f:
            items = re.split('\t',line[0:-1])
            id_int_dict[items[0]] = int(items[1])

    print 'finished reading in id_int_dict file'
    count = 0
    out = codecs.open(output_file_phone, 'w', 'utf-8')
    with codecs.open(input_file_phone, 'r') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            count += 1
            if count % 1000 == 0:
                print 'processed line...',str(count)
            for k, v in obj.values()[0].items():
                    if len(v) <= 1:
                        continue
                    else:
                        new_list = list()
                        for g in v:
                            new_list.append(id_int_dict[g])
                        new_list.sort()
                        n_new_list = [str(j) for j in new_list]
                        out.write(str(n_new_list[0])+' '+' '.join(n_new_list[1:])+'\n') # this is all that is necessary for cc alg.

    out.close()
    print 'finished generating phone adj list file'
    count = 0
    out = codecs.open(output_file_postid, 'w', 'utf-8')
    with codecs.open(input_file_postid, 'r') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            count += 1
            if count % 1000 == 0:
                print 'processed line...', str(count)
            for k, v in obj.values()[0].items():
                if len(v) <= 1:
                    continue
                else:
                    new_list = list()
                    for g in v:
                        new_list.append(id_int_dict[g])
                    new_list.sort()
                    n_new_list = [str(j) for j in new_list]
                    out.write(str(n_new_list[0]) + ' ' + ' '.join(n_new_list[1:]) + '\n')

    out.close()
    print 'finished generating postid adj list file'


def build_general_adj_list(data_for_memex, val_key='post_id', is_list=False):
    adj_dict = dict()
    with codecs.open(data_for_memex, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])  # exclude newline
            if val_key not in obj or not obj[val_key]:
                continue

            else:

                if obj['_id'] not in adj_dict:
                    adj_dict[obj['_id']] = list()
                if is_list is True:
                    adj_dict[obj['_id']] += obj[val_key]
                else:
                    adj_dict[obj['_id']].append(obj[val_key])


def build_name_adj_list(data_for_memex=path+'data_for_memex.json', output_file=path+'adj_lists/name.jl'):
    """
    different from build_general_adj_list because we are using name for key
    We print out not_name_count (num jsons where there is no name) and name_count (num. unique names) as well
    as obj_name_count which is the number of jsons with at least one name.

    :param data_for_memex:
    :return:
    """
    name_adj_dict = dict()
    name_count = 0
    obj_name_count = 0
    not_name_count = 0
    count = 0
    with codecs.open(data_for_memex, 'r', 'utf-8') as f:

        for line in f:

            obj = json.loads(line[0:-1]) # exclude newline
            if 'name' not in obj or len(obj['name'])==0:
                not_name_count += 1
                continue
            else:
                obj_name_count += 1
                for name in obj['name']:
                    name_count += 1
                    if name not in name_adj_dict:
                        name_adj_dict[name] = list()
                    name_adj_dict[name].append(obj['_id'])
            count += 1
            if count % 100000 == 0:
                print 'processed ',str(count),' jsons'

    print 'num jsons where there is no name...',str(not_name_count)
    print 'num unique names...', str(name_count)
    print 'num jsons with at least one name...', str(obj_name_count)
    out = codecs.open(output_file, 'w')
    for k, v in name_adj_dict.items():
        ans = dict()
        ans[k] = v
        json.dump(ans, out)
        out.write('\n')
    out.close()


def build_name_and_postid_or_phone_adj_list(data_for_memex=path+'data_for_memex.json', output_file=path+'adj_lists/worker-network.jl'):
    """
    different from build_general_adj_list because we are using name+post-id for ID
    We print out not_name_count (num jsons for which an ID could not be created because of non-existence or null in one
     of the fields) and name_count (num. unique IDs) as well
    as obj_name_count which is the number of jsons with at least one name AND at least one postid.

    :param data_for_memex:
    :return:
    """
    name_adj_dict = dict()
    # name_count = 0
    obj_name_count = 0
    not_name_count = 0
    count = 0
    with codecs.open(data_for_memex, 'r', 'utf-8') as f:

        for line in f:

            obj = json.loads(line[0:-1]) # exclude newline
            if 'name' not in obj or len(obj['name'])==0:
                not_name_count += 1
                continue
            else:
                if ('post_id' not in obj or not obj['post_id']) and ('phone' not in obj or not obj['phone']):
                    not_name_count += 1
                    continue
                elif 'post_id' in obj and obj['post_id'] and ('phone' not in obj or not obj['phone']):

                    obj_name_count += 1
                    postid = obj['post_id']
                    for n in obj['name']:
                        name = n+'-'+postid
                        # name_count += 1
                        if name not in name_adj_dict:
                            name_adj_dict[name] = list()
                        name_adj_dict[name].append(obj['_id'])
                elif 'phone' in obj and obj['phone'] and ('post_id' not in obj or not obj['post_id']):
                    obj_name_count += 1
                    phone = obj['phone']
                    for n in obj['name']:
                        for p in phone:
                            name = n + '-' + str(p)
                            # name_count += 1
                            if name not in name_adj_dict:
                                name_adj_dict[name] = list()
                            name_adj_dict[name].append(obj['_id'])
                elif 'phone' in obj and obj['phone'] and 'post_id' in obj and obj['post_id']:
                    obj_name_count += 1
                    phone = obj['phone']
                    for n in obj['name']:
                        for p in phone:
                            name = n + '-' + str(p) + '-' + obj['post_id']
                            # name_count += 1
                            if name not in name_adj_dict:
                                name_adj_dict[name] = list()
                            name_adj_dict[name].append(obj['_id'])
                else:
                    raise Exception
            count += 1
            if count % 100000 == 0:
                print 'processed ',str(count),' jsons'


    out = codecs.open(output_file, 'w')
    for k, v in name_adj_dict.items():
        ans = dict()
        ans[k] = v
        json.dump(ans, out)
        out.write('\n')
    out.close()
    print 'num jsons where no id could be generated...', str(not_name_count)
    print 'num unique ids...', str(len(name_adj_dict.keys()))
    print 'num jsons with non-null ID...', str(obj_name_count)


def build_name_phone_adj_list(data_for_memex=path+'data_for_memex.json', output_file=path+'adj_lists/name-phone.jl'):
    """
   different from build_general_adj_list because we are using name+phone for ID. for multiple names and phones
   we use the cross product to produce the set of IDs for a given ad.
    We print out not_name_count (num jsons for which an ID could not be created because of non-existence or null in one
     of the fields) and name_count (num. unique IDs) as well
    as obj_name_count which is the number of jsons with at least one name AND at least one phone.

    :param data_for_memex:
    :return:
    """
    name_adj_dict = dict()
    # name_count = 0
    obj_name_count = 0
    not_name_count = 0
    count = 0
    with codecs.open(data_for_memex, 'r', 'utf-8') as f:

        for line in f:

            obj = json.loads(line[0:-1]) # exclude newline
            if 'name' not in obj or len(obj['name'])==0 or 'phone' not in obj or len(obj['phone'])==0 :
                not_name_count += 1
                continue
            else:
                obj_name_count += 1
                phone = obj['phone']
                for n in obj['name']:
                  for p in phone:
                    name = n+'-'+str(p)
                    # name_count += 1
                    if name not in name_adj_dict:
                        name_adj_dict[name] = list()
                    name_adj_dict[name].append(obj['_id'])
            count += 1
            if count % 100000 == 0:
                print 'processed ',str(count),' jsons'


    out = codecs.open(output_file, 'w')
    for k, v in name_adj_dict.items():
        ans = dict()
        ans[k] = v
        json.dump(ans, out)
        out.write('\n')
    out.close()
    print 'num jsons where there is no name or no phone...', str(not_name_count)
    print 'num unique name-phone pairs...', str(len(name_adj_dict.keys()))
    print 'num jsons with at least one name AND at least one phone...', str(obj_name_count)


def build_name_phone_postid_adj_list(data_for_memex=path+'data_for_memex.json', output_file=path+'adj_lists/name-phone-postid.jl'):
    """
    This is an AND adj. list and is different from the name AND (phone OR postid)
   different from build_general_adj_list because we are using name+phone+postid for ID.
    We print out not_name_count (num jsons for which an ID could not be created because of non-existence or null in one
     of the fields) and name_count (num. unique IDs) as well
    as obj_name_count which is the number of jsons with at least one name AND at least one phone AND postID.

    :param data_for_memex:
    :return:
    """
    name_adj_dict = dict()
    # name_count = 0
    obj_name_count = 0
    not_name_count = 0
    count = 0
    with codecs.open(data_for_memex, 'r', 'utf-8') as f:

        for line in f:

            obj = json.loads(line[0:-1]) # exclude newline
            if 'name' not in obj or len(obj['name'])==0 or 'phone' not in obj or len(obj['phone'])==0 \
                    or 'post_id' not in obj or not obj['post_id']:
                not_name_count += 1
                continue
            else:
                obj_name_count += 1
                phone = obj['phone']
                for n in obj['name']:
                  for p in phone:
                    name = n+'-'+str(p)+'-'+obj['post_id']
                    # name_count += 1
                    if name not in name_adj_dict:
                        name_adj_dict[name] = list()
                    name_adj_dict[name].append(obj['_id'])
            count += 1
            if count % 100000 == 0:
                print 'processed ',str(count),' jsons'


    out = codecs.open(output_file, 'w')
    for k, v in name_adj_dict.items():
        ans = dict()
        ans[k] = v
        json.dump(ans, out)
        out.write('\n')
    out.close()
    print 'num jsons where there is no name or no phone or no postid...', str(not_name_count)
    print 'num unique name-phone-postid pairs...', str(len(name_adj_dict.keys()))
    print 'num jsons with at least one name AND at least one phone AND postid...', str(obj_name_count)


def build_name_postid_adj_list(data_for_memex=path+'data_for_memex.json', output_file=path+'adj_lists/name-phone-postid.jl'):
    """


    :param data_for_memex:
    :return:
    """
    name_adj_dict = dict()
    # name_count = 0
    obj_name_count = 0
    not_name_count = 0
    count = 0
    with codecs.open(data_for_memex, 'r', 'utf-8') as f:

        for line in f:

            obj = json.loads(line[0:-1]) # exclude newline
            if 'name' not in obj or len(obj['name'])==0 or 'post_id' not in obj or not obj['post_id']:
                not_name_count += 1
                continue
            else:
                obj_name_count += 1
                phone = obj['post_id']
                for n in obj['name']:

                    name = n+'-'+obj['post_id']
                    # name_count += 1
                    if name not in name_adj_dict:
                        name_adj_dict[name] = list()
                    name_adj_dict[name].append(obj['_id'])
            count += 1
            if count % 100000 == 0:
                print 'processed ',str(count),' jsons'


    out = codecs.open(output_file, 'w')
    for k, v in name_adj_dict.items():
        ans = dict()
        ans[k] = v
        json.dump(ans, out)
        out.write('\n')
    out.close()
    print 'num jsons where there is no name or no postid...', str(not_name_count)
    print 'num unique name-postid pairs...', str(len(name_adj_dict.keys()))
    print 'num jsons with at least one name AND at least one postid...', str(obj_name_count)


def build_id_integer_mappings(data_for_memex=path+'data_for_memex.json', id_int_output=path+'adj_lists/id-int-mapping.tsv',
                             int_id_output=path+'adj_lists/int-id-mapping.tsv'):
    count = 1
    out_id_int = codecs.open(id_int_output, 'w')
    out_int_id = codecs.open(int_id_output, 'w')
    with codecs.open(data_for_memex, 'r', 'utf-8') as f:
        for line in f:
            id = json.loads(line[0:-1])['_id']
            out_id_int.write(id+'\t'+str(count)+'\n')
            out_int_id.write(str(count)+'\t'+id+'\n')
            count += 1

    out_id_int.close()
    out_int_id.close()


def serialize_as_list_of_sorted_ints(adj_file=path+'adj_lists/worker-network.jl', id_int_file=
    path+'adj_lists/id-int-mapping.tsv', output_file=path+'adj_lists/worker-network-sorted-ints.jl'):
    id_int = dict()
    out = codecs.open(output_file, 'w')
    with codecs.open(id_int_file, 'r', 'utf-8') as f:
        for line in f:
            fields = re.split('\t', line[0:-1])
            if fields[0] in id_int:
                raise Exception
            else:
                id_int[fields[0]] = int(fields[1])
    count = 0
    with codecs.open(adj_file, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            ad_ids = obj.values()[0]
            int_list = [id_int[k] for k in ad_ids]
            int_list.sort()
            obj[obj.keys()[0]] = int_list
            json.dump(obj, out)
            out.write('\n')
            count += 1
            if count % 100000 == 0:
                print 'processed ', str(count), ' jsons'
    out.close()



# UNDER CONSTRUCTION
def build_edge_list(adj_file=path+'adj_lists/name-phone.jl', id_int_file=path+'adj_lists/id-int-mapping.tsv', output_file=None):
    """
    the output file is an edge list without the mtx style header (i.e. the first line is an edge). We use space
    and ints.
    :param adj_file:
    :param id_int_file:
    :param output_file:
    :return:
    """
    # edges = list()
    G = nx.Graph()
    print G.is_directed()
    id_int = dict()
    with codecs.open(id_int_file, 'r', 'utf-8') as f:
        for line in f:
            fields = re.split('\t', line[0:-1])
            if fields[0] in id_int:
                raise Exception
            else:
                id_int[fields[0]] = int(fields[1])
                # G.add_node(int(fields[1]))
    print 'finished adding nodes to graph. Added num nodes...',str(len(G.nodes()))
    num_edges = 0
    count = 0
    with codecs.open(adj_file, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            ad_ids = obj.values()[0]
            if len(ad_ids) <= 1:
                continue
            else:
                k = len(ad_ids)
                num_edges += (k*(k-1)/2)
            for i in range(0, len(ad_ids)-1): # if the list only has one element, won't enter the loop
                for j in range(i+1, len(ad_ids)):
                    G.add_edge(id_int[ad_ids[i]], id_int[ad_ids[j]])
            count += 1
            if count % 100000 == 0:
                print 'processed ', str(count), ' jsons'
    print 'finished adding edges to graph. Added num distinct undirected edges...', str(len(G.edges()))
    print 'num (not necessarily distinct) edges read from file...',str(num_edges)
    print 'Graph info: '
    print nx.info(G)


def distribution_cluster_size(cluster_file=path+'adj_lists/worker-network-sorted-ints.jl',
                              output_file=path+'adj_lists/worker-network-cluster-size-distr.tsv'):
    freq = dict()
    with codecs.open(cluster_file, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            cluster_size = len(obj.values()[0])
            if cluster_size not in freq:
                freq[cluster_size] = 0
            freq[cluster_size] += 1
    out = codecs.open(output_file, 'w')
    for k, v in freq.items():
        out.write(str(k)+'\t'+str(v)+'\n')
    out.close()


def plot_cluster_sizes(input_file=path+'adj_lists/worker-network-cluster-size-distr.tsv'):
    x = list()
    y = list()
    with codecs.open(input_file, 'r', 'utf-8') as f:
        for line in f:
            fields = re.split('\t',line[0:-1])
            # x.append(math.log(int(fields[0])))
            # y.append(math.log(int(fields[1])))
            x.append(int(fields[0]))
            y.append(int(fields[1]))
    plt.loglog(x, y, 'ro')
    plt.show()


# UNDER CONSTRUCTION: not sure if this is mathematically possible with the inputs below.
def worker_network_distribution_cluster_size(
        phone_postid_cluster_file=path+'adj_lists/name-phone-postid-sorted-ints.jl',
        phone_cluster_file=path+'adj_lists/name-phone-sorted-ints.jl',
        postid_cluster_file=path+'adj_lists/name-postid-sorted-ints.jl',
                              output_file=path+'adj_lists/name-phone-postid-cluster-size-distr.tsv'):
    pass

def read_in_pseudo_adj_lists(phone_postid_list=path+'adj_lists/adj_list_phone_postid.txt'):
    """
    Too much memory for local machine. Have implemented this on the server instead.
    :param phone_postid_list:
    :return:
    """
    G = nx.Graph()
    G.add_nodes_from(nodes=range(1,1000))
    # G.add_edge(5,6)
    # G.add_edge(1,2)
    graphs = list(nx.connected_component_subgraphs(G))
    print len(graphs)
    # for g in graphs:
    #     print type(g)


def combine_name_phone_postid_jls(phone=path+'adj_lists/name_phone.jl',postid=path+'adj_lists/name_postid.jl',
                                id_int_file=path+'adj_lists/id-int-mapping.tsv', output_folder=path+'adj_lists/macro-workers/'):
    id_int_dict = dict()
    with codecs.open(id_int_file, 'r') as f:
        for line in f:
            items = re.split('\t', line[0:-1])
            id_int_dict[items[0]] = int(items[1])

    print 'finished reading in id_int_dict file'
    # phone_dict = dict()
    count = 0
    with codecs.open(phone, 'r', 'utf-8') as f:
        for line in f:
            count += 1
            if count % 1000 == 0:
                print 'processed line...', str(count)
            obj = json.loads(line[0:-1])
            name = obj.keys()[0]
            output_file = output_folder + name + '.txt'
            out = codecs.open(output_file, 'w', 'utf-8')
            dic = obj[name]
            for k, v in dic.items():
                if len(v) == 1:
                    out.write(str(id_int_dict[v[0]]))
                    out.write('\n')
                else:
                    v.sort()
                    new_list = [str(id_int_dict[i]) for i in v]
                    out.write(' '.join(new_list))
                    out.write('\n')
            out.close()
    # out = codecs.open(output_file, 'w', 'utf-8')
    print 'finished writing out phone pseudo-adj lists, working on postid...'
    count = 0
    with codecs.open(postid, 'r', 'utf-8') as f:
        for line in f:
            count += 1
            if count % 1000 == 0:
                print 'processed line...', str(count)
            obj = json.loads(line[0:-1])
            name = obj.keys()[0]
            output_file = output_folder+name+'.txt'
            out = codecs.open(output_file, 'a', 'utf-8')
            dic = obj[name]
            for k, v in dic.items():
                if len(v) == 1:
                    out.write(str(id_int_dict[v[0]]))
                    out.write('\n')
                else:
                    v.sort()
                    new_list = [str(id_int_dict[i]) for i in v]
                    out.write(' '.join(new_list))
                    out.write('\n')
            out.close()


def conn_comp_from_macro_worker_file(in_file):
    G = nx.read_adjlist(in_file)
    return sorted(nx.connected_components(G), key = len, reverse=True)

def serialize_connected_components_to_file(graphs, out_prefix):
    for i in range(0, len(graphs)):
        out = codecs.open(out_prefix+str(i+1)+'.txt', 'w', 'utf-8')
        g = list(graphs[i])
        g.sort()
        out.write(' '.join(g)+'\n')
        out.close()

def conn_comp_from_macro_workers(input_folder=path+'adj_lists/macro-workers/',
                    output_folder=path+'adj_lists/connected-component-workers/'):
    files = glob.glob(input_folder+'*.txt')
    for f in files:
        out_prefix = re.split(input_folder+'|txt',f)[1][0:-1]
        serialize_connected_components_to_file(conn_comp_from_macro_worker_file(f),output_folder+out_prefix+'-')


def construct_int_phone_map(id_int_file=path+'adj_lists/id-int-mapping.tsv',
                                data_for_memex=path+'data_for_memex.json',
                            output_file=path+'adj_lists/int-phones.jl'):
    """
    Will not check whether name exists or not. Hence, this is comprehensive, can be used, regardless of whether
    we are using names.
    :param id_int_file:
    :param data_for_memex:
    :param output_file:
    :return:
    """
    id_int_dict = dict()
    with codecs.open(id_int_file, 'r') as f:
        for line in f:
            items = re.split('\t', line[0:-1])
            id_int_dict[items[0]] = int(items[1])

    print 'finished reading in id_int_dict file'
    out = codecs.open(output_file, 'w', 'utf-8')

    with codecs.open(data_for_memex, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])  # exclude newline
            int_id = id_int_dict[obj['_id']]
            if 'phone' not in obj or len(obj['phone']) == 0:
                continue
            else:
                answer = dict()
                answer[int_id] = obj['phone']
                json.dump(answer, out)
                out.write('\n')
    out.close()





def construct_conn_comp_phone_map(int_phone_file=path+'adj_lists/int-phones.jl',
                                  conn_comp_folder=path+'adj_lists/connected-component-workers/',
                                  output_file=path+'adj_lists/connected-component-phone-map.jl'):
    int_phone_dict = dict()
    with codecs.open(int_phone_file, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])  # exclude newline
            int_phone_dict[obj.keys()[0]] = obj[obj.keys()[0]]
    print 'finished reading in int phone dict...'
    files = glob.glob(conn_comp_folder + '*.txt')
    print 'finished reading in file names...'
    out = codecs.open(output_file, 'w', 'utf-8')
    count = 0
    for f in files:
        count += 1
        if count % 10000 == 0:
            print count
        list_of_ints = list()
        with codecs.open(f, 'r', 'utf-8') as m:
            counter = 0
            for line in m:
                list_of_ints = re.split(' ',line[0:-1])
                counter += 1
            if counter != 1:
                print 'problems in file.'+f+'...more than one line...'
                exit()
        phones = set()
        for el in list_of_ints:
            if el not in int_phone_dict:
                continue
            for ph in int_phone_dict[el]:
                phones.add(ph)
        phones = list(phones)
        phones.sort()
        answer = dict()
        answer[re.split(conn_comp_folder+'|txt',f)[1][0:-1]] = phones
        json.dump(answer, out)
        out.write('\n')


    out.close()

def output_guaranteed_singleton_workers(conn_comp_phone_file=path+'adj_lists/connected-component-phone-map.jl',
                      output_file=path+'adj_lists/singleton-workers.txt'):
    out = codecs.open(output_file, 'w', 'utf-8')
    with codecs.open(conn_comp_phone_file, 'r', 'utf-8') as f:
        count = 0
        for line in f:
            obj = json.loads(line[0:-1])
            count += 1
            if count % 50000 == 0:
                print count
            k = obj.keys()[0]
            v = obj[obj.keys()[0]]
            if len(v) == 0:
                out.write(k)
                out.write('\n')


    out.close()


def reverse_phone_map(phone_map_file=path+'adj_lists/connected-component-phone-map.jl',
                      output_file=path+'adj_lists/phone-connected-component-map.jl'):
    """
    Be careful: worker nodes guaranteed to be singletons (those with no phones) will be ignored.
    :param phone_map_file:
    :param output_file:
    :return:
    """
    phone_conn_dict = dict()
    with codecs.open(phone_map_file, 'r', 'utf-8') as f:
        count = 0
        for line in f:
            obj = json.loads(line[0:-1])
            count += 1
            if count % 10000 == 0:
                print count
            k = obj.keys()[0]
            v = obj[obj.keys()[0]]
            for el in v:
                if el not in phone_conn_dict:
                    phone_conn_dict[el] = list()
                phone_conn_dict[el].append(k)

    out = codecs.open(output_file, 'w', 'utf-8')
    for k, v in phone_conn_dict.items():
        answer = dict()
        answer[k] = v
        json.dump(answer, out)
        out.write('\n')
    out.close()

def get_size_statistics(pcc_file=path+'adj_lists/phone-connected-component-map.jl'):
    """

    :param pcc_file:
    :return:
    """
    size_dict = dict()
    with codecs.open(pcc_file, 'r', 'utf-8') as f:
        # count = 0
        for line in f:
            obj = json.loads(line[0:-1])
            # count += 1
            # k = obj.keys()[0]
            v = obj[obj.keys()[0]]
            if len(v) not in size_dict:
                size_dict[len(v)] = 0
            size_dict[len(v)] += 1
    if 1 not in size_dict:
        size_dict[1] = 153339
    else:
        size_dict[1] += 153339
    sizes = size_dict.keys()
    sizes.sort(reverse=True)
    print 'largest size found...',str(sizes[0])
    x = list()
    y = list()
    for k, v in size_dict.items():
        x.append(k)
        y.append(v)
    plt.loglog(x, y, 'ro')
    plt.show()



def write_edge_list(pcc_file=path+'adj_lists/phone-connected-component-map.jl',
                    edge_list=path+'adj_lists/edge-list-names'):
    """
    File will not be deduplicated.
    :param pcc_file:
    :param edge_list:
    :return:
    """
    out = codecs.open(edge_list, 'w', 'utf-8')
    with codecs.open(pcc_file, 'r', 'utf-8') as f:
        count = 0
        for line in f:
            obj = json.loads(line[0:-1])
            count += 1
            if count % 10000 == 0:
                print count
            # k = obj.keys()[0]
            v = obj[obj.keys()[0]]
            v.sort()
            if len(v) == 0 or len(v) == 1:
                continue
            for i in range(0, len(v)-1):
                for j in range(i+1, len(v)):
                    out.write(v[i]+'\t'+v[j]+'\n')

    out.close()


def analyze_edge_list(edge_list=path+'adj_lists/edge-list-names',ccp_file=path+'adj_lists/connected-component-phone-map.jl'):
    node_list = list()
    G = nx.read_edgelist(edge_list, delimiter='\t')
    print 'is the graph directed? ',
    print G.is_directed()
    with codecs.open(ccp_file, 'r', 'utf-8') as f:
        for line in f:
            node_list.append(json.loads(line[0:-1]).keys()[0])
    G.add_nodes_from(node_list)
    print 'num nodes...',str(len(G.nodes()))
    print 'num edges...', str(len(G.edges()))
    print 'num connected components...',str(nx.number_connected_components(G))
    return G


def print_largest_phone_hypergraphs(pcc_file=path+'adj_lists/phone-connected-component-map.jl',
            cc_folder=path+'adj_lists/connected-component-workers/', int_id_file=path+'adj_lists/int-id-mapping.tsv',
                              output_file=path+'adj_lists/largest_hypergraphs-100.jl', larger_than=100):
    int_id_dict = dict()
    with codecs.open(int_id_file, 'r') as f:
        for line in f:
            items = re.split('\t', line[0:-1])
            int_id_dict[int(items[0])] = items[1]
    print 'finished reading in int id file...'
    out = codecs.open(output_file, 'w', 'utf-8')
    cluster_count = 0
    with codecs.open(pcc_file, 'r', 'utf-8') as f:

        count = 0
        for line in f:
            obj = json.loads(line[0:-1])
            count += 1
            if count % 10000 == 0:
                print count
            k = obj.keys()[0]
            v = obj[obj.keys()[0]]
            if len(v) < larger_than:
                continue
            else:
                list_of_ids = _get_list_of_ids(cc_folder, v, int_id_dict) #guaranteed to be a sorted list of unique ids.
                answer = dict()
                answer[k] = list_of_ids
                json.dump(answer, out)
                out.write('\n')
                cluster_count += 1
    out.close()
    print 'number of hypergraphs larger than ',str(larger_than),' is ',str(cluster_count)




def _get_list_of_ids(cc_folder, list_of_workers, int_id_dict):
    answer = list()
    for item in list_of_workers:
        with codecs.open(cc_folder+item+'.txt', 'r', 'utf-8') as f:
            g = list()
            for line in f:
                g = [int(i) for i in re.split(' ',line[0:-1])]
            answer = answer+g
    answer = list(set(answer))
    ans = [int_id_dict[i] for i in answer]
    ans.sort()
    return ans

def degree_distribution_plot_worker_network(edge_list=path+'adj_lists/edge-list-names',
                                    ccp_file=path+'adj_lists/connected-component-phone-map.jl'):
        """

        :param phone_network_file:
        :param phone_mapping_file:
        :param output_graph:
        :return:
        """
        G = analyze_edge_list(edge_list, ccp_file)
        # degree_sequence = sorted(nx.degree(G).values(), reverse=True)  # degree sequence
        # print "Degree sequence", degree_sequence
        # dmax = max(degree_sequence)
        degree_hist = nx.classes.function.degree_histogram(G)
        log_k = list()
        log_freq = list()
        total_sum = np.sum(degree_hist)
        k_vec = list()
        freq_vec = list()
        print 'total sum of degree hist. list is...', str(total_sum)
        # degree_dict = dict()
        if degree_hist[0] > 0:
            log_k.append(0)
            k_vec.append(0)
            log_freq.append(math.log(degree_hist[0] * 1.0 / total_sum))
            freq_vec.append(degree_hist[0] * 1.0 / total_sum)
        for i in range(1, len(degree_hist)):
            if degree_hist[i] == 0:
                continue
            else:
                k_vec.append(i)
                freq_vec.append(degree_hist[i] * 1.0 / total_sum)
                log_k.append(math.log(i))
                log_freq.append(math.log(degree_hist[i] * 1.0 / total_sum))
        # plt.loglog(degree_sequence, 'b-', marker='o')
        regr = linear_model.LinearRegression()
        print len(log_k)
        print len(log_freq)
        # Train the model using the training sets
        regr.fit(np.array(log_k).reshape(-1, 1), np.array(log_freq).reshape(-1, 1))

        # The coefficients
        print('Coefficients: \n', regr.coef_)
        plt.loglog(k_vec, freq_vec, 'ro')

        plt.title("Degree distribution plot")
        plt.ylabel("Prob(k)")
        plt.xlabel("k")
        plt.show()

def print_ids_in_hypergraph_containing_phone(data_for_memex=path+'data_for_memex.json',
                        hypergraph_orig=path+'adj_lists/largest_hypergraphs-100.jl',
                                hypergraph_pruned=path+'adj_lists/largest_hypergraphs-100-phone-subset.jl',
                            hypergraph_pruned_urls=path + 'adj_lists/largest_hypergraphs-100-phone-subset-URLs.jl'):
    """
    Primarily designed for data cleaning/verification. Take a hypergraph jl file and print out a new file
    where the phone references IDs that contain that phone. This is not true for the ids in the hypergraph
    file in general, obviously.
    :return:
    """
    id_phone = dict()
    id_url = dict()
    with codecs.open(data_for_memex, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            if len(obj['phone']) > 0:
                id_phone[obj['_id']] = obj['phone']
                id_url[obj['_id']] = obj['url']
    print 'finished reading data file...'
    out = codecs.open(hypergraph_pruned, 'w', 'utf-8')
    out1 = codecs.open(hypergraph_pruned_urls, 'w', 'utf-8')
    with codecs.open(hypergraph_orig, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            k = obj.keys()[0]
            new_dict = dict()
            new_dict[k] = list()
            new_dict1 = dict()
            new_dict1[k] = list()
            v = obj[k]
            for i in v:
                if i not in id_phone:
                    continue
                if k in id_phone[i]:
                    new_dict[k].append(i)
                    new_dict1[k].append(id_url[i])
            json.dump(new_dict, out)
            out.write('\n')
            json.dump(new_dict1, out1)
            out1.write('\n')
    out.close()
    out1.close()


def serialize_edge_list_to_graphviz_dot(edge_list=path+'adj_lists/edge-list-names',
                                output_file=path+'adj_lists/visualizations/edge-list-names.dot'):
    G = nx.read_edgelist(edge_list, delimiter='\t')
    nx_agraph.write_dot(G, output_file)

def layout_graph(dot_file=path+'adj_lists/visualizations/edge-list-names.dot',
                 image_file=path+'adj_lists/visualizations/edge-list-names.png'):
    G = pgv.AGraph(dot_file)
    G.layout(prog='dot')
    G.draw(image_file)


# serialize_edge_list_to_graphviz_dot()
layout_graph()
# print_ids_in_hypergraph_containing_phone()
# st = 'cara michelle-1'
# st1 = st.replace(' ','_')
# print st1
### phase 3: plot
# construct_conn_comp_phone_map()
# output_guaranteed_singleton_workers()
# print_largest_phone_hypergraphs()
# degree_distribution_plot_worker_network()
# reverse_phone_map()
# write_edge_list()
# plot_cluster_sizes()
# analyze_edge_list()
# get_size_statistics()
### phase 2: serialize each adjacency list, compute cluster size distr. Make sure to change params.
# serialize_connected_components_to_file(conn_comp_from_macro_worker_file(in_file=path+'adj_lists/macro-workers/mileah.txt'),
#                          path + 'adj_lists/connected-component-workers/mileah-')
# print len(glob.glob(path+'adj_lists/connected-component-workers/*.txt'))
# conn_comp_from_macro_workers()
# serialize_as_list_of_sorted_ints()
# print re.split(path+'adj_lists/connected-component-workers/'+'|txt',glob.glob(path+'adj_lists/connected-component-workers/*.txt')[0])
# distribution_cluster_size()
# construct_adj_list_phone_postid()
# read_in_pseudo_adj_lists()
### set up phase (1)
# generate_worker_ids()
# combine_name_phone_postid_jls()
# generate_name_phone_file()
# build_edge_list() # under construction, will likely have to move to server/mapreduce
# build_name_and_postid_or_phone_adj_list()
# build_id_integer_mappings() # one off, leave alone once constructed


