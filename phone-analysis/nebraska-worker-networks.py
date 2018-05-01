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


def construct_int_day_map(id_int_file=path+'adj_lists/id-int-mapping.tsv',
                                data_for_memex=path+'data_for_memex.json',
                            output_file=path+'adj_lists/int-day.jl'):
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
            if 'day' not in obj:
                continue
            else:
                answer = dict()
                answer[int_id] = obj['day']
                json.dump(answer, out)
                out.write('\n')
    out.close()


def construct_int_postid_map(id_int_file=path+'adj_lists/id-int-mapping.tsv',
                                data_for_memex=path+'data_for_memex.json',
                            output_file=path+'adj_lists/int-postid.jl'):
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
            if 'post_id' not in obj:
                continue
            else:
                answer = dict()
                answer[int_id] = obj['post_id']
                json.dump(answer, out)
                out.write('\n')
    out.close()


def construct_conn_comp_day_map(int_day_file=path+'adj_lists/int-day.jl',
                          conn_comp_folder=path + 'adj_lists/connected-component-workers/',
                          output_file=path + 'adj_lists/connected-component-day-map.jl'
                          ):
    int_day_dict = dict()
    with codecs.open(int_day_file, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])  # exclude newline
            int_day_dict[obj.keys()[0]] = obj[obj.keys()[0]]
    print 'finished reading in int day dict...'
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
                list_of_ints = re.split(' ', line[0:-1])
                counter += 1
            if counter != 1:
                print 'problems in file.' + f + '...more than one line...'
                exit()
        days = set()
        for el in list_of_ints:
            if el not in int_day_dict:
                continue
            days.add(int_day_dict[el])
        days = list(days)
        days.sort()
        answer = dict()
        answer[re.split(conn_comp_folder + '|txt', f)[1][0:-1]] = days
        json.dump(answer, out)
        out.write('\n')

    out.close()

def construct_conn_comp_postid_map(int_postid_file=path+'adj_lists/int-postid.jl',
                                  conn_comp_folder=path+'adj_lists/connected-component-workers/',
                                  output_file=path+'adj_lists/connected-component-postid-map.jl'):
    int_postid_dict = dict()
    with codecs.open(int_postid_file, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])  # exclude newline
            int_postid_dict[obj.keys()[0]] = obj[obj.keys()[0]]
    print 'finished reading in int postid dict...'
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
        postids = set()
        for el in list_of_ints:
            if el not in int_postid_dict:
                continue
            postids.add(int_postid_dict[el])
        postids = list(postids)
        postids.sort()
        answer = dict()
        answer[re.split(conn_comp_folder+'|txt',f)[1][0:-1]] = postids
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

def output_guaranteed_singleton_workers(conn_comp_phone_file=path+'adj_lists/connected-component-postid-map.jl',
                      output_file=path+'adj_lists/postid-guaranteed-singleton-workers.txt'):
    """
    Only a subset of the true singleton workers i.e. those workers who have no corr. attribute values (e.g. no phone)
    and hence cannot form an edge in the higher-order worker network under any circumstances
    :param conn_comp_phone_file:
    :param output_file:
    :return:
    """
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


def output_all_singleton_workers(edge_list_file=path+'adj_lists/postid-edge-list-names',
    conn_comp_file=path+'adj_lists/connected-component-postid-map.jl',
                      output_file=path+'adj_lists/postid-all-singleton-workers.txt'):
    # out = codecs.open(output_file, 'w', 'utf-8')
    conn_comp_ids = set()
    with codecs.open(conn_comp_file, 'r', 'utf-8') as f:
        count = 0
        for line in f:
            obj = json.loads(line[0:-1])
            count += 1
            if count % 50000 == 0:
                print count
            conn_comp_ids.add(obj.keys()[0])
    print 'finished reading in conn. comp. ids...',str(len(conn_comp_ids))
    with codecs.open(edge_list_file, 'r', 'utf-8') as f:
        count = 0
        for line in f:
            fields = re.split('\t',line[0:-1])
            count += 1
            if count % 50000 == 0:
                print count
            conn_comp_ids.discard(fields[0])
            conn_comp_ids.discard(fields[1])
    out = codecs.open(output_file, 'w', 'utf-8')
    for item in conn_comp_ids:
        out.write(item+'\n')

    out.close()


def reverse_connected_components_map(map_file=path+'adj_lists/connected-component-postid-map.jl',
                      output_file=path+'adj_lists/postid-connected-component-map.jl'):
    """
    Be careful: worker nodes guaranteed to be singletons (those with no phones) will be ignored.
    :param phone_map_file:
    :param output_file:
    :return:
    """
    phone_conn_dict = dict() # should not have 'phone': artefact of first impl.
    with codecs.open(map_file, 'r', 'utf-8') as f:
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



def write_edge_list(pcc_file=path+'adj_lists/postid-connected-component-map.jl',
                    edge_list=path+'adj_lists/postid-edge-list-names'):
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


def analyze_edge_list(edge_list=path+'adj_lists/postid-edge-list-names',ccp_file=path+'adj_lists/connected-component-postid-map.jl'):
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
    conn_comps=sorted(nx.connected_components(G), key=len)
    # print type(conn_comps)
    singleton_conn_comp = 0
    for c in conn_comps:
        if len(c) == 1:
            singleton_conn_comp += 1
    print 'num singleton connected components...',str(singleton_conn_comp)
    return G


def analyze_phone_postid_edge_lists(edge_list_postid=path+'adj_lists/postid-edge-list-names',ccp_file_postid=path+'adj_lists/connected-component-postid-map.jl',
                                    edge_list_phone=path + 'adj_lists/phone-edge-list-names',
                                    ccp_file_phone=path + 'adj_lists/connected-component-phone-map.jl'):
    """
    We need the ccp files to ensure we're reading in the singletons as well.
    :param edge_list_postid:
    :param ccp_file_postid:
    :param edge_list_phone:
    :param ccp_file_phone:
    :return:
    """
    node_list = list()
    G_phone = nx.read_edgelist(edge_list_phone, delimiter='\t')
    G_postid = nx.read_edgelist(edge_list_postid, delimiter='\t')
    G = nx.compose(G_phone, G_postid)
    print 'is the graph directed? ',
    print G.is_directed()
    with codecs.open(ccp_file_phone, 'r', 'utf-8') as f:
        for line in f:
            node_list.append(json.loads(line[0:-1]).keys()[0])
    G.add_nodes_from(node_list)

    node_list = list()
    with codecs.open(ccp_file_postid, 'r', 'utf-8') as f:
        for line in f:
            node_list.append(json.loads(line[0:-1]).keys()[0])
    G.add_nodes_from(node_list)

    print 'num nodes...',str(len(G.nodes()))
    print 'num edges...', str(len(G.edges()))
    print 'num connected components...',str(nx.number_connected_components(G))
    conn_comps = sorted(nx.connected_components(G), key=len)
    # print type(conn_comps)
    singleton_conn_comp = 0
    for c in conn_comps:
        if len(c) == 1:
            singleton_conn_comp += 1
    print 'num singleton connected components...', str(singleton_conn_comp)
    return G


def analyze_phone_postid_conn_components(edge_list_postid=path+'adj_lists/postid-edge-list-names',ccp_file_postid=path+'adj_lists/connected-component-postid-map.jl',
                                    edge_list_phone=path + 'adj_lists/phone-edge-list-names',
                                    ccp_file_phone=path + 'adj_lists/connected-component-phone-map.jl',
                                         out_file_cid_conn_comp = path+'adj_lists/connected-component-analyses/cid-conn-comps.jl',
                                         out_file_all= path+'adj_lists/connected-component-analyses/worker-connected-component-analysis-all.tsv',
                                         out_file_non_singletons=path + 'adj_lists/connected-component-analyses/worker-connected-component-analysis-non-singletons.tsv'):
    node_list = list()
    G_phone = nx.read_edgelist(edge_list_phone, delimiter='\t')
    print 'finished reading in phone edge list...'
    G_postid = nx.read_edgelist(edge_list_postid, delimiter='\t')
    print 'finished reading in postid edge list...'
    G = nx.compose(G_phone, G_postid)
    print 'is the graph directed? ',
    print G.is_directed()
    with codecs.open(ccp_file_phone, 'r', 'utf-8') as f:
        for line in f:
            node_list.append(json.loads(line[0:-1]).keys()[0])
    G.add_nodes_from(node_list)

    node_list = list()
    with codecs.open(ccp_file_postid, 'r', 'utf-8') as f:
        for line in f:
            node_list.append(json.loads(line[0:-1]).keys()[0])
    G.add_nodes_from(node_list)
    print 'finished adding singletons'

    print 'num nodes...', str(len(G.nodes()))
    print 'num edges...', str(len(G.edges()))
    print 'num connected components...', str(nx.number_connected_components(G))
    conn_comps = sorted(nx.connected_components(G), key=len) # construct connected components. Now the analysis begins
    print 'iterating through connected components...'

    out1 = codecs.open(out_file_all, 'w', 'utf-8')
    header_string = 'row_id\tnodes\tedges\tdensity\tavg. clustering coefficient\ttransitivity\tavg. shortest path length\tdiameter\tdeg. assort. coefficient\n'
    out1.write(header_string)
    out2 = codecs.open(out_file_non_singletons, 'w', 'utf-8')
    out2.write(header_string)
    out3 = codecs.open(out_file_cid_conn_comp, 'w', 'utf-8')

    count = 0
    for c in conn_comps:
        count += 1
        if count % 10000 == 0:
            print count
        # print c
        # print len(c)
        Gsub = G.subgraph(c)

        num_nodes = str(len(Gsub.nodes()))
        num_edges = str(len(Gsub.edges()))
        density = str(nx.density(Gsub))
        clust = str(nx.average_clustering(Gsub))
        transit = str(nx.transitivity(Gsub))
        # avg_path = str(nx.average_shortest_path_length(Gsub))
        avg_path = 'NULL'
        # diameter = str(nx.diameter(Gsub))
        diameter = 'NULL'
        if len(c) == 1:
            deg_assort = 'NULL'
        else:
            deg_assort = str(nx.degree_assortativity_coefficient(Gsub))

        write_list = [str(count),num_nodes,num_edges,density,clust,transit,avg_path,diameter,deg_assort]
        out1.write('\t'.join(write_list))
        out1.write('\n')
        if len(c) > 1:
            out2.write('\t'.join(write_list))
            out2.write('\n')
        answer = dict()
        answer[count] = list(c)
        json.dump(answer, out3)
        out3.write('\n')

        # break
    out1.close()
    out2.close()
    out3.close()



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

def degree_distribution_plot_worker_network(edge_list=path+'adj_lists/phone-edge-list-names',
                                    ccp_file=path+'adj_lists/connected-component-phone-map.jl', G=None):
        """

        :param phone_network_file:
        :param phone_mapping_file:
        :param output_graph:
        :return:
        """
        if G is None:
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


def serialize_edge_list_to_graphviz_dot(edge_list=path+'adj_lists/postid-edge-list-names',
                                output_file=path+'adj_lists/postid-worker-visualizations/postid-edge-list-names.dot'):
    G = nx.read_edgelist(edge_list, delimiter='\t')
    nx_agraph.write_dot(G, output_file)

def serialize_multi_edge_list_to_graphviz_dot(edge_list1=path+'adj_lists/postid-edge-list-names',edge_list2=path+'adj_lists/phone-edge-list-names',
                                output_file=path+'adj_lists/phone-postid-worker-visualizations/phone-postid-edge-list-names.dot'):
    G1 = nx.read_edgelist(edge_list1, delimiter='\t')
    G2 = nx.read_edgelist(edge_list2, delimiter='\t')
    G = nx.compose(G1, G2)
    nx_agraph.write_dot(G, output_file)

def layout_graph(dot_file=path+'adj_lists/visualizations/postid-edge-list-names.dot',
                 image_file=path+'adj_lists/postid-worker-visualizations/postid-edge-list-names.png'):
    # not sure this is working, I think we had to use sfdp instead
    G = pgv.AGraph(dot_file)
    G.layout(prog='dot')
    G.draw(image_file)

def conn_comp_date_range(input_file=path + 'adj_lists/connected-component-day-map.jl', output_file=path + 'adj_lists/connected-component-day-range.tsv'):
    out = codecs.open(output_file, 'w', 'utf-8')
    with codecs.open(input_file, 'r', 'utf-8') as f:
        count = 0
        for line in f:
            obj = json.loads(line[0:-1])
            count += 1
            if count % 10000 == 0:
                print count
            k = obj.keys()[0]
            v = obj[obj.keys()[0]]
            if len(v) <= 1:
                v = '0 days'
            else:
                dt = parse(str(v[-1])) - parse(str(v[0]))
                v = str(dt)
            out.write(obj.keys()[0]+'\t'+v+'\n')

    out.close()


def compute_phone_postid_singletons(postid_singletons=path+'adj_lists/postid-all-singleton-workers.txt', phone_singletons=
    path+'adj_lists/phone-all-singleton-workers.txt', output_file = path+'adj_lists/phone-postid-all-singleton-workers.txt'):
    """
    Careful, we're dealing with an intersection not union, since a node may not be a singleton anymore once
    we account for both types of edges
    :param postid_singletons:
    :param phone_singletons:
    :param output_file:
    :return:
    """
    phone_singleton_set = set()
    postid_singleton_set = set()
    with codecs.open(phone_singletons, 'r', 'utf-8') as f:

        for line in f:
            phone_singleton_set.add(line[0:-1])
    print 'finished reading in phone singletons'
    with codecs.open(postid_singletons, 'r', 'utf-8') as f:

        for line in f:
            postid_singleton_set.add(line[0:-1])
    print 'finished reading in postid singletons'
    out = codecs.open(output_file, 'w', 'utf-8')
    for item in phone_singleton_set.intersection(postid_singleton_set):
        out.write(item+'\n')
    out.close()


def connected_component_size_stratified_sampling(min_size=2, max_size=10, sample_size=10,
                                 cid_file=path+'adj_lists/connected-component-analyses/cid-conn-comps.jl',
                                                 output_folder=path+'adj_lists/connected-component-samples/2_10_10/'):
    """
    we sample at least one item from each bucket within min_size and max_size. for this reason, total may be more than
    10.
    :param min_size:
    :param max_size:
    :param sample_size:
    :param cid_file:
    :param output_folder:
    :return:
    """
    sampling_dict = dict()
    total = 0
    cids = dict()
    with codecs.open(cid_file, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            k = obj.keys()[0]
            if len(obj[k]) >= min_size and len(obj[k]) <= max_size:
                size = len(obj[k])
                if size not in sampling_dict:
                    sampling_dict[size] = list()
                sampling_dict[size].append(k)
                total += 1
                cids[k] = obj[k]
    print 'printing populations sizes, and outputting samples'

    for k, v in sampling_dict.items():
        print 'size: ',str(k),' number of items: ',str(len(v))
        local_size = int(sample_size*1.0*len(v)/total)
        if local_size < 1:
            local_size = 1
        from numpy.random import choice
        samples = choice(v, local_size, False)
        for s in samples:
            out = codecs.open(output_folder+s+'.txt', 'w')
            out.write('\t'.join(cids[s])+'\n')
            out.close()


def connected_component_ad_ints(input_folder=path+'adj_lists/connected-component-samples/2_10_10/',
                                   output_folder=path+'adj_lists/connected-component-samples/2_10_10_ints/',
                                   ccw_folder=path + 'adj_lists/connected-component-workers/'):
    input_files = glob.glob(input_folder + '*.txt')
    output_files = [i.replace(input_folder, output_folder) for i in input_files]
    for infi in range(0, len(input_files)):
        ints = list()
        with codecs.open(input_files[infi], 'r') as inf:
            fields = None
            for line in inf:
                fields = re.split('\t',line[0:-1])

            for field in fields:
                with codecs.open(ccw_folder+field+'.txt') as field_f:
                    for line in field_f:
                        ints += re.split(' ',line[0:-1])
            ints = list(set(ints))
            ints.sort()
        out = codecs.open(output_files[infi], 'w', 'utf-8')
        out.write(' '.join(ints)+'\n')
        out.close()


    # for fi in files:
    #     with codecs.open(fi, 'r', 'utf-8') as f:
    #         pass


def connected_component_ad_samples(input_folder=path+'adj_lists/connected-component-samples/2_10_10_ints/',
                                   output_folder=path+'adj_lists/connected-component-samples/2_10_10_ads/',
                                    data_for_memex=path + 'data_for_memex.json',
                                   int_id_file=path + 'adj_lists/int-id-mapping.tsv'):
    input_files = glob.glob(input_folder + '*.txt')
    output_files = [i.replace(input_folder, output_folder) for i in input_files]
    int_id_dict = dict()
    with codecs.open(int_id_file, 'r') as f:
        for line in f:
            items = re.split('\t', line[0:-1])
            int_id_dict[int(items[0])] = items[1]
    print 'finished reading in int id file...'
    output_dict = dict()
    big_fields = list()
    for infi in range(0, len(input_files)):
        fields = list()

        with codecs.open(input_files[infi], 'r') as inf:

            for line in inf:
                fields = re.split(' ',line[0:-1])

            for i in range(0, len(fields)):
                fields[i] = int_id_dict[int(fields[i])]


        output_dict[output_files[infi]] = fields
        big_fields += fields

    big_fields = list(set(big_fields))

    #enable when you have more time. For now just print out IDs.
    # id_data = dict()
    # count = 0
    # with codecs.open(data_for_memex, 'r', 'utf-8') as f:
    #     for line in f:
    #
    #         if count % 100000 == 0:
    #             print count
    #             # break
    #         obj = json.loads(line[0:-1])
    #         count += 1
    #         if len(obj['name'])==0 or obj['_id'] not in big_fields:
    #             continue
    #         else:
    #             id_data[obj['_id']] = obj
    # print 'finished reading data memex'

    for o, v in output_dict.items():
        out = codecs.open(o, 'w')
        json.dump(v, out)
        out.write('\n')
        out.close()



def worker_attribute_analysis(input_folder = path+'adj_lists/connected-component-workers/',
                              data_for_memex=path + 'data_for_memex.json',
                              id_int_file=path + 'adj_lists/id-int-mapping.tsv',
                              output = path+'adj_lists/worker-attribute-analyses/worker-attribute-distribution.jl' ):
    """
    Currently only computing for ethnicity. Blank values are also counted as 'NULL'. Also, we're using the fact
    that we don't need to load in data if it doesn't have a name
    :param input_folder:
    :param data_for_memex:
    :param id_int_file:
    :return:
    """
    files = glob.glob(input_folder + '*.txt')
    print 'finished reading in file names in connected component workers...'
    id_int = dict()
    int_data = dict()

    with codecs.open(id_int_file, 'r', 'utf-8') as f:
        for line in f:
            fields = re.split('\t',line[0:-1])
            id_int[fields[0]] = fields[1]
    print 'finished processing id int file...'
    count = 1
    with codecs.open(data_for_memex, 'r', 'utf-8') as f:
        for line in f:

            if count % 100000 == 0:
                print count
            obj = json.loads(line[0:-1])
            if len(obj['name']) == 0:
                count += 1
                continue
            int_data[id_int[obj['_id']]] = dict()
            int_data[id_int[obj['_id']]]['ethnicity'] = obj['ethnicity']
            int_data[id_int[obj['_id']]]['price'] = obj['price']
            int_data[id_int[obj['_id']]]['age'] = obj['age']
            int_data[id_int[obj['_id']]]['indicators'] = obj['indicators']
            int_data[id_int[obj['_id']]]['city'] = obj['city']
            count += 1
            del id_int[obj['_id']]

    print 'finished reading in data...'
    # id_int = None
    out = codecs.open(output, 'w', 'utf-8')
    for fi in files:
        # print f
        fields = list()
        count = 0
        with codecs.open(fi, 'r', 'utf-8') as f:
            for line in f:
                # print line
                fields = re.split(' ',line[0:-1])
                # print fields
                # for i in range(0, len(fields)):
                #     fields[i] = int(fields[i])
                count += 1
                if count != 1:
                    print f
                    raise Exception
            ethnicities = dict()
            ages = dict()
            prices = dict()
            cities = dict()
            indicators = dict()
            answer = dict()
            for i in fields:
                #ethnicities
                eth = int_data[i]['ethnicity']
                if len(eth) == 0:
                    if 'NULL' not in ethnicities:
                        ethnicities['NULL'] = 0
                    ethnicities['NULL'] += 1
                else:
                    for e in eth:
                        if e not in ethnicities:
                            ethnicities[e] = 0
                        ethnicities[e] += 1
                #ages
                a = int_data[i]['age']
                if a == "":
                    if 'NULL' not in ages:
                        ages['NULL'] = 0
                    ages['NULL'] += 1
                else:

                        if a not in ages:
                            ages[a] = 0
                        ages[a] += 1
                #prices
                a = int_data[i]['price']
                if a == "":
                    if 'NULL' not in prices:
                        prices['NULL'] = 0
                    prices['NULL'] += 1
                else:

                    if a not in prices:
                        prices[a] = 0
                    prices[a] += 1
                # cities
                a = int_data[i]['city']
                if a == "":
                    if 'NULL' not in cities:
                        cities['NULL'] = 0
                    cities['NULL'] += 1
                else:

                    if a not in cities:
                        cities[a] = 0
                    cities[a] += 1
                # indicators
                ind = int_data[i]['indicators']
                if len(ind) == 0:
                    if 'NULL' not in indicators:
                        indicators['NULL'] = 0
                    indicators['NULL'] += 1
                else:
                    for e in ind:
                        if e not in indicators:
                            indicators[e] = 0
                        indicators[e] += 1
            answer[fi[len(input_folder):]] = dict()
            answer[fi[len(input_folder):]]['ethnicity'] = ethnicities
            answer[fi[len(input_folder):]]['ages'] = ages
            answer[fi[len(input_folder):]]['prices'] = prices
            answer[fi[len(input_folder):]]['cities'] = cities
            answer[fi[len(input_folder):]]['indicators'] = indicators
            json.dump(answer, out)
            out.write('\n')

    out.close()


def worker_attribute_entropy_profile(attribute_distr = path+'adj_lists/worker-attribute-analyses/worker-attribute-distribution.jl',
                                     entropy_profile_output = path+'adj_lists/worker-attribute-analyses/worker-attribute-distribution-entropy.csv'):
    attribute_dict = dict()
    count = 0
    with codecs.open(attribute_distr, 'r', 'utf-8') as f:
        for line in f:
            if count % 10000 == 0:
                print count
            obj = json.loads(line[0:-1])
            obj = obj[obj.keys()[0]]
            attributes = obj.keys() # these must be uniform! i.e. the jsons have the exact same inner schema, even when values are missing
            for a in attributes:
                ent= _compute_entropy(obj[a])
                if a not in attribute_dict:
                    attribute_dict[a] = list()
                attribute_dict[a].append(ent)
            count += 1
    print 'finished populating attribute dict...'
    out = codecs.open(entropy_profile_output, 'w')
    out.write('Attribute,Avg. Entropy across Workers,Std. Dev.\n')
    for k,v in attribute_dict.items():
        avg = np.mean(v)
        std = np.std(v)
        out.write(k+','+str(avg)+','+str(std)+'\n')
    out.close()

def _compute_entropy(value_dictionary):
    # value dictionary has 'names' for keys and values (i.e. ints) for values. We assume discreteness
    v = list(value_dictionary.values())
    if len(v) <= 1:
        return 0.0
    c = np.sum(v)
    entropy = 0.0
    for i in v:
        if i == 0:
            continue
        else:
            P = 1.0*i/c
            entropy += (math.log(P)*P*-1.0)
    return entropy


def cid_homophily_analysis(cid_file=path+'adj_lists/connected-component-analyses/cid-conn-comps.jl',
                    worker_attribute_file=path+'adj_lists/worker-attribute-analyses/worker-attribute-distribution.jl',
                    edge_list_postid=path+'adj_lists/postid-edge-list-names',
                    ethnicity_dict_file=path+'adj_lists/Ethnicity-Tiers.json',
                    cc_day_map=path+'adj_lists/connected-component-day-map.jl',
                    edge_list_phone=path + 'adj_lists/phone-edge-list-names',
                    out_file=path+'adj_lists/connected-component-analyses/worker-cc-homophily-analysis-non-singletons.tsv'):

    ethnicity_resolver = read_ethnicity_dict(ethnicity_dict_file)
    G = nx.compose(nx.read_edgelist(edge_list_phone, delimiter='\t'), nx.read_edgelist(edge_list_postid, delimiter='\t'))
    print 'finished reading in graph'

    edge_dict = dict()
    for e in G.edges():
        if e[0] not in edge_dict:
            edge_dict[e[0]] = set()
        edge_dict[e[0]].add(e[1])
    G = None



    worker_attribute_dict = dict()
    with codecs.open(worker_attribute_file, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            worker_attribute_dict[obj.keys()[0]] = obj[obj.keys()[0]]

    with codecs.open(cc_day_map, 'r', 'utf-8') as f:
        # count = 0
        for line in f:
            obj = json.loads(line[0:-1])
            # count += 1
            # if count % 10000 == 0:
            #     print count
            k = obj.keys()[0]
            if k+'.txt' not in worker_attribute_dict:
                raise Exception
            v = obj[obj.keys()[0]]
            if len(v) <= 1:
                worker_attribute_dict[k+'.txt']['avg_days_per_week'] = 0
            else:
                dt = parse(str(v[-1])) - parse(str(v[0]))
                if dt.days+1 < 30:
                    worker_attribute_dict[k + '.txt']['avg_days_per_week'] = 0
                else:
                    worker_attribute_dict[k + '.txt']['avg_days_per_week'] = (len(set(v))*7)/(dt.days+1)
    print 'finished reading worker attribute dict'
    out = codecs.open(out_file, 'w', 'utf-8')
    out.write('row_id\tage_assortativity_coefficient\tprice_assortativity_coefficient\t'
              'in/outcall_assortativity_coefficient\tethnicity_assortativity_coefficient\tavg_days_assortativity_coefficient,'
              '\tdegree_assortativity\n')


    with codecs.open(cid_file, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            if len(obj[obj.keys()[0]])<=1:
                continue
            mixing = compute_assortative_mixing(obj[obj.keys()[0]], worker_attribute_dict,edge_dict,ethnicity_resolver)
            out.write(obj.keys()[0]+'\t'+str(mixing['age'])+'\t'+str(mixing['price'])
                      +'\t'+str(mixing['in/outcall'])+'\t'+str(mixing['ethnicity'])+'\t'+str(mixing['avgDays'])+
                      str(mixing['degree'])+'\n')
            # print obj.keys()[0]

            # break

    out.close()

def compute_assortative_mixing(worker_list, worker_attribute_dict,G_edge_dict,ethnicity_resolver):
    G = nx.Graph()
    for worker in worker_list:
        inout = "0"
        if 'indicators' in worker_attribute_dict[worker+'.txt'] and "29" in worker_attribute_dict[worker+'.txt']['indicators']\
                and worker_attribute_dict[worker+'.txt']['indicators']["29"] >= 1:
            inout = "1"
        ethnicity_set = set()
        for eth in worker_attribute_dict[worker+'.txt']['ethnicity'].keys():
            if eth == 'NULL' or eth not in ethnicity_resolver:
                ethnicity_set.add(eth)
            else:
                ethnicity_set.add(ethnicity_resolver[eth])
        worker_ethnicity = "mixed"
        if len(ethnicity_set) == 1:
            worker_ethnicity = list(ethnicity_set)[0]
        G.add_node(worker, price=_get_max_element(worker_attribute_dict[worker+'.txt']['prices']),
                   age=_get_max_element(worker_attribute_dict[worker+'.txt']['ages']),
                   inOutcall=inout,
                   ethnicity=worker_ethnicity,
                   avgDays=worker_attribute_dict[worker+'.txt']['avg_days_per_week'])
        if worker in G_edge_dict:
            for element in G_edge_dict[worker]:
                if element not in worker_list:
                    raise Exception
                G.add_edge(worker, element)

    answer = dict()

    from networkx.algorithms.assortativity.mixing import attribute_mixing_matrix, numeric_mixing_matrix
    if attribute_mixing_matrix(G, 'price').trace() == len(attribute_mixing_matrix(G, 'price')):
        answer['price'] = 1.0
    else:

        answer['price'] = nx.attribute_assortativity_coefficient(G,'price')

    if attribute_mixing_matrix(G, 'age').trace() == len(attribute_mixing_matrix(G, 'age')):
        answer['age'] = 1.0
    else:
        answer['age'] = nx.attribute_assortativity_coefficient(G, 'age')

    if attribute_mixing_matrix(G, 'inOutcall').trace() == len(attribute_mixing_matrix(G, 'inOutcall')):
        answer['in/outcall'] = 1.0
    else:
        answer['in/outcall'] = nx.attribute_assortativity_coefficient(G, 'inOutcall')

    if attribute_mixing_matrix(G, 'ethnicity').trace() == len(attribute_mixing_matrix(G, 'ethnicity')):
        answer['ethnicity'] = 1.0
    else:
        answer['ethnicity'] = nx.attribute_assortativity_coefficient(G, 'ethnicity')

    if attribute_mixing_matrix(G, 'avgDays').trace() == len(attribute_mixing_matrix(G, 'avgDays')):
        answer['avgDays'] = 1.0
    else:
        answer['avgDays'] = nx.attribute_assortativity_coefficient(G, 'avgDays')
    # print answer
    answer['degree'] = nx.degree_assortativity_coefficient(G)
    return answer


def populate_cc_samples_with_ad_jsons(data_in=path+'data_for_memex_txt.json',
                                      ads_dir=path+'adj_lists/connected-component-samples/2_10_10_ads/',
                                      output_dir=path+'adj_lists/connected-component-samples/2_10_10_ads_txt/'):
    input_files = glob.glob(ads_dir + '*.txt')
    output_folders = [i.replace(ads_dir, '') for i in input_files]
    output_folders = [i.replace('.txt', '/') for i in output_folders]
    # print output_folders
    import os
    for output_folder in output_folders:
        if not os.path.exists(output_dir+output_folder):
            os.makedirs(output_dir+output_folder)
    output_folders = [output_dir+output_folder for output_folder in output_folders]

    file_dict = dict()
    id_data = dict()
    ids_set = set()

    # print file
    for i in range(0,len(input_files)):
        ids = json.load(codecs.open(input_files[i], 'r', 'utf-8'))
        ids_set = ids_set.union(set(ids))
        file_dict[output_folders[i]] = ids
    print ids_set
    print file_dict
    with codecs.open(data_in, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            if obj['_id'] not in ids_set:
                continue
            else:
                id_data[obj['_id']] = obj

    for f, d in file_dict.items():
        for element in d:
            out = codecs.open(f+element+'.json', 'w', 'utf-8')
            json.dump(id_data[element], out, indent=4)
            # out.write('\n')
            out.close()


def populate_cc_samples_with_ad_extractions(data_in=path+'data_for_memex.json',
                                      ads_dir=path+'adj_lists/connected-component-samples/2_10_10_ads/',
                                      output_dir=path+'adj_lists/connected-component-samples/2_10_10_ads_extractions/'):
    input_files = glob.glob(ads_dir + '*.txt')
    output_folders = [i.replace(ads_dir, '') for i in input_files]
    output_folders = [i.replace('.txt', '/') for i in output_folders]
    # print output_folders
    import os
    for output_folder in output_folders:
        if not os.path.exists(output_dir+output_folder):
            os.makedirs(output_dir+output_folder)
    output_folders = [output_dir+output_folder for output_folder in output_folders]

    file_dict = dict()
    id_data = dict()
    ids_set = set()

    # print file
    for i in range(0,len(input_files)):
        ids = json.load(codecs.open(input_files[i], 'r', 'utf-8'))
        ids_set = ids_set.union(set(ids))
        file_dict[output_folders[i]] = ids
    print ids_set
    print file_dict
    with codecs.open(data_in, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            if obj['_id'] not in ids_set:
                continue
            else:
                id_data[obj['_id']] = obj

    for f, d in file_dict.items():
        for element in d:
            out = codecs.open(f+element+'.json', 'w', 'utf-8')
            json.dump(id_data[element], out, indent=4)
            # out.write('\n')
            out.close()

def read_ethnicity_dict(ethnicity_file=path+'adj_lists/Ethnicity-Tiers.json'):
    ethnicities = json.load(open(ethnicity_file, 'r'))
    reverse_dict = dict()
    reverse_dict["northkorean"] = "asian"
    reverse_dict["southkorean"] = "asian"
    for k, v in ethnicities.items():
        if k == "african_american" or k == "ss_african" or k == "american_indian":
            for element in v:
                reverse_dict[element] = k
        elif k == "midEast_nAfrica" or k == "hispanic_latino" or k =="white_non_hispanic":
            for k1, v1 in v.items():
                for element in v1:
                    reverse_dict[element] = k
                reverse_dict[k1] = k
        elif k == "asian":
            for k1, v1 in v.items():

                for element in v1:
                    if type(element) == dict:
                        print element
                        continue
                    else:
                        reverse_dict[element] = k
                reverse_dict[k1] = k
        else:
            print k
            raise Exception

    return reverse_dict


def compute_ad_statistics(data_in=path+'data_for_memex.json',
                          output=path + 'adj_lists/statistics/ad-distributions/',
                          schema={'price', 'age', 'indicators', 'ethnicity', 'name', 'city'}, ignore_no_name=True):
    """
    Ignore_no_name=False has not been implemented yet, do not use
    We use placeholder NO_VALUE if the attribute is not in the object.
    :param data_in:
    :param output:
    :param schema:
    :param ignore_name:
    :return:
    """
    if ignore_no_name is False:
        raise Exception
    count = 1
    big_dict = dict() # i.e. for all attributes in schema
    for s in schema:
        big_dict[s] = dict()
    with codecs.open(data_in, 'r', 'utf-8') as f:
        for line in f:

            if count % 1000000 == 0:
                print count

            obj = json.loads(line[0:-1])
            if 'name' not in obj or len(obj['name']) == 0:
                count += 1
                continue
            _update_statistics_dict(schema, big_dict, obj)
            count += 1
    _write_out_statistics_to_folder(schema, big_dict, output)



def _write_out_statistics_to_folder(schema, big_dict, output):
    for s in schema:
        out = codecs.open(output+s+'.tsv', 'w')
        out.write('Attribute_value'+'\t'+'Ad_frequency'+'\n')
        sorted_keys = sorted(list(big_dict[s].keys()))
        for k in sorted_keys:
            v = big_dict[s][k]
            out.write(str(k)+'\t'+str(v)+'\n')
        out.close()

def _update_statistics_dict(schema, big_dict, obj):
    for s in schema:
        if s not in obj or (type(obj[s]) == list and len(obj[s]) == 0):
            if 'NO_VALUE' not in big_dict[s]:
                big_dict[s]['NO_VALUE'] = 0
            big_dict[s]['NO_VALUE'] += 1
            continue

        if type(obj[s]) != list:
            if obj[s] not in big_dict[s]:
                big_dict[s][obj[s]] = 0
            big_dict[s][obj[s]] += 1
        else:
            for l in obj[s]:
                if l not in big_dict[s]:
                    big_dict[s][l] = 0
                big_dict[s][l] += 1
    return


def compute_worker_statistics(data_in=path+'data_for_memex.json', input_folder = path+'adj_lists/connected-component-workers/',
                              id_int_file=path + 'adj_lists/id-int-mapping.tsv',
                              output=path + 'adj_lists/statistics/worker-distributions/',
                              schema={'price', 'age', 'indicators', 'ethnicity', 'name', 'city'}):
    """
    We haven't generated workers yet that do not have a name, so we don't need ignore name here like with
    generating ad distributions.
    :param data_in:
    :param input_folder:
    :param id_int_file:
    :param output:
    :param schema:
    :return:
    """
    files = glob.glob(input_folder + '*.txt')
    print 'finished reading in file names in connected component workers...'
    id_int = dict()
    int_data = dict()

    with codecs.open(id_int_file, 'r', 'utf-8') as f:
        for line in f:
            fields = re.split('\t', line[0:-1])
            id_int[fields[0]] = fields[1]
    print 'finished processing id int file...'
    count = 1
    with codecs.open(data_in, 'r', 'utf-8') as f:
        for line in f:

            if count % 100000 == 0:
                print count
            obj = json.loads(line[0:-1])
            if len(obj['name']) == 0:
                count += 1
                continue
            int_data[id_int[obj['_id']]] = dict()
            for s in schema:
                int_data[id_int[obj['_id']]][s] = obj[s]

            count += 1
            del id_int[obj['_id']]
    big_dict = dict()
    for s in schema:
        big_dict[s] = dict()
    print 'finished reading in data...'
    for fi in files:
        fields = list()
        count = 0
        worker_dict = dict()
        with codecs.open(fi, 'r', 'utf-8') as f:
            for line in f:
                fields = re.split(' ', line[0:-1])
                count += 1
                if count != 1:
                    print f
                    raise Exception

                for s in schema:
                    worker_dict[s] = set()
                for f in fields:
                    obj = int_data[f]
                    for s in schema:
                        if s not in obj or (type(obj[s]) == list and len(obj[s]) == 0):
                            worker_dict[s].add('NO_VALUE')
                            continue

                        if type(obj[s]) != list:
                            worker_dict[s].add(obj[s])
                        else:
                            worker_dict[s] = worker_dict[s].union(set(obj[s]))

                for s in schema:
                    worker_dict[s] = list(worker_dict[s])
                _update_statistics_dict(schema, big_dict, worker_dict)
    _write_out_statistics_to_folder(schema, big_dict, output)


def filter_similar_name_pairs(edge_list_dist=path+'adj_lists/postid-edge-list-names-phones-levenstein-sim',thresh_dist=2,
                              output_file=path+'adj_lists/name-matching/levenstein-2-edge-list.tsv'):
    out = codecs.open(output_file, 'w')
    with codecs.open(edge_list_dist, 'r') as f:
        for line in f:
            fields = re.split('\t', line[0:-1])
            if int(fields[2]) <= thresh_dist:
                out.write(line)
    out.close()




def compute_edge_list_name_similarities(edge_list_postid=path+'adj_lists/postid-edge-list-names',edge_list_phone=path + 'adj_lists/phone-edge-list-names',
                                        out_file=path+'adj_lists/postid-edge-list-names-phones-levenstein-sim'):
        out = codecs.open(out_file, 'w', 'utf-8')
        G = nx.compose(nx.read_edgelist(edge_list_phone, delimiter='\t'),
                       nx.read_edgelist(edge_list_postid, delimiter='\t'))
        print 'num name pairs processing...',str(len(G.edges()))
        count = 0
        for e in G.edges():
            name1 = re.split('-',e[0])[0]
            name2 = re.split('-',e[1])[0]

            out.write(e[0]+'\t'+e[1]+'\t'+str(_levenshtein(name1, name2))+'\n')
            if count % 50000 == 0:
                print count
            count += 1
            # break
        out.close()


memo = {} # for levenshtein memoization
def _levenshtein(s, t):
    if s == "":
        return len(t)
    if t == "":
        return len(s)
    cost = 0 if s[-1] == t[-1] else 1

    i1 = (s[:-1], t)
    if not i1 in memo:
        memo[i1] = _levenshtein(*i1)
    i2 = (s, t[:-1])
    if not i2 in memo:
        memo[i2] = _levenshtein(*i2)
    i3 = (s[:-1], t[:-1])
    if not i3 in memo:
        memo[i3] = _levenshtein(*i3)
    res = min([memo[i1] + 1, memo[i2] + 1, memo[i3] + cost])

    return res


def global_worker_homophily(worker_attribute_file=path+'adj_lists/worker-attribute-analyses/worker-attribute-distribution.jl',
                    edge_list_postid=path+'adj_lists/postid-edge-list-names',
                    ethnicity_dict_file=path+'adj_lists/Ethnicity-Tiers.json',
                    cc_day_map=path+'adj_lists/connected-component-day-map.jl',
                    edge_list_phone=path + 'adj_lists/phone-edge-list-names',
                    out_file=path+'adj_lists/worker-global-homophily-analysis-non-singletons.jl'):
    ethnicity_resolver = read_ethnicity_dict(ethnicity_dict_file)
    G = nx.compose(nx.read_edgelist(edge_list_phone, delimiter='\t'),
                   nx.read_edgelist(edge_list_postid, delimiter='\t'))
    print 'finished reading in graph'
    worker_attribute_dict = dict()
    with codecs.open(worker_attribute_file, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            worker_attribute_dict[obj.keys()[0]] = obj[obj.keys()[0]]

    with codecs.open(cc_day_map, 'r', 'utf-8') as f:
        # count = 0
        for line in f:
            obj = json.loads(line[0:-1])
            # count += 1
            # if count % 10000 == 0:
            #     print count
            k = obj.keys()[0]
            if k + '.txt' not in worker_attribute_dict:
                raise Exception
            v = obj[obj.keys()[0]]
            if len(v) <= 1:
                worker_attribute_dict[k + '.txt']['avg_days_per_week'] = 0
            else:
                dt = parse(str(v[-1])) - parse(str(v[0]))
                if dt.days + 1 < 30:
                    worker_attribute_dict[k + '.txt']['avg_days_per_week'] = 0
                else:
                    worker_attribute_dict[k + '.txt']['avg_days_per_week'] = (len(set(v)) * 7) / (dt.days + 1)
    print 'finished reading worker attribute dict'


    for worker in worker_attribute_dict.keys():
        worker = worker[0:-4] # remove the .txt from the end
        attrdict = dict()

        # average days per week
        attrdict['avgDays'] = worker_attribute_dict[worker + '.txt']['avg_days_per_week']

        # incall/outcall
        inout = "0"
        if 'indicators' in worker_attribute_dict[worker+'.txt'] and "29" in worker_attribute_dict[worker+'.txt']['indicators']\
                and worker_attribute_dict[worker+'.txt']['indicators']["29"] >= 1:
            inout = "1"
        attrdict['inOutcall'] = inout

        #ethnicity
        ethnicity_set = set()
        for eth in worker_attribute_dict[worker+'.txt']['ethnicity'].keys():
            if eth == 'NULL':
                continue
            if eth not in ethnicity_resolver:
                ethnicity_set.add(eth)
            else:
                ethnicity_set.add(ethnicity_resolver[eth])
        if len(ethnicity_set) != 0:

            worker_ethnicity = "mixed"
            if len(ethnicity_set) == 1:
                worker_ethnicity = list(ethnicity_set)[0]
            attrdict['ethnicity'] = worker_ethnicity


        #price
        price = _get_max_element(worker_attribute_dict[worker+'.txt']['prices'])

        if  price!= -1 and price != 'NULL':
            attrdict['price'] = int(float(price))

        # age
        age = _get_max_element(worker_attribute_dict[worker + '.txt']['ages'])

        if age != -1 and age != 'NULL':
                attrdict['age'] = int(age)
        G.add_node(worker, **attrdict)

    print 'finished adding attribute-updated nodes to graph'
    answer = dict()

    from networkx.algorithms.assortativity.mixing import attribute_mixing_matrix, numeric_mixing_matrix
    if numeric_mixing_matrix(G, 'price').trace() == len(numeric_mixing_matrix(G, 'price')):
        answer['price'] = 1.0
    else:

        answer['price'] = nx.numeric_assortativity_coefficient(G,'price')

    if numeric_mixing_matrix(G, 'age').trace() == len(numeric_mixing_matrix(G, 'age')):
        answer['age'] = 1.0
    else:
        answer['age'] = nx.numeric_assortativity_coefficient(G, 'age')

    if attribute_mixing_matrix(G, 'inOutcall').trace() == len(attribute_mixing_matrix(G, 'inOutcall')):
        answer['in/outcall'] = 1.0
    else:
        answer['in/outcall'] = nx.attribute_assortativity_coefficient(G, 'inOutcall')

    if attribute_mixing_matrix(G, 'ethnicity').trace() == len(attribute_mixing_matrix(G, 'ethnicity')):
        answer['ethnicity'] = 1.0
    else:
        answer['ethnicity'] = nx.attribute_assortativity_coefficient(G, 'ethnicity')

    if numeric_mixing_matrix(G, 'avgDays').trace() == len(numeric_mixing_matrix(G, 'avgDays')):
        answer['avgDays'] = 1.0
    else:
        answer['avgDays'] = nx.numeric_assortativity_coefficient(G, 'avgDays')


    answer['degree'] = nx.degree_assortativity_coefficient(G)
    json.dump(answer, codecs.open(out_file, 'w'), indent=4)



def _get_max_element(dictionary):
    K = None
    val = 0
    for k,v in dictionary.items():
        if v > val:
            K = k
            val = v

    return K


def prepare_text_embeddings_file(input_file=path+'data_for_memex_txt.json', output_file=path+'data_for_memex_serialized_txt.txt'):
    from bs4 import BeautifulSoup
    out = codecs.open(output_file, 'w', 'utf-8')
    with codecs.open(input_file, 'r', 'utf-8') as f:
        for line in f:
            soup = BeautifulSoup(json.loads(line[0:-1])['posting_body'], 'html.parser')
            # print 'printing...'
            txt = soup.get_text()
            single_line = ''
            for l in re.split('\r\n', txt):
                p = l.strip()
                if len(p) <= 0:
                    continue
                else:
                    single_line += (p.lower().replace('\n', ' ')+' ')
            out.write(single_line[0:-1]+'\n')


            # print 'finished printing'
            #
            # break

    out.close()


prepare_text_embeddings_file()
# worker_attribute_analysis()
# worker_attribute_entropy_profile()
# connected_component_ad_samples()
# populate_cc_samples_with_ad_extractions()
# global_worker_homophily()
# compute_edge_list_name_similarities()
# print read_ethnicity_dict()
# cid_homophily_analysis()
# print 10*7/(parse('20160309') - parse('20160314')).days
# from networkx.algorithms.assortativity.correlation import attribute_ac
# compute_worker_statistics()
# filter_similar_name_pairs()
# M = np.array([[1.]])
# print attribute_ac(M)
# global_worker_homophily()
# analyze_phone_postid_conn_components()
# construct_int_postid_map()
# construct_conn_comp_postid_map()
# serialize_multi_edge_list_to_graphviz_dot()
# layout_graph()
# print_ids_in_hypergraph_containing_phone()
# st = 'cara michelle-1'
# st1 = st.replace(' ','_')
# print st1
### phase 3: plot
# construct_conn_comp_phone_map()
# output_guaranteed_singleton_workers()
# output_all_singleton_workers()
# compute_phone_postid_singletons()
# analyze_phone_postid_edge_lists()
# print_largest_phone_hypergraphs()
# degree_distribution_plot_worker_network(G=analyze_phone_postid_edge_lists())
# degree_distribution_plot_worker_network()
# reverse_connected_components_map()
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
# construct_int_day_map()
# construct_conn_comp_day_map()
# conn_comp_date_range()



