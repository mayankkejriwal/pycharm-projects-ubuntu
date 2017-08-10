import codecs, re, json
import math
import networkx as nx
from networkx import algorithms, assortativity
from networkx import info, density, degree_histogram
import matplotlib.pyplot as plt


path = '/Users/mayankkejriwal/datasets/memex/nebraska-data/data-sharing/'

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


### phase 3: plot

# plot_cluster_sizes()

### phase 2: serialize each adjacency list, compute cluster size distr. Make sure to change params.

# serialize_as_list_of_sorted_ints()
# distribution_cluster_size()

### set up phase (1)

# build_edge_list() # under construction, will likely have to move to server/mapreduce
# build_name_and_postid_or_phone_adj_list()
# build_id_integer_mappings() # one off, leave alone once constructed


