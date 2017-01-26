import codecs
import re
import math
from DeriveProbabilityDistributions import *

def extract_latitude_longitude(geonames_KB, output_file):
    lat_long_dict = dict()
    count = 1
    with codecs.open(geonames_KB, 'r', 'utf-8') as f:
        for line in f:
            count += 1
            if count % 50000 == 0:
                print 'processing line...',count
            # if count > 100:
            #     break
            fields = re.split('\t',line[0:-1])
            if fields[1] != '<latitude>' and fields[1]!='<longitude>':
                continue
            if fields[0] not in lat_long_dict:
                lat_long_dict[fields[0]] = dict()
            lat_long_dict[fields[0]][fields[1]] = fields[2]
    out = codecs.open(output_file, 'w', 'utf-8')
    for k, v in lat_long_dict.items():
        if '<latitude>' not in v or '<longitude>' not in v:
            print 'one/both of the lat/long is missing for ',k
            continue
        else:
            out.write(k+'\t'+v['<latitude>']+'\t'+v['<longitude>']+'\n')
    out.close()


def sort_lat_long_file(lat_long_file, output_file, lat=True):
    line_dict = dict()
    count = 1
    with codecs.open(lat_long_file, 'r', 'utf-8') as f:
        for line in f:
            count += 1
            if count % 50000 == 0:
                print 'processing line...', count
            # if count > 100:
            #     break
            if lat:
                quant = float(re.split('\t',line[0:-1])[1][1:-1])
            else:
                quant = float(re.split('\t',line[0:-1])[2][1:-1])
            if quant not in line_dict:
                line_dict[quant] = list()
            line_dict[quant].append(line)
    h = line_dict.keys()
    h.sort()
    out = codecs.open(output_file, 'w', 'utf-8')
    for i in h:
        for line in line_dict[i]:
            out.write(line)
    out.close()


def isolate_cities_villages(geonames_KB, output_file):
    cities = ['PPL', 'PPLA', 'PPLA2', 'PPLA3', 'PPLA4', 'PPLC', 'PPLCH', 'PPLF', 'PPLG', 'PPLH',
              'PPLL', 'PPLQ', 'PPLR', 'PPLS', 'PPLW', 'PPLX', 'STLMT']
    for i in range(len(cities)):
        cities[i] = '<'+cities[i]+'>'
    eligible_places = set()
    with codecs.open(geonames_KB, 'r', 'utf-8') as f:
        for line in f:
            fields = re.split('\t', line[0:-1])
            if fields[1] != '<feature_code>':
                continue
            if fields[2] in cities:
                eligible_places.add(fields[0])
    out = codecs.open(output_file, 'w', 'utf-8')
    for p in eligible_places:
        out.write(p+'\n')
    out.close()

def prune_lat_long_file(populated_places, lat_long_file, output_file):
    cities = set()
    with codecs.open(populated_places, 'r', 'utf-8') as f:
        for line in f:
            cities.add(line[0:-1])
    out = codecs.open(output_file, 'w', 'utf-8')
    with codecs.open(lat_long_file, 'r', 'utf-8') as f:
        for line in f:
            fields = re.split('\t', line[0:-1])
            if fields[0] in cities:
                out.write(line)
    out.close()


def write_out_haversine_weights(location, list_of_locations, out):
    lat1 = location[1]
    long1 = location[2]
    dist_dict = dict()
    for l in list_of_locations:
        lat2 = l[1]
        long2 = l[2]
        R = 6371000 # in meters, radius of the earth
        phi1 = math.radians(lat1)
        phi2 = math.radians(lat2)
        del_phi = math.radians(lat2-lat1)
        del_lambda = math.radians(long2-long1)
        a = math.sin(del_phi/2)*math.sin(del_phi/2)+\
            math.cos(phi1)*math.cos(phi2)*math.sin(del_lambda/2)*math.sin(del_lambda/2)
        c = 2*math.atan2(math.sqrt(a),math.sqrt(1-a))
        dist = int(math.ceil((R*c)/1000)) # compute the haversine distance between l and lat1, long1
        dist_dict[l[0]] = dist
    string = location[0]
    for k, v in dist_dict.items():
        string += ('\t'+k+'\t'+str(v))
    out.write(string+'\n')


def compute_location_weights(sorted_list_file, output_file, limit=50):
    big_list = list()
    with codecs.open(sorted_list_file, 'r', 'utf-8') as f:
        for line in f:
            tmp = list()
            fields = re.split('\t', line[0:-1])
            tmp.append(fields[0])
            tmp.append(float(fields[1][1:-1]))
            tmp.append(float(fields[2][1:-1]))
            big_list.append(tmp)
    print 'finished reading in the sorted list file...'
    out = codecs.open(output_file, 'w', 'utf-8')
    for i in range(0, len(big_list)-1):
        write_out_haversine_weights(big_list[i], big_list[i+1:i+limit], out)
    out.close()


def _test_haversine():
    lat1 = -77.846
    long1 = 166.676
    lat2 = -63.3209
    long2 = -57.89956
    R = 6371000  # in meters, radius of the earth
    phi1 = math.radians(lat1)
    print phi1
    phi2 = math.radians(lat2)
    print phi2
    del_phi = math.radians(lat2 - lat1)
    del_lambda = math.radians(long2 - long1)
    a = math.sin(del_phi / 2) * math.sin(del_phi / 2) + \
        math.cos(phi1) * math.cos(phi2) * math.sin(del_lambda / 2) * math.sin(del_lambda / 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    dist = R * c  # compute the haversine distance between l and lat1, long1
    print int(math.ceil(dist / 1000))


def sort_dist_file_by_name(dist_file, output_file):
    line_dict = dict()
    with codecs.open(dist_file, 'r', 'utf-8') as f:
        for line in f:
            line_dict[re.split('\t',line[0:-1])[0]] = line
    places = line_dict.keys()
    places.sort()
    out = codecs.open(output_file, 'w', 'utf-8')
    for p in places:
        out.write(line_dict[p])
    out.close()


def merge_fields_into_single_line(lat_fields, long_fields):
    tmp_dict = dict()
    for i in range(1, len(lat_fields), 2):
        tmp_dict[lat_fields[i]] = lat_fields[i+1]
    for i in range(1, len(long_fields), 2):
        if long_fields[i] not in tmp_dict:
            tmp_dict[long_fields[i]] = long_fields[i+1]
    string = lat_fields[0]
    for k, v in tmp_dict.items():
        string += ('\t'+k+'\t'+v)
    return string+'\n'


def merge_lat_long_dist_files(lat_file, long_file, output_file):
    merged_dict = dict()
    with codecs.open(lat_file, 'r', 'utf-8') as f:
        for line in f:
            merged_dict[re.split('\t', line[0:-1])[0]]=re.split('\t', line[0:-1])
    print 'finished reading in latitude file...'
    out = codecs.open(output_file, 'w', 'utf-8')
    with codecs.open(long_file, 'r', 'utf-8') as f:
        for line in f:
            fields = re.split('\t', line[0:-1])
            if fields[0] not in merged_dict:
                out.write(line)
            else:
                out.write(merge_fields_into_single_line(merged_dict[fields[0]],fields))
    # print 'finished building merge dict...'

    # places = merged_dict.keys()
    # places.sort()
    # for p in places:
    #     out.write(merged_dict[p])
    out.close()


def map_populated_places(populated_places, output_file):
    out = codecs.open(output_file, 'w', 'utf-8')
    count = 1
    with codecs.open(populated_places, 'r', 'utf-8') as f:
        for line in f:
            out.write(line[0:-1]+'\t'+str(count)+'\n')
            count += 1
    out.close()

def map_merged_dist_file(mapped_populated_places, merged_dist, output_file):
    mapped_dict = dict()
    with codecs.open(mapped_populated_places, 'r', 'utf-8') as f:
        for line in f:
            fields = re.split('\t', line[0:-1])
            mapped_dict[fields[0]] = int(fields[1])
    print 'finished reading in mapped_dict...'
    count = 1
    out = codecs.open(output_file, 'w', 'utf-8')
    with codecs.open(merged_dist, 'r', 'utf-8') as f:
        for line in f:
            try:
                fields = re.split('\t', line[0:-1])
                fields[0] = int(mapped_dict[fields[0]])
                for i in range(1, len(fields)-1, 2):
                    fields[i] = int(mapped_dict[fields[i]])
                string = str(fields[0])
                for i in range(1, len(fields)):
                    string += ('\t' + str(fields[i]))
                string = string + '\n'
                out.write(string)
                count += 1
            except Exception as e:
                print e
                print 'in line...',str(count)
                break
    out.close()

def correct_mapped_merged_file(mapped_merged_file, output_file):
    """
    Written because of a bug in the output of map_merged_dist_file which has since been
    corrected
    :param mapped_merged_file:
    :param output_file:
    :return:
    """
    out = codecs.open(output_file, 'w', 'utf-8')
    with codecs.open(mapped_merged_file, 'r', 'utf-8') as f:
        for line in f:
            fields = re.split('\t', line[0:-1])
            string = fields[0]
            for field in fields[2:]:
                string += ('\t' + field)
            string += '\n'
            out.write(string)
    out.close()


def get_countries_file(populated_file, geonames_KB, output_file):
    """
    Outputs tab-delimited lines with the populated place and the country code
    :param populated_file: the output of get_population_file
    :param geonames_KB:
    :param output_file:
    :return:
    """
    set_pop_places = set()
    with codecs.open(populated_file, 'r', 'utf-8') as f:
        for line in f:
            set_pop_places.add(re.split('\t', line[0:-1])[0])
    out = codecs.open(output_file, 'w', 'utf-8')
    count = 1
    with codecs.open(geonames_KB, 'r', 'utf-8') as f:
        for line in f:
            count += 1
            if count % 50000 == 0:
                print 'processing line...', count
            # if count > 100:
            #     break
            fields = re.split('\t', line[0:-1])
            if fields[1] != '<country_code>':
                continue
            if fields[0] not in set_pop_places:
                continue
            out.write(fields[0] + '\t' + str(fields[2]) + '\n')
    out.close()

def get_countries_file_counts(countries_file, statistics_file):
    countries_dict = dict()
    with codecs.open(countries_file, 'r', 'utf-8') as f:
        for line in f:
            country_code = re.split('\t', line[0:-1])[1]
            if country_code not in countries_dict:
                countries_dict[country_code] = 0
            countries_dict[country_code] += 1
    out = codecs.open(statistics_file, 'w', 'utf-8')
    for k, v in countries_dict.items():
        out.write(k + '\t' + str(v) + '\n')
    out.close()


def get_population_file(populated_places, geonames_KB, output_file):
    list_pop_places = list()
    with codecs.open(populated_places, 'r', 'utf-8') as f:
        for line in f:
            list_pop_places.append(line[0:-1])
    set_pop_places = set(list_pop_places)
    count = 1
    populations = dict()
    with codecs.open(geonames_KB, 'r', 'utf-8') as f:
        for line in f:
            count += 1
            if count % 50000 == 0:
                print 'processing line...', count
            # if count > 100:
            #     break
            fields = re.split('\t', line[0:-1])
            if fields[1] != '<population>':
                continue
            if fields[0] not in set_pop_places:
                continue
            populations[fields[0]] = int(fields[2][1:-1])
    out = codecs.open(output_file, 'w', 'utf-8')
    for place in list_pop_places:
        out.write(place+'\t'+str(populations[place])+'\n')
    out.close()


def prune_merged_dist(merged_dist, populated_places_population, mapped_populated_places, output_file):
    """
    The goal is to prune merged dist so that it only contains 'nodes' that have non zero
    population
    :param merged_dist:
    :param populated_places_population:
    :return:
    """
    set_places = set()
    place_ints = set()
    with codecs.open(populated_places_population, 'r', 'utf-8') as f:
        for line in f:
            fields = re.split('\t', line[0:-1])
            if int(fields[1]) > 0:
                set_places.add(fields[0])
    with codecs.open(mapped_populated_places, 'r', 'utf-8') as f:
        for line in f:
            fields = re.split('\t', line[0:-1])
            if fields[0] in set_places:
                place_ints.add(int(fields[1]))
    out = codecs.open(output_file, 'w', 'utf-8')
    with codecs.open(merged_dist, 'r', 'utf-8') as f:
        for line in f:
            fields = re.split('\t', line[0:-1])
            if int(fields[0]) not in place_ints:
                continue
            string = fields[0]
            flag = False
            for i in range(1, len(fields)-1, 2):
                if int(fields[i]) in place_ints:
                    flag = True
                    string += ('\t'+fields[i]+'\t'+fields[i+1])
            if flag == True:
                string += '\n'
                out.write(string)

    out.close()


def get_country_integer_nodes(mapped_countries_file, mapped_file, country_codes=set(['<FR>','<AU>','<GM>'])):
    """
    if country codes is None, we will get all integer nodes in the graph
    :param mapped_countries_file:
    :param mapped_file:
    :param country_codes:
    :return:
    """
    set_pop_places = set()
    integer_nodes = set()
    with codecs.open(mapped_countries_file, 'r', 'utf-8') as f:
        for line in f:
            fields = re.split('\t',line[0:-1])
            if not country_codes:
                set_pop_places.add(fields[0])
            elif fields[1] in country_codes:
                set_pop_places.add(fields[0])
    with codecs.open(mapped_file, 'r', 'utf-8') as f:
        for line in f:
            fields = re.split('\t',line[0:-1])
            if fields[0] in set_pop_places:
                integer_nodes.add(int(fields[1]))
    print 'got num. integer nodes',str(len(integer_nodes))
    return integer_nodes


def read_weighted_adj_graph(adj_graph_file):
    """

    :param adj_graph_file:
    :return: The output is a dictionary, where the key is an int, and the value is a list with two elements (both lists)
    the first being a list of ints, the second being a list of floats, repr. a probability distribution over the first list.
    """
    weighted_dict = dict()
    with codecs.open(adj_graph_file, 'r', 'utf-8') as f:
        for line in f:
            fields = re.split('\t', line[0:-1])
            list1 = list()
            list2 = list()
            for i in range(1, len(fields), 2):
                list1.append(int(fields[i]))
                list2.append(float(fields[i+1]))
            biglist = list()
            biglist.append(list1)
            biglist.append(list2)
            weighted_dict[int(fields[0])] = biglist
    return weighted_dict


def build_weighted_adj_graph_v1(dist_file, output_file):
    """
    v1 because of the specific weighting scheme we use. First, I do max(e, dist) and then take the natural log.
    The inverse of the natural log becomes the unnormalized weight. We do an l1 norm to ensure a probability distr.
    For engineering capabilities, I am writing out these probability distr. derivations in a separate file.
    :param dist_file:
    :param output_file:
    :return:
    """
    out = codecs.open(output_file, 'w', 'utf-8')
    with codecs.open(dist_file, 'r', 'utf-8') as f:
        for line in f:
            fields = re.split('\t', line[0:-1])
            edge_dict = dict()
            for i in range(1, len(fields), 2):
                edge_dict[fields[i]] = int(fields[i+1])
            inverse_log_max(edge_dict) # modifies edge_dict
            write_string = fields[0]
            for k, v in edge_dict.items():
                write_string += ('\t'+k+'\t'+str(v))
            out.write(write_string)
            out.write('\n')
    out.close()


path = '/Users/mayankkejriwal/datasets/lorelei/KB-CIA/'
# build_weighted_adj_graph_v1(path+'pruned_mapped_merged_dist.tsv', path+'prob_adjacency_file_1.tsv')
# integer_nodes = get_country_integer_nodes(path+'populated_places_countries.tsv',path+'mapped_populated_places.txt')
# prune_merged_dist(path+'mapped_merged_dist.tsv',path+'populated_places_populations.tsv',
#                   path+'mapped_populated_places.txt', path+'pruned_mapped_merged_dist.tsv')
# get_countries_file_counts(path+'populated_places_countries.tsv', path+'populated_places_countries_counts.tsv')
# get_population_file(path+'populated_places.txt',path+'KB_geonames.nt',path+'populated_places_populations.tsv')
# correct_mapped_merged_file(path+'mapped_merged_dist.tsv',path+'mapped_merged_dist_2.tsv')
# map_merged_dist_file(path+'mapped_populated_places.txt',path+'merged_dist.tsv',
#                      path+'mapped_merged_dist.tsv')
# map_populated_places(path+'populated_places.txt', path+'mapped_populated_places.txt')
# merge_lat_long_dist_files(path+'lat_dist_sortedByKey.tsv',path+'long_dist_sortedByKey.tsv', path+'merged_dist.tsv')
# sort_dist_file_by_name(path+'lat_sorted_dist.tsv', path+'lat_dist_sortedByKey.tsv')
# compute_location_weights(path+'geonames_long_sorted_pruned.tsv', path+'long_sorted_dist.tsv')
# prune_lat_long_file(path+'populated_places.txt', path+'geonames_lat_sorted.tsv',
#                     path+'geonames_lat_sorted_pruned.tsv')
# isolate_cities_villages(path+'KB_geonames.nt', path+'populated_places.txt')
# sort_lat_long_file(path+'geonames_lat_long.tsv', path+'geonames_long_sorted.tsv', False)
# extract_latitude_longitude(path+'KB_geonames.nt', path+'geonames_lat_long.tsv')