import codecs
import re
import json

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




path = '/Users/mayankkejriwal/datasets/lorelei/KB-CIA/'
# prune_lat_long_file(path+'populated_places.txt', path+'geonames_lat_sorted.tsv',
#                     path+'geonames_lat_sorted_pruned.tsv')
# isolate_cities_villages(path+'KB_geonames.nt', path+'populated_places.txt')
# sort_lat_long_file(path+'geonames_lat_long.tsv', path+'geonames_long_sorted.tsv', False)
# extract_latitude_longitude(path+'KB_geonames.nt', path+'geonames_lat_long.tsv')