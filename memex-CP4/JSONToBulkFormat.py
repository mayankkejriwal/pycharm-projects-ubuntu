import codecs, json

"""
This  file is designed to take a JSON lines file as input and output a file that can then
be bulk loaded in elastic search.
"""

def convert(input_file, output_file, index, type, id_field=None, starting_id = 1):
    """

    :param input_file:
    :param index:
    :param type:
    :param output_file:
    :return: None
    """
    file = codecs.open(output_file, 'w', 'utf-8')
    bad_jsons = 0
    good_jsons = 0
    with codecs.open(input_file, 'r') as f:
            for line in f:
                try:
                    entry = json.loads(line)
                    # print entry
                    metadata = dict()
                    if 'inferlink_posting-date' in entry:
                        del entry['inferlink_posting-date']

                    forbidden_fields = ['posting-date', 'review-id']
                    for field in forbidden_fields:
                        if 'high_precision' in entry and field in entry['high_precision']:
                            del entry['high_precision'][field]
                        if 'high_recall' in entry and field in entry['high_recall']:
                            del entry['high_recall'][field]

                    if '_id' in entry:
                        metadata['index'] = _build_metadata_dict(index, type, entry['_id'])
                        del entry['_id']
                    elif id_field:

                        metadata['index'] = _build_metadata_dict(index, type, entry[id_field])
                    else:
                        metadata['index'] = _build_metadata_dict(index, type, starting_id)
                        starting_id += 1
                    json.dump(metadata, file)
                    file.write('\n')
                    del metadata
                    json.dump(entry, file)
                    file.write('\n')
                    good_jsons += 1
                except:
                    print line
                    print 'GAP'
                    bad_jsons += 1
                    continue


    file.close()
    print 'bad jsons...',
    print bad_jsons
    print 'good jsons...',
    print good_jsons


def get_id_statistics(jl_file_1, jl_file_2):
    id_set1 = set()
    id_set2 = set()
    with codecs.open(jl_file_1, 'r') as f:
            for line in f:
                id_set1.add(json.loads(line)['_id'])
    with codecs.open(jl_file_2, 'r') as f:
        for line in f:
            id_set2.add(json.loads(line)['_id'])
    print 'id set 1...',
    print len(id_set1)
    print 'id set 2...',
    print len(id_set2)
    print 'intersection of id sets...',
    print len(id_set1.intersection(id_set2))

def _build_metadata_dict(index, type, id):
    answer = dict()
    answer['_index'] = str(index)
    answer['_type'] = str(type)
    answer['_id'] = str(id)
    return answer

# path = '/home/mayankkejriwal/Downloads/dig-data/sample-datasets/escorts/'
# path = '/Users/mayankkejriwal/datasets/memex-evaluation-november/clustering-mapping-CDR-id/'
# convert(path+'phone-sim-de-24-0.1.txt', path+'phone-sim-de-24-0.1-bulk-load.jl',
#        'gt-index-1', 'clusters', 'cluster-id')
# get_id_statistics(path+'de_output_07_no_raw.jl', path +'de_output_03.jl')
