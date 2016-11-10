import codecs, json

"""
This  file is designed to take a JSON lines file as input and output a file that can then
be bulk loaded in elastic search.
"""

def convert(input_file, output_file, index, type, starting_id = 1):
    """

    :param input_file:
    :param index:
    :param type:
    :param output_file:
    :return: None
    """
    file = codecs.open(output_file, 'w', 'utf-8')

    with codecs.open(input_file, 'r', 'utf-8') as f:
            for line in f:
                entry = json.loads(line)
                metadata = dict()
                if '_id' in entry:
                    metadata['index'] = _build_metadata_dict(index, type, entry['_id'])
                    del entry['_id']
                else:
                    metadata['index'] = _build_metadata_dict(index, type, starting_id)
                starting_id += 1
                json.dump(metadata, file)
                file.write('\n')
                del metadata
                json.dump(entry, file)
                file.write('\n')
    file.close()


def _build_metadata_dict(index, type, id):
    answer = dict()
    answer['_index'] = str(index)
    answer['_type'] = str(type)
    answer['_id'] = str(id)
    return answer

# path = '/home/mayankkejriwal/Downloads/dig-data/sample-datasets/escorts/'
# path = '/Users/mayankkejriwal/datasets/memex-evaluation-november/'
# convert(path+'de_output_03.jl', path+'de_output_03_elasticsearch_bulk_load.jl',
#        'de-output-03-index', 'ads')
