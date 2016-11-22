import codecs
import json
import re
import TextPreprocessors

def convert_seq_to_jlines(seq_file, output_file):
    """

    :param seq_file: Must be decompressed into text already, and each line is a tuple. Not all seq. files
    will obey this format.
    :param output_file: A json lines file
    :return:
    """
    out = codecs.open(output_file, 'w', 'utf-8')
    with codecs.open(seq_file, 'r', 'utf-8') as f:
        for line in f:
            k = eval(line)
            json.dump(json.loads(k[1]), out)
            out.write('\n')
    out.close()


def check_tab_delimiting(input_file):
    """
    The goal is simple. We take the part-00000.txt, and for each line, do a tab delimiting. Make
    sure the number of fields is exactly 2. That way we can proceed with serialization.
    :param input_path:
    :return:
    """
    count = 0
    with codecs.open(input_file, 'r', 'utf-8') as f:
        for line in f:
            count += 1
            if len(re.split('\t',line))!=2:
                print 'Delimiting problems in line: '+str(count)


def convert_txt_tojlines(input_file, output_file):
    out = codecs.open(output_file, 'w', 'utf-8')
    with codecs.open(input_file, 'r', 'utf-8') as f:
        for line in f:
            json.dump(json.loads(re.split('\t',line)[1]), out)
            out.write('\n')
    out.close()


def segregate_annotated_cities(annotated_cities_jl, output_folder):
    """
    Each field (from the ones we care about) gets mapped to its own document in the output folder.
    We use set based semantics, so there's no notion of order or correspondence between the files.
    We'll lower-case everything before writing out.
    :param annotated_cities_jl:
    :param output_folder:
    :return:
    """
    fields = ['annotated_states', 'correct_states', 'annotated_cities', 'correct_cities',
              'annotated_cities_title', 'correct_cities_title', 'correct_country']
    field_dict = dict()
    for field in fields:
        field_dict[field] = set()
    with codecs.open(annotated_cities_jl, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line)
            for field in fields:
                if field in obj and field != 'correct_country':
                    field_dict[field] = field_dict[field].union(set(TextPreprocessors.
                                                        TextPreprocessors._preprocess_tokens(obj[field],['lower'])))
                elif field in obj and field == 'correct_country':
                    country = list()
                    country.append(obj[field])
                    field_dict[field] = field_dict[field].union(set(TextPreprocessors.
                                                    TextPreprocessors._preprocess_tokens(country, ['lower'])))
    for field in fields:
        out = codecs.open(output_folder+field+'.txt', 'w', 'utf-8')
        for element in field_dict[field]:
            out.write(element)
            out.write('\n')
        out.close()

# path = '/Users/mayankkejriwal/datasets/memex-evaluation-november/annotated-cities/'
# segregate_annotated_cities(path+'ann_city_title_state_1_50.txt',path)
# path='/Users/mayankkejriwal/ubuntu-vm-stuff/home/mayankkejriwal/tmp/'
# convert_txt_tojlines(path+'part-00000.txt', path+'part-00000.json')

