import codecs
import json

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

path='/home/mayankkejriwal/Downloads/memex-cp4-october/'
convert_seq_to_jlines(path+'part-00000', path+'part-00000.json')