import codecs
import json
import re

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

# path='/Users/mayankkejriwal/ubuntu-vm-stuff/home/mayankkejriwal/tmp/'
# convert_txt_tojlines(path+'part-00000.txt', path+'part-00000.json')

