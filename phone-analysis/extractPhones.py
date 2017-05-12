from jsonpath_rw import jsonpath, parse
import codecs
import json, re
import gzip
import glob

def extract_phones_from_all_files(outer_folder=
                '/Users/mayankkejriwal/datasets/memex/user-worker-dig3-full-phase4-2017-feb/'):
    """
    Will write out strict phones in outer_folder+'strict_phones.txt' and relaxed_phones in outer_folder+'relaxed_phones.txt'
    Note that while we do 'set' checking for each individual json there can be many repeated phones across the file.
    Each line will be tab-delimited and represent the phones per json.
    :param outer_folder:
    :return:
    """
    jpath_relaxed = parse('fields.phone.relaxed[*].name')
    jpath_strict = parse('fields.phone.strict[*].name')
    listOfFiles = glob.glob(outer_folder + 'gz/*.txt.gz')
    out_strict = codecs.open(outer_folder+'strict_phones.txt', 'w', 'utf-8')
    out_relaxed = codecs.open(outer_folder + 'relaxed_phones.txt', 'w', 'utf-8')
    for fi in listOfFiles:
        print 'processing file: ',fi
        with gzip.open(fi, 'rb') as f:
            for line in f:
                j = json.loads(re.split('\t', line[0:-1])[1])

                strict_phones = list(set([match.value for match in jpath_strict.find(j)]))
                if len(strict_phones) != 0:
                    strict_phones = '\t'.join(strict_phones)
                    out_strict.write(strict_phones)
                    out_strict.write('\n')

                relaxed_phones = list(set([match.value for match in jpath_relaxed.find(j)]))
                if len(relaxed_phones) != 0:
                    relaxed_phones = '\t'.join(relaxed_phones)
                    out_relaxed.write(relaxed_phones)
                    out_relaxed.write('\n')

    out_strict.close()
    out_relaxed.close()


def extract_2017_phones_from_nebraska_file(jl_file1, jl_file2, output_file):
    out = codecs.open(output_file, 'w', 'utf-8')
    with codecs.open(jl_file1, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line)
            if 'day' in obj and str(obj['day'])[0:4] == '2017':
                if 'phone' in obj and len(obj['phone']) > 0:
                    k = '\t'.join(obj['phone'])
                    out.write(k)
                    out.write('\n')
    print 'finished processing first jl file'
    with codecs.open(jl_file2, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line)
            if 'day' in obj and str(obj['day'])[0:4] == '2017':
                if 'phone' in obj and len(obj['phone']) > 0:
                    k = '\t'.join(obj['phone'])
                    out.write(k)
                    out.write('\n')
    print 'finished processing second jl file'
    out.close()

path = '/Users/mayankkejriwal/datasets/memex/nebraska-data/memex_comm/'
extract_2017_phones_from_nebraska_file(path+'data_for_memex_1.json', path+'data_for_memex_2.json', path+'phones-2017.txt')
