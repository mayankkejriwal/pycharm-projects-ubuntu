from jsonpath_rw import jsonpath, parse
import codecs
import json, re
import gzip
import glob
import datetime


def print_first_n_lines(gz_file='/Users/mayankkejriwal/datasets/memex/user-worker-dig3-full-phase4-2016-1-1/gz/part-00000.txt.gz',
                        n=1):
    count = 0
    with gzip.open(gz_file, 'rb') as f:
        for line in f:
            print line
            count += 1
            if count == n:
                break


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


def extract_phones_urls_from_all_files(outer_folder=
                '/Users/mayankkejriwal/datasets/memex/user-worker-dig3-full-phase4-2017/'):
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
    out_strict = codecs.open(outer_folder+'strict_phones_urls.txt', 'w', 'utf-8')
    out_relaxed = codecs.open(outer_folder + 'relaxed_phones_urls.txt', 'w', 'utf-8')
    for fi in listOfFiles:
        print 'processing file: ',fi
        with gzip.open(fi, 'rb') as f:
            for line in f:
                j = json.loads(re.split('\t', line[0:-1])[1])

                strict_phones = list(set([match.value for match in jpath_strict.find(j)]))
                if len(strict_phones) != 0:
                    strict_phones = '\t'.join(strict_phones)
                    out_strict.write(j['url']+'\t'+strict_phones)
                    out_strict.write('\n')

                relaxed_phones = list(set([match.value for match in jpath_relaxed.find(j)]))
                if len(relaxed_phones) != 0:
                    relaxed_phones = '\t'.join(relaxed_phones)
                    out_relaxed.write(j['url']+'\t'+relaxed_phones)
                    out_relaxed.write('\n')

    out_strict.close()
    out_relaxed.close()


def extract_phones_urls_timestamps_from_all_files(outer_folder=
                '/Users/mayankkejriwal/datasets/memex/user-worker-dig3-full-phase4-2015-2/'):
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
    out_strict = codecs.open(outer_folder+'strict_phones_urls_timestamp.txt', 'w', 'utf-8')
    out_relaxed = codecs.open(outer_folder + 'relaxed_phones_urls_timestamp.txt', 'w', 'utf-8')
    for fi in listOfFiles:
        print 'processing file: ',fi
        with gzip.open(fi, 'rb') as f:
            for line in f:
                try:
                    j = json.loads(re.split('\t', line[0:-1])[1])
                    # t = datetime.datetime.fromtimestamp(
                    #     j['timestamp']
                    # )
                    # print t.year, t.month, t.date
                    timestamp = j['timestamp']
                    strict_phones = list(set([match.value for match in jpath_strict.find(j)]))
                    if len(strict_phones) != 0:
                        strict_phones = '\t'.join(strict_phones)
                        out_strict.write(j['url']+'\t'+str(timestamp)+'\t'+strict_phones)
                        out_strict.write('\n')

                    relaxed_phones = list(set([match.value for match in jpath_relaxed.find(j)]))
                    if len(relaxed_phones) != 0:
                        relaxed_phones = '\t'.join(relaxed_phones)
                        out_relaxed.write(j['url']+'\t'+str(timestamp)+'\t'+relaxed_phones)
                        out_relaxed.write('\n')
                except:
                    print 'there was some problem in the line below...continuing.'
                    print line
                    continue

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


def extract_2017_phones_urls_from_nebraska_file(jl_file1, jl_file2, output_file):
    out = codecs.open(output_file, 'w', 'utf-8')
    with codecs.open(jl_file1, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line)
            if 'day' in obj and str(obj['day'])[0:4] == '2017':
                if 'phone' in obj and len(obj['phone']) > 0:
                    k = '\t'.join(obj['phone'])
                    out.write(obj['url']+'\t'+k)
                    out.write('\n')
    print 'finished processing first jl file'
    with codecs.open(jl_file2, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line)
            if 'day' in obj and str(obj['day'])[0:4] == '2017':
                if 'phone' in obj and len(obj['phone']) > 0:
                    k = '\t'.join(obj['phone'])
                    out.write(obj['url']+'\t'+k)
                    out.write('\n')
    print 'finished processing second jl file'
    out.close()


def count_num_phones_greater_than_10_digits(
        phones_tsv='/Users/mayankkejriwal/Dropbox/memex-shared/networks/raw-data/strict-phones-network.txt'):
    unique_phones = list() # first, do not do the set thing, membership check could be expensive!
    unique_phones_10 = list()
    with codecs.open(phones_tsv, 'r', 'utf-8') as f:
        for line in f:
            phones = re.split('\t',line[0:-1])
            for p in phones:
                unique_phones.append(p)
                if len(p) == 10:
                    unique_phones_10.append(p)
    unique_phones = set(unique_phones)
    unique_phones_10 = set(unique_phones_10)
    print 'number of all unique phones in ',phones_tsv,' is ',str(len(unique_phones))
    print 'number of 10-digit unique phones in ', phones_tsv, ' is ', str(len(unique_phones_10))



# def deduplicate_file_by_url(phone_url_file):
#     with codecs.open(phone_url_file, 'r', 'utf-8') as f:
#         for line in f:
#             elements = re.split('\t', line[0:-1])

extract_phones_urls_timestamps_from_all_files()
# count_num_phones_greater_than_10_digits()
# print_first_n_lines()
# extract_phones_urls_from_all_files()
# path = '/Users/mayankkejriwal/datasets/memex/nebraska-data/memex_comm/'
# extract_2017_phones_from_nebraska_file(path+'data_for_memex_1.json', path+'data_for_memex_2.json', path+'phones-2017.txt')
# extract_2017_phones_urls_from_nebraska_file(path+'data_for_memex_1.json', path+'data_for_memex_2.json', path+'phones-urls-2017.txt')
