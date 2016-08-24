import codecs
import json

"""
Primarily for evaluating the results of TestRandomIndexing, JaccardOnWordCloudBaseline and any other baselines (e.g. tf-idf)
that we will use for forming clusters.
"""


def  _derive_list_of_objects(RWPDirPath, list_of_uuids):
    """
    Based closely, but not equivalent to, on Jaccard..._derive...
    :param RWPDirPath:
    :param list_of_uuids: a list of uuids
    :return: a list of objects
    """
    list_of_objects = list()
    for uuid in list_of_uuids:
        with codecs.open(RWPDirPath+'data_'+uuid+'.json', 'r', 'utf-8') as f:
            list_of_objects.append(json.load(f))
    return list_of_objects


def expand_ranked_uuid_file(ranked_uuid_file, output_file,
                            RWPDirPath='/home/mayankkejriwal/Downloads/lorelei/reliefWebProcessed/'):
    """

    :param ranked_uuid_file: e.g. 250d-10-nn-for-first-10-uuids.txt
    :param output_file: e.g. something like WCjaccard-10-nn-for-first-10-uuids-FULL.txt
    :return: None
    """
    out = codecs.open(output_file, 'w', 'utf-8')
    with codecs.open(ranked_uuid_file, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line)
            answer = dict()
            for k, v in obj.items():
                answer['ranked_list'] = _derive_list_of_objects(RWPDirPath, v)
                answer['subject'] = _derive_list_of_objects(RWPDirPath, [k])[0]
            json.dump(answer, out, indent=4)
            out.write('\n')
    out.close()

# path = '/home/mayankkejriwal/Downloads/lorelei/reliefWebProcessed-prepped/'
# expand_ranked_uuid_file(path+'3000d-10-nn-for-first-10-uuids.txt', path+'3000d-10-nn-for-first-10-uuids-FULL.txt')


