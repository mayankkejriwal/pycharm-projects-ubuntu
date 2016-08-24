import json
import codecs

"""
This file is supposed to evaluate a simple baseline. We match mentions by seeing if
they exactly match (case insensitive). Since we don't have a ground-truth, we need to sample the results
to see if we're right.
"""


def baseline(records_file, output_file):
    """
    :param records_file Must not be indented
    :param output_file
    :return:
    """
    results = dict() # key will be a mention, value will be a list of uuids
    with codecs.open(records_file, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line)
            if 'situationFrame' not in obj or 'entities' not in obj['situationFrame'] or not obj['situationFrame']['entities']:
                # print obj
                continue
            # print obj
            # print obj['situationFrame']['entities']
            for entity in obj['situationFrame']['entities']:
                ent = entity.lower()
                if ent not in results:
                    results[ent] = list()
                results[ent].append(obj['uuid'])

    out = codecs.open(output_file, 'w', 'utf-8')
    for r, v in results.items():
        if len(v) > 1:
            answer = dict()
            answer[r] = v
            json.dump(answer, out)
            out.write('\n')
    out.close()

# path = '/home/mayankkejriwal/Downloads/lorelei/ebola_data/'
# baseline(path+'record_dump_50000.json', path+'exactMatchOnRecordDump50000.json')