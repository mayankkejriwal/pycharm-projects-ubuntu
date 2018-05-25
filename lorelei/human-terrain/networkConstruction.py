import glob
import json
import codecs
import re

ER_path = '/Users/mayankkejriwal/Dropbox/zihai-outputs/entity_resolution_output_and_performance/'



def read_in_ER_results(ER_results, entity_type):
    files = glob.glob(ER_results+'*_'+entity_type+'.json')
    preferred_name_dict = dict()
    for f in files:
        f_dict = json.load(codecs.open(f, 'r', 'utf-8'))
        if f_dict['type'] != entity_type:
            raise Exception
        else:
            for name_dict in f_dict['originalNames']:
                for docid in name_dict['docIds']:
                    docid = re.split('ll_nepal_out_|.json',docid)[1]
                    # print docid
                    # break
                    if docid not in preferred_name_dict:
                        preferred_name_dict[docid] = dict()
                    preferred_name_dict[docid][name_dict['name']] = f_dict['preferredName']
    return preferred_name_dict


def read_in_authors(ll_nepal_authors='/Users/mayankkejriwal/Dropbox/lorelei/datasets/ll_nepal_ux_out_ui.json'):
    """
    Careful: this is a jsonlines file
    :param ll_nepal_authors:
    :return:
    """
    doc_author_dict = dict()
    with codecs.open(ll_nepal_authors, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            doc_author_dict[str(obj['_source']['id'])] = obj['_source']['screenName']
    return doc_author_dict



def GPE_PER_author_network(raw_data='/Users/mayankkejriwal/Dropbox/lorelei/datasets/ll_nepal/',out_file=
                    '/Users/mayankkejriwal/Dropbox/lorelei/human-terrain/ll_nepal_PER_GPE_resolved.tsv',
                    out_file_author='/Users/mayankkejriwal/Dropbox/lorelei/human-terrain/ll_nepal_PER_GPE_author.tsv'):
    PER_results = read_in_ER_results(
        ER_path + 'monge_elkan_similarity/normal_monge_elkan_similarity_output_thr=0.9_nepal/', 'PER')
    GPE_results = read_in_ER_results(
        ER_path + 'symmetric_monge_elkan_similarity/normal_symmetric_monge_elkan_similarity_output_thr=0.8_nepal/',
        'GPE')
    # print PER_results.keys()[0]
    # import sys
    # sys.exit(-1)
    doc_author_dict = read_in_authors()
    mentioned_with = set()
    files = glob.glob(raw_data + '*.json')
    for f in files:
        f_dict = json.load(codecs.open(f, 'r', 'utf-8'))
        resolved_entity_set = set()
        try:
            if 'PER' in f_dict:
                if str(f_dict['id']) not in PER_results:
                    print str(f_dict['id']), ' not in resolved entities. Treating as singleton and ignoring.'

                    # if type(f_dict['PER']) != list:
                    #     raise Exception
                    # for p in f_dict['PER']:
                    #     resolved_entity_set.add(p+'_PER')
                else:

                    if type(f_dict['PER']) != list:
                        raise Exception
                    for p in f_dict['PER']:
                        if p not in PER_results[str(f_dict['id'])]:
                            print 'missing entry in resolved entities...',p
                            continue
                        resolved_entity_set.add(PER_results[str(f_dict['id'])][p]+'_PER')

            if 'GPE' in f_dict:
                if str(f_dict['id']) not in GPE_results:
                    print str(f_dict['id']), ' not in resolved entities. Treating as singleton and ignoring.'
                    # if type(f_dict['GPE']) != list:
                    #     raise Exception
                    # for g in f_dict['GPE']:
                    #     resolved_entity_set.add(g+'_GPE')
                else:

                    if type(f_dict['GPE']) != list:
                        raise Exception
                    for g in f_dict['GPE']:
                        if g not in GPE_results[str(f_dict['id'])]:
                            print 'missing entry in resolved entities...',g
                            continue
                        resolved_entity_set.add(GPE_results[str(f_dict['id'])][g]+'_GPE')
        except Exception as e:
            print e
            print f_dict

        resolved_entity_set = sorted(list(resolved_entity_set))
        for i in range(len(resolved_entity_set)-1):
            for j in range(i+1, len(resolved_entity_set)):
                mentioned_with.add(resolved_entity_set[i]+'\t'+resolved_entity_set[j])

    mentioned_with = sorted(list(mentioned_with))
    out = codecs.open(out_file, 'w', 'utf-8')
    for m in mentioned_with:
        out.write(m+'\n')
    out.close()

    # we need to change the doc ids in the PER and GPE_results dicts so that the last two digits are now always 00
    PER_results_new = dict()
    GPE_results_new = dict()

    for k, v in PER_results.items():
        k1 = k[0:-2]+'00'

        if k1 in PER_results_new:
            print 'repeated ID...',
            print k,' ',
            print k1

        PER_results_new[k1] = v

    for k, v in GPE_results.items():
        k1 = k[0:-2] + '00'
        if k1 in GPE_results_new:
            print 'repeated ID...',
            print k,' ',
            print k1
        GPE_results_new[k1] = v

    out = codecs.open(out_file_author, 'w', 'utf-8')
    mentions_links = set()
    for d, a in doc_author_dict.items():
        if a is None:
            continue
        try:
            if d in PER_results_new:
                for p in PER_results_new[d].keys():
                    mentions_links.add(PER_results_new[d][p]+'_PER'+'\t'+a+'\n')
            if d in GPE_results_new:
                for g in GPE_results_new[d].keys():


                    mentions_links.add(GPE_results_new[d][g]+'_GPE'+'\t'+a+'\n')
        except Exception:
            print d
            print a
            print GPE_results_new[d]
            print PER_results_new[d]
    mentions_links = sorted(list(mentions_links))
    for m in mentions_links:
        out.write(m)
    out.close()

GPE_PER_author_network()







