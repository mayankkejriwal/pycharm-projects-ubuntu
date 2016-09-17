import codecs
import json
import JaccardOnWordCloudBaseline
import TestRandomIndexing

def _extract_hashtags_from_wordcloud(jsonObj):
    answer = set()
    if 'loreleiJSONMapping.wordcloud' in jsonObj and jsonObj['loreleiJSONMapping.wordcloud']:
        for word in jsonObj['loreleiJSONMapping.wordcloud']:
            if len(word) >= 1 and word[0] == '#':
                answer.add(word)
    if answer:
        return answer


def _extract_entities(jsonObj):
    answer = set()
    if 'situationFrame.entities' in jsonObj and jsonObj['situationFrame.entities']:
        answer = answer.union(set(jsonObj['situationFrame.entities']))
    if answer:
        return answer


def field_value_expansion(seed_file, full_file, output_file,
                          field_list=['_extract_hashtags_from_wordcloud','_extract_entities']):
    """
    The premise is simple. We take the seed file, and use the field list to obtain sets of values/field for
    the seed file. Any object in the full file that shares a value in the right field with any of the seed
    objects will be output.  By definition, this includes all objects in the seed file as well, assuming
    the seed entities are a subset of the full entities. In our initial run, we designed it for hashtags in the field
    loreleiJSONMapping.wordcloud field and situationFrame.entities using freetown/westafrica as the seed file
    and ebolaXFer-condensed as the full file. We're defined a 'field' as a function on each json for
     full expressiveness
    :param seed_file: A json lines file (condensed or otherwise), where each json is treated as a seed
    :param full_file: Another json lines file on which the clustering will be conducted
    :param output_file: A json lines file to which the ground-truth will be output
    :param field_list: A list of fields (i.e. functions) that will be applied to each object both in seed_file and full_file
    Each function will take a json dict as input and return either None or a set
    :return: None
    """
    val_dict = dict()
    # print globals()
    for field in field_list:
        val_dict[field] = set()
    with codecs.open(seed_file, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line)
            for field in field_list:
                field_val = globals()[field](obj)
                if field_val:
                    val_dict[field] = val_dict[field].union(field_val)
    out = codecs.open(output_file, 'w', 'utf-8')
    with codecs.open(full_file, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line)
            for field in field_list:
                field_val = globals()[field](obj)
                if field_val and val_dict[field].intersection(field_val):
                    json.dump(obj, out)
                    out.write('\n')
    out.close()


def jaccard_prune_expanded_file_to_top_k(seed_file, expanded_file, output_file, k=1000):
    """
    The westafrica-expanded file is too big and contains too many objects (over 7000). We'll compute the average
    jaccard of each entity in the expanded file to all entities in the seed file (including seed
    objects themselves, if they show up in the expanded file). The top k will get
    selected and written out to output_file. There is a chance seed elements do NOT get written out to output file.
    :param seed_file:
    :param expanded_file:
    :param output_file:
    :param k:
    :return:
    """

    # read in the seed_file, with uuid referencing the wordcloud
    seeds = list()
    objects_dict = dict()
    score_dict = dict() # a score references a list of uuids
    with codecs.open(seed_file, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line)
            seeds.append(obj['loreleiJSONMapping.wordcloud'])
    with codecs.open(expanded_file, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line)
            objects_dict[obj['uuid']] = obj
            score = JaccardOnWordCloudBaseline._average_jaccard_on_word_clouds(
                obj['loreleiJSONMapping.wordcloud'], seeds)
            if score not in score_dict:
                score_dict[score] = list()
            score_dict[score].append(obj['uuid'])
    all_k = TestRandomIndexing._extract_top_k(score_dict, k=1, disable_k=True)
    top_k = TestRandomIndexing._extract_top_k_unique_from_list(all_k, k)
    out = codecs.open(output_file, 'w', 'utf-8')
    for uuid in top_k:
        json.dump(objects_dict[uuid], out)
        out.write('\n')
    out.close()


path = '/home/mayankkejriwal/Downloads/lorelei/ebola_data/'
jaccard_prune_expanded_file_to_top_k(path+'ebolaXFer-freetown-condensed.json',path+'freetown-expanded.json',
                                     path+'westafrica-top-all.json')