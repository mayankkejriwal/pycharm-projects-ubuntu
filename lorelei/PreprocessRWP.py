import glob
import codecs
import json

"""
Time to do some preprocessing on the (json) files in ReliefWebProcessed|ebola_data/ebolaXFer.
"""


def _extract_obj_fields_into_answer(obj, answer):
    """
    Modifies answer. obj is the 'full' object, 'answer' is what will be written out.
    :param obj:
    :param answer:
    :return: None
    """
    answer['uuid'] = obj['uuid']
    if 'situationFrame' in obj and 'type' in obj['situationFrame']:
        answer['situationFrame.type'] = obj['situationFrame']['type']
    else:
        answer['situationFrame.type'] = None

    if 'situationFrame' in obj and 'entities' in obj['situationFrame']:
        answer['situationFrame.entities'] = obj['situationFrame']['entities']
    else:
        answer['situationFrame.entities'] = None

    if 'loreleiJSONMapping' in obj and 'topics' in obj['loreleiJSONMapping']:
        answer['loreleiJSONMapping.topics'] = obj['loreleiJSONMapping']['topics']
    else:
        answer['loreleiJSONMapping.topics'] = None

    if 'loreleiJSONMapping' in obj and 'wordcloud' in obj['loreleiJSONMapping']:
        answer['loreleiJSONMapping.wordcloud'] = obj['loreleiJSONMapping']['wordcloud']
    else:
        answer['loreleiJSONMapping.wordcloud'] = None

    if 'loreleiJSONMapping' in obj and 'sourcedata' in obj['loreleiJSONMapping']\
            and 'theme' in obj['loreleiJSONMapping']['sourcedata']:
        answer['loreleiJSONMapping.sourcedata.theme'] = obj['loreleiJSONMapping']['sourcedata']['theme']
    else:
        answer['loreleiJSONMapping.sourcedata.theme'] = None


def condenseRWP(input_folder, output_file):
    """
    Following fields are extracted:
        uuid
        situationFrame.type
        situationFrame.entities
        loreleiJSONMapping.topics
        loreleiJSONMapping.wordcloud
        loreleiJSONMapping.sourcedata.theme
    Extraction is complete; if a field is not there, we give it a None value rather than not have it.
    :param input_folder: the folder where all the individual files are
    :param output_file: We're condensing everything into one file
    :return: None
    """
    listOfFiles = glob.glob(input_folder+'*.json') # I've tested this; it works
    out = codecs.open(output_file, 'w', 'utf-8')
    for f in listOfFiles:
        obj = None
        answer = dict()
        with codecs.open(f, 'r', 'utf-8') as openFile:
            obj = json.load(openFile)
        _extract_obj_fields_into_answer(obj, answer)
        json.dump(answer, out)
        out.write('\n')
    out.close()


def condenseWCJaccard(WCJaccard_file, output_file):
    """
    We will take a WCJaccard cluster file as input, and output a condensed file that contains only those instances
    :return: None
    """
    written_uuids = set()
    out = codecs.open(output_file, 'w', 'utf-8')
    with codecs.open(WCJaccard_file, 'r', 'utf-8') as f:
        for line in f:
            big_obj = json.loads(line)
            subject_obj = big_obj['subject']
            if subject_obj['uuid'] not in written_uuids:
                answer = dict()
                _extract_obj_fields_into_answer(subject_obj, answer)
                json.dump(answer, out)
                out.write('\n')
                written_uuids.add(subject_obj['uuid'])
            for ranked_obj in big_obj['ranked_list']:
                if ranked_obj['uuid'] not in written_uuids:
                    answer = dict()
                    _extract_obj_fields_into_answer(ranked_obj, answer)
                    json.dump(answer, out)
                    out.write('\n')
                    written_uuids.add(ranked_obj['uuid'])
    out.close()


def build_reference_uuids_file(WCJaccard_file, output_file):
    """

    :param WCJaccard_file:
    :param output_file
    :return: None
    """
    out = codecs.open(output_file, 'w', 'utf-8')
    with codecs.open(WCJaccard_file, 'r', 'utf-8') as f:
        for line in f:
            big_obj = json.loads(line)
            out.write(big_obj['subject']['uuid'])
            out.write('\n')
    out.close()


# path = '/home/mayankkejriwal/Downloads/lorelei/ebola_data/'
# condenseRWP(path+'ebolaXFer/',path+'ebolaXFer.json')
# build_reference_uuids_file(path+'WCjaccard-10-nn-for-first-10-uuids-FULL-nonindent.txt', path+'WCjaccard-10-10-reference-uuids.txt')

