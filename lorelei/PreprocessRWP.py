import glob
import codecs
import json
import csv
from dateutil import parser

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


def condenseRWP(input_folder, output_file, extractAll=False):
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
    :param extractAll: If True, then will not do the extractions above, but dump everything into the file,
    otherwise will extract the above fields.
    :return: None
    """
    listOfFiles = glob.glob(input_folder+'*.json') # I've tested this; it works
    out = codecs.open(output_file, 'w', 'utf-8')
    for f in listOfFiles:
        obj = None
        answer = dict()
        with codecs.open(f, 'r', 'utf-8') as openFile:
            obj = json.load(openFile)
        if extractAll:
            json.dump(obj, out)
        else:
            _extract_obj_fields_into_answer(obj, answer)
            json.dump(answer, out)
        out.write('\n')
    out.close()


def condenseRWPWithUUIDFilter(input_folder, uuid_file, output_file, extractAll=False):
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
    :param uuid_file: a list of uuids
    :param output_file: We're condensing everything into one file
    :param extractAll: If True, then will not do the extractions above, but dump everything into the file,
    otherwise will extract the above fields.
    :return: None
    """
    uuids_set = set()
    with codecs.open(uuid_file, 'r', 'utf-8') as f:
        for line in f:
            uuids_set.add(line[0:-1])
    listOfFiles = glob.glob(input_folder+'*.json') # I've tested this; it works
    out = codecs.open(output_file, 'w', 'utf-8')
    for f in listOfFiles:
        obj = None
        answer = dict()
        with codecs.open(f, 'r', 'utf-8') as openFile:
            obj = json.load(openFile)
        if obj['uuid'] not in uuids_set:
            continue
        else:
            uuids_set.discard(obj['uuid'])
        if extractAll:
            json.dump(obj, out)
        else:
            _extract_obj_fields_into_answer(obj, answer)
            json.dump(answer, out)
        out.write('\n')
    out.close()
    if uuids_set:
        print 'uuids_set is not empty'
        print uuids_set


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


def build_uuids_file_from_csv(uuidsFile, csvFile):
    with open(csvFile, 'rb') as f:
        reader = csv.reader(f)
        my_list = list(reader)
    out = open(uuidsFile, 'w')
    for i in range(1, len(my_list)):
        out.write(my_list[i][-2])
        out.write('\n')
    out.close()


def sort_objects_by_createdAt(origFile, outputFile):
    objects = dict()
    with codecs.open(origFile, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line)
            objects[parser.parse(obj['loreleiJSONMapping']['status']['createdAt'])] = obj
    dates = objects.keys()
    dates.sort()
    out = codecs.open(outputFile, 'w', 'utf-8')
    for date in dates:
        json.dump(objects[date], out)
        out.write('\n')
    out.close()


def build_tokens_file(condensed_file, output_file):
    """
    Each object in the final file will be uuid referencing a preprocessed word cloud. At present,
    preprocessing is limited to converting each string to its lower case equivalent.
    :param condensed_file:
    :param output_file:
    :return:
    """
    out = codecs.open(output_file, 'w', 'utf-8')
    with codecs.open(condensed_file, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line)
            object = dict()
            wordcloud = obj['loreleiJSONMapping.wordcloud']
            for i in range(0,len(wordcloud)):
                wordcloud[i] = wordcloud[i].lower()
            object[obj['uuid']] = wordcloud
            json.dump(object, out)
            out.write('\n')
    out.close()

path = '/home/mayankkejriwal/Downloads/lorelei/ebola_data/'
# build_tokens_file(path+'ebolaXFer-condensed.json', path+'tokens/ebolaXFer_lowerCase.json')
sort_objects_by_createdAt(path+'ebolaXFer-freetown-allFields.json', path+'ebolaXFer-freetown-allFields-sorted.json')
# build_uuids_file_from_csv(path+'westafrica-uuids.txt', path+'queryResultsTable-2-westafrica.csv')
# condenseRWPWithUUIDFilter(path+'data/ebolaXFer/',path+'freetown-uuids.txt',path+'ebolaXFer-freetown-condensed.json', extractAll=False)
# build_reference_uuids_file(path+'WCjaccard-10-nn-for-first-10-uuids-FULL-nonindent.txt', path+'WCjaccard-10-10-reference-uuids.txt')
# dt1 = parser.parse('Fri Sep 19 23:38:45 PDT 2014')
# dt2 = parser.parse('Fri Sep 19 07:21:56 PDT 2015')
# print dt1<dt2
