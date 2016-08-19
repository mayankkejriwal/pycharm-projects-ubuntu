import glob
import codecs
import json

"""
Time to do some preprocessing on the (json) files in ReliefWebProcessed.
"""


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

        json.dump(answer, out)
        out.write('\n')
    out.close()

path = '/home/mayankkejriwal/Downloads/lorelei/'
condenseRWP(path+'reliefWebProcessed/', path+'reliefWebProcessed-prepped/preprocessed-objects.json')

