import codecs
import json

import rank
import train_ranker

#Files to be present in home dir
TRAINING_FILE_CITIES = 'manual_7_cities.jl'
TRAINING_FILE_NAMES = 'manual_50_names.jl'
TRAINING_FILE_ETHNICITIES = 'manual_50_ethnicities.jl'
ACTUAL_FILE_CITIES = 'manual_50_cities.jl'
ACTUAL_FILE_NAMES = 'manual_50_names.jl'
ACTUAL_FILE_ETHNICITIES = 'manual_50_ethnicities.jl'

EMBEDDINGS_FILE = 'unigram-part-00000-v2.json'
FIELD_NAMES_CITIES = {
    "text_field": "readability_text",
    "annotated_field":"annotated_cities",
    "correct_field":"correct_cities"
}
FIELD_NAMES_NAMES = {
    "text_field": "readability_text",
    "annotated_field":"annotated_names",
    "correct_field":"correct_names"
}
FIELD_NAMES_ETHNICITIES = {
    "text_field": "readability_text",
    "annotated_field":"annotated_ethnicities",
    "correct_field":"correct_ethnicities"
}

def read_embedding_file(embeddings_file):
    unigram_embeddings = dict()
    with codecs.open(embeddings_file, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line)
            for k, v in obj.items():
                unigram_embeddings[k] = v
    return unigram_embeddings

def get_texts(json_object):
    """
    Parsing logic for getting texts
    """
    texts = list()
    texts.append(json_object.get(FIELD_NAMES_CITIES['text_field']))
    return texts

def get_annotated_list(json_object):
    """
    Parsing logic for getting annotated field
    """
    return json_object.get(FIELD_NAMES_CITIES['annotated_field'])

embeddings_dict = read_embedding_file(EMBEDDINGS_FILE)

classifier = train_ranker.train_ranker(embeddings_dict, TRAINING_FILE_CITIES, FIELD_NAMES_CITIES)

with codecs.open(ACTUAL_FILE_CITIES, 'r', 'utf-8') as f:
    for line in f:
        obj = json.loads(line)
        list_of_texts = get_texts(obj)
        annotated_list = get_annotated_list(obj)
        print "Annotated tokens:",
        print annotated_list
        ranked_list = rank.rank(embeddings_dict, list_of_texts, annotated_list, classifier)
        print "Ranked List:",
        print ranked_list

