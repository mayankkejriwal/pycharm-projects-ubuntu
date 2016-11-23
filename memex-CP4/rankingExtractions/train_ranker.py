import codecs
import json

import classification
import numpy as np
import text_preprocessors


def train_ranker(embeddings_dict, training_data_file, field_names):
    training_data = _extract_from_training_file(training_data_file, field_names)
    #print training_data
    feature_vector_dict = _get_feature_vectors(training_data, embeddings_dict)
    classifier = classification.train_classifier(feature_vector_dict['feature_vectors'], feature_vector_dict['labels'])
    #print feature_vector_dict
    return classifier

def _extract_from_training_file(training_data_file, field_names):
    training_data = []
    with codecs.open(training_data_file, 'r', 'utf-8') as f:
        for line in f:
            data_from_json = dict()
            json_object = json.loads(line)
            data_from_json['text_field'] = json_object.get(field_names['text_field'])
            data_from_json['correct_field'] = json_object.get(field_names['correct_field'])
            data_from_json['annotated_field'] = json_object.get(field_names['annotated_field'])
            training_data.append(data_from_json)
    return training_data

def _get_feature_vectors(training_data, embeddings_dict):
    result = dict()
    words = []
    feature_vectors = None
    labels = []
    count = 0
    for data in training_data:
        # print count
        count += 1
        text = data['text_field']
        correct_tokens = data['correct_field']
        annotated_tokens = data['annotated_field']
        if((len(correct_tokens) == 0) or len(annotated_tokens) == 0):
            #Cannot train on empty
            continue;
        text = text.lower()
        correct_tokens = text_preprocessors.list_to_lower(correct_tokens)
        annotated_tokens = text_preprocessors.list_to_lower(annotated_tokens)
        tokenized_text = text_preprocessors.preprocess_text(text)
        # tokenized_text = text_preprocessors.tokenize(text)
        # tokenized_text = text_preprocessors.tokens_remove_non_alpha(tokenized_text)
        feature_vectors_dict = classification.get_feature_vectors(tokenized_text, annotated_tokens, embeddings_dict, correct_tokens)
        
        words = words + feature_vectors_dict['words']
        #print feature_vectors_dict['feature_vectors']
        if(feature_vectors is None):
            feature_vectors = feature_vectors_dict['feature_vectors']
        elif(feature_vectors_dict['feature_vectors'] is None):
            #Nothing returned from feature vectors
            continue
        else:
            feature_vectors = np.vstack((feature_vectors, feature_vectors_dict['feature_vectors']))

        labels = labels+feature_vectors_dict['labels']

    #feature_vectors = normalize(feature_vectors)
    result['words'] = words
    result['feature_vectors'] = feature_vectors
    result['labels'] = labels
    #print result
    return result