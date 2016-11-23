import numpy as np
from sklearn.ensemble import RandomForestClassifier

import text_preprocessors

def classify(words, feature_vectors, model):
    if(model is None):
        return None

    if(feature_vectors is None or model is None):
        return None
    
    if(feature_vectors.ndim == 1):
        feature_vectors = feature_vectors.reshape(1, -1)

    predicted_labels = model.predict(feature_vectors)
    predicted_probabilities = model.predict_proba(feature_vectors)

    return {'predicted_probabilities':predicted_probabilities, 'predicted_labels':predicted_labels}




def train_classifier(feature_vectors, labels, classifier_model = 'random_forest'):
    model = None
    if classifier_model == 'random_forest':
        model = RandomForestClassifier()
        model.fit(feature_vectors, labels)
    return model

def get_feature_vectors(tokenized_text, annotated_tokens, embeddings_dict, correct_tokens = None, process_duplicates = False):
    words_covered = set()
    result = dict()
    words_result = []
    feature_vectors_result = None
    label_result = []
    words_not_found_in_text = []

    for word in annotated_tokens:
        
        if(not process_duplicates):
            #Do not process duplicates in annotated_tokens
            if(word in words_covered):
                continue
            else:
                words_covered.add(word)

        word_tokens = text_preprocessors.tokenize_string(word)
        
        if len(word_tokens) <= 1: 
            # we're dealing with a single word
            if word not in tokenized_text:
                words_not_found_in_text.append(word)
                # print 'skipping word not found in text field: ',
                # print word
                continue
            context_vecs = context_generator(word, tokenized_text, embeddings_dict)
        elif is_sublist_in_big_list(tokenized_text, word_tokens):
            # this is multi word
            context_vecs = context_generator(word, tokenized_text, embeddings_dict, multi=True)
        else:
            words_not_found_in_text.append(word)
            # print 'skipping word not found in text field: ',
            # print word
            continue


        if not context_vecs:
            # print 'context_generator did not return anything for word: ',
            # print word
            continue

        #Handling Multiple Occurences of the same word in the tokenized text
        count = 0
        context_vecs_for_word = None
        for context_vec in context_vecs:
            count+=1
        #    print context_vec
            if (context_vecs_for_word is None):
                context_vecs_for_word = np.array(context_vec)
            else:
                context_vecs_for_word = np.vstack((context_vecs_for_word, context_vec))

        #print context_vecs_for_word
        if(count > 1):
            #Adding and taking average of the vecs for all occurences of the word
            combined_context_vec = context_vecs_for_word.sum(axis=0)
            combined_context_vec = combined_context_vec/count
        else:
            combined_context_vec = context_vecs_for_word

        words_result.append(word)
        
        if(feature_vectors_result is None):
            feature_vectors_result = combined_context_vec
        else:
            feature_vectors_result = np.vstack((feature_vectors_result, combined_context_vec))

        if(correct_tokens is not None):
            if word in correct_tokens:
                label_result.append(1)
            else:
                label_result.append(0)

    result['words'] = words_result
    result['feature_vectors'] = feature_vectors_result
    result['words_not_found_in_text'] = words_not_found_in_text
    if(correct_tokens is not None):
        result['labels'] = label_result
    return result


def context_generator(word, list_of_words, embeddings_dict, window_size=2, multi=False, consider_word_itself = False):
    """
    The algorithm will search for occurrences of word in list_of_words (there could be multiple or even 0), then
    symmetrically look backward and forward up to the window_size. If the words in the window (not including
    the word itself) are in the embeddings_dict, we will add them up, and that constitutes a context_vec.
    If the word is not there in embeddings_dict, we do not include it. Note that if no words in the embeddings_dict
    then we will act as if the word itself had never occurred in the list_of_words.
    :param word:
    :param list_of_words: e.g. high_recall_readability_text
    :param embeddings_dict:
    :param window_size
    :param multi: If True, then word is multi-token. You must tokenize it first, then generate context embedd.
    :return: a list of lists, with each inner list representing the context vectors. If there are no occurrences
    of word, will return None. Check for this in your code.
    """
    if not list_of_words:
        return None
    context_vecs = list()
    if multi:
        word_tokens = text_preprocessors.tokenize_string(word)
    for i in range(0, len(list_of_words)):
        if multi:
            if list_of_words[i] == word_tokens[0] and list_of_words[i:i + len(word_tokens)] == word_tokens:
                min_index = i-window_size
                max_index = ((i + len(word_tokens))-1)+window_size
            else:
                continue
        elif list_of_words[i] != word:
            continue
        else:
            min_index = i-window_size
            max_index = i+window_size

        # make sure the indices are within range
        if min_index < 0:
            min_index = 0
        if max_index >= len(list_of_words):
            max_index = len(list_of_words)-1

        new_context_vec = []
        for j in range(min_index, max_index+1):
            if(not consider_word_itself):
                # we do not want the vector of the word/work_tokens itself
                if multi:   
                    if j >= i and j < i+len(word_tokens):
                        continue
                elif j == i:
                    continue

            if list_of_words[j] not in embeddings_dict: # is the word even in our embeddings?
                continue

            vec = list(embeddings_dict[list_of_words[j]])  # deep copy of list
            if not new_context_vec:
                new_context_vec = vec
            else:
                _add_vectors(new_context_vec, vec)
        if not new_context_vec:
            continue
        else:
            context_vecs.append(new_context_vec)
    if not context_vecs:
        return None
    else:
        return context_vecs

def is_sublist_in_big_list(big_list, sublist):
    # matches = []
    for i in range(len(big_list)):
        if big_list[i] == sublist[0] and big_list[i:i + len(sublist)] == sublist:
            return True
    return False

def _add_vectors(big_vector, little_vector):
    """
    little_vector gets added into big_vector. Vectors must be same length. big_vector may get modified.
    :param big_vector:
    :param little_vector:
    :return: None
    """
    if len(little_vector) != len(big_vector):
        print 'Error! Vector lengths are different!'
        return
    else:
        for i in range(0, len(little_vector)):
            big_vector[i] += little_vector[i]