from scipy.stats import rankdata

import classification
import text_preprocessors

def rank(embeddings_dict, list_of_texts, annotated_tokens, classifier):
    if(annotated_tokens is None or len(annotated_tokens) == 0):
        #Cannot rank None
        return None
    words_not_found_in_text = []
    for index, text in enumerate(list_of_texts):
        #There are multiple texts in the order of increasing precision 
        # text = text_preprocessors.text_to_lower(text)
        annotated_tokens = text_preprocessors.list_to_lower(annotated_tokens)
        tokenized_text = text_preprocessors.preprocess_text(text)
        # print tokenized_text
        # print annotated_tokens
        # tokenized_text = text_preprocessors.tokenize(text)
        # tokenized_text = text_preprocessors.tokens_remove_non_alpha(tokenized_text)
        feature_vectors_dict = classification.get_feature_vectors(tokenized_text, annotated_tokens, embeddings_dict)
        if(len(feature_vectors_dict['words_not_found_in_text']) > 0):
            if((index+1) != len(list_of_texts)):
                #A word was not found and there are remaining texts to be seen
                continue
            else:
                #This is the last text. Have to do with it
                words_not_found_in_text = feature_vectors_dict['words_not_found_in_text']

        predictions = classification.classify(feature_vectors_dict['words'], feature_vectors_dict['feature_vectors'], classifier)
    # print predictions
    if(predictions is None):
        print 'no predictions. Returning original list...'
        return annotated_tokens

    ranked_list = _rank_using_predictions(feature_vectors_dict['words'], predictions)
    # print ranked_list
    #Putting the words not found at the end
    ranked_list = ranked_list + words_not_found_in_text
    return ranked_list

def _rank_using_predictions(annotated_tokens, predictions):
    negative_predictions = predictions['predicted_probabilities'][:,0]
    #print "nega:",
    #print negative_predictions
    ranking = get_rankings(negative_predictions)
    return [annotated_tokens for (ranking, annotated_tokens) in sorted(zip(ranking,annotated_tokens))]

def get_rankings(data):
    return rankdata(data, method='ordinal')
