def convert_wordcloud_to_term_dict(wordcloud, binary=True):
    """
    A dictionary of word features. Values will be either term counts in the wordcloud, or binary (i.e. 1).
    :param wordcloud: a list of words
    :param binary: if true, will only record 1 for the word that occurs, Otherwise, records term frequency
    :return: dict
    """
    result = dict()
    if binary:
        words = set(wordcloud)
        for word in words:
            result[word] = 1
    else:
        for word in wordcloud:
            if word not in result:
                result[word] = 0
            result[word] += 1
    return result