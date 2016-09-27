import TextPreprocessors

class ContextVectorGenerators:
    """
    Each non-protected method in this class has the same signature and is static. Typically, these functions will
    be passed in as parameters to some function in TokenSupervised, in order for datasets to be prepared.
    """

    @staticmethod
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

    @staticmethod
    def symmetric_generator(word, list_of_words, embeddings_dict, window_size=2, multi=False):
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
            word_tokens = TextPreprocessors.TextPreprocessors.tokenize_string(word)
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
                if multi:   # we do not want the vector of the word/work_tokens itself
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
                    ContextVectorGenerators._add_vectors(new_context_vec, vec)
            if not new_context_vec:
                continue
            else:
                context_vecs.append(new_context_vec)
        if not context_vecs:
            return None
        else:
            return context_vecs

    @staticmethod
    def tokenize_add_all_generator(word, list_of_words, embeddings_dict):
        """
        We will not preprocess the string, only tokenize it
        :param word: this parameter will not be used
        :param list_of_words: a list of words.
        :param embeddings_dict: The embeddings dictionary
        :return: A context vector
        """
        context_vec = list()
        for w in list_of_words:
            if w not in embeddings_dict:
                continue
            if not context_vec:
                context_vec = list(embeddings_dict[w])
            else:
                ContextVectorGenerators._add_vectors(context_vec, embeddings_dict[w])
        if context_vec:
            return context_vec
