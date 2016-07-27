import nltk

class Tokenizer:
    @staticmethod
    def nltk_tokenize(text):
        """

        Returns:
            A list of tokens

        Credit:
            Taken from brandonrose.com/clustering
        """
        # first tokenize by sentence, then by word to ensure that punctuation is caught as it's own token

        tokens = [word.lower() for sent in nltk.sent_tokenize(text) for word in nltk.word_tokenize(sent)]

        return tokens

#nltk.download()
#Tokenizer.nltk_tokenize('a,b c')
