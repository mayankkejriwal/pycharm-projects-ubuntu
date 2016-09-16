

class WordCloudPreprocessor:
    """
    Based on some of the things I've noted in ~/Downloads/lorelei/RWP_thingsToTry.txt, this file
    will contain static methods for preprocessing the word-clouds in the condensed objects.
    """

    @staticmethod
    def convert_list_to_lower(wordcloud):
        """
        Modifies wordcloud
        :param wordcloud:
        :return:
        """
        for i in range(0, len(wordcloud)):
            wordcloud[i] = wordcloud[i].lower()