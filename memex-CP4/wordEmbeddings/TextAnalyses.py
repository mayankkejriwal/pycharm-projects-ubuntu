import codecs
import json


class TextAnalyses:
    """
    Use this once the text tokens have been extracted; e.g. on the readability_tokens.json file. The most
    important thing we need to do is document frequency generation
    """

    @staticmethod
    def generate_document_frequencies(tokens_list_file, output_file):
        """
        In the output_file, each line contains two tab delimited fields: the first field is the token,
        the second field is the count of documents in which that token occurred
        :param tokens_list_file: Each line in the file is a json, with an identifier referring to a list of tokens
        :return: None
        """
        df = dict()  # the key is the token, the value is the count
        with codecs.open(tokens_list_file, 'r', 'utf-8') as f:
            for line in f:
                tokens_obj = dict()
                obj = json.loads(line)