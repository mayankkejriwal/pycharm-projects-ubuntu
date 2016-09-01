import codecs
import json
import pprint


class FieldAnalyses:
    """
    Use static methods in this class to analyze the structure of the data. Things like, what fields
    are present in the file, and what are their data types?

    We will also use this class to develop methods for measuring field coherence, once we have our unigram embeds.
    """

    @staticmethod
    def print_fields_data_types(input_file):
        """
        We are only considering 'upper-level' fields.
        :param input_file: A json lines file
        :return: None
        """
        fields = dict()  # the key is the field name, the value will be the data type
        pp = pprint.PrettyPrinter(indent=4)
        with codecs.open(input_file, 'r', 'utf-8') as f:
            for line in f:
                obj = json.loads(line)
                for k, v in obj.items():
                    if k not in fields:
                        fields[k] = set()
                    fields[k].add(type(v))
        print pp.pprint(fields)


# path='/home/mayankkejriwal/Downloads/memex-cp4-october/'
# FieldAnalyses.print_fields_data_types(path+'ground-truth-corpus.json')