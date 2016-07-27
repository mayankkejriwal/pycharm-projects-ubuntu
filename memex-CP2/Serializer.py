import Tokenizer
import json
import six, codecs

class Serializer:
    """
    Each method in this class takes a json lines file as input, and (optionally)
    a list of fields. The goal is to serialize each file so that it's ready for flattening/joining
    and finally, LSH
    """

    @staticmethod
    def extract_tokens(input_file, output_file, key, fields = None):
        """

        :param input_file: The jl file which must be serialized
        :param output_file: The serialized file
        :param key: A field which is meant to serve as key
        :param fields: A list of fields. If None, we assume all fields. Note also that
        we are assuming a flat JSON structure
        :return: None
        """
        file = codecs.open(output_file, 'w', 'utf-8')

        with codecs.open(input_file, 'r', 'utf-8') as f:
         for line in f:
             d = json.loads(line)
             #print(d)
             output_dict = {}
             output_dict[key] = d[key]
             #print(output_dict[key])
             tokens_set = []
             if fields:
                 for field in fields:
                     if field in d and d[field]:
                        if isinstance(d[field], six.string_types):
                            tokens_set.append(Tokenizer.Tokenizer.nltk_tokenize(d[field]))
                        else:
                            tokens_set.append(Tokenizer.Tokenizer.nltk_tokenize(str(d[field])))
             else:
                 for field in d.keys():
                     if d[field]:
                        if isinstance(d[field], six.string_types):
                            tokens_set.append(Tokenizer.Tokenizer.nltk_tokenize(d[field]))
                        else:
                            tokens_set.append(Tokenizer.Tokenizer.nltk_tokenize(str(d[field])))
             output_dict['tokens_set'] = Serializer._flatten(tokens_set)
             json.dump(output_dict, file)
             file.write('\n')

         file.close()

    @staticmethod
    def _flatten(l):
        """
        Unwraps one 'layer' of listing. Will print an error message and return None if input is not a list.

        Attributes:
            l: A list of lists/'elements'.

        Returns:
                A flattened list of elements

        """
        answer = []
        if not isinstance(l, list):
            print('Error! Input is not a list!')
            return None
        if not l:
            return l
        else:
            for element in l:
                if isinstance(element, list):
                    for el in element:
                        answer.append(el)
                else:
                     answer.append(element)

        return answer

#upper_path = '/home/mayankkejriwal/Downloads/memex-cp2/'
#Serializer.extract_tokens(upper_path+'postsAFiltered-latinized.jl', upper_path+'postsASerialized.jl', 'userName')