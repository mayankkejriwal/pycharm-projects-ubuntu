import codecs
import json
import re


class TextAnalyses:
    """
    Use this once the text tokens have been extracted; e.g. on the readability_tokens.json file. The most
    important thing we need to do is document frequency generation
    """

    @staticmethod
    def read_in_and_prune_idf(df_file, lower_prune_ratio=0.0005, upper_prune_ratio=0.5):
        """

        :param idf_file: e.g. readability_tokens_df.txt
        :param lower_prune_ratio: any token with ratio strictly below this gets discarded
        :param upper_prune_ratio: any token with ratio strictly above this gets discarded
        :return: A dictionary of tokens with their idfs
        """
        idf = dict()
        with codecs.open(df_file, 'r', 'utf-8') as f:
            for line in f:
                fields = re.split('\t',line[:-1])
                if len(fields) != 3:
                    print 'error in splitting df file'
                elif float(fields[2])>lower_prune_ratio and float(fields[2])<upper_prune_ratio:
                        idf[fields[0]] = fields[2]
        print 'total number of words in idf dict : ',
        print len(idf)
        return idf

    @staticmethod
    def generate_document_frequencies(tokens_list_file, output_file, inner_field = None):
        """
        In the output_file, each line contains three tab delimited fields: the first field is the token,
        the second field is the count of documents in which that token occurred, the third field
        is a percentage.

        IMP.: We increase df count of every token by one, and increase the total number of documents by one.
        The idea is that there is a 'mega' document that contains all possible tokens. This avoids
        uncomfortable issues like dividing by zero.

        :param tokens_list_file: Each line in the file is a json, with an identifier referring to a list of tokens
        :param output_file:
        :param inner_field: if None, then each read-in dictionary is a single key-value pair, with the value
        being the list of tokens. Otherwise, the 'value' itself is a dictionary and we reference the inner
        field of that dictionary. This distinction is particularly useful for phone embeddings e.g.
        :return: None
        """
        df = dict()  # the key is the token, the value is the count
        total = 0
        with codecs.open(tokens_list_file, 'r', 'utf-8') as f:
            for line in f:
                total += 1
                obj = json.loads(line)
                if not inner_field:
                    for k, v in obj.items():
                        forbidden = set()
                        for token in v:
                            if token in forbidden:
                                continue
                            if token not in df:
                                df[token] = 1
                                forbidden.add(token)
                            else:
                                df[token] += 1
                                forbidden.add(token)
                elif inner_field:
                    forbidden = set()
                    v =obj.values()[0][inner_field]
                    for token in v:
                        if token in forbidden:
                            continue
                        if token not in df:
                            df[token] = 1
                            forbidden.add(token)
                        else:
                            df[token] += 1
                            forbidden.add(token)

        out = codecs.open(output_file, 'w', 'utf-8')
        print total
        for k,v in df.items():
            string = k+'\t'+str(v)+'\t'+str((v+1)*1.0/(total+1))+'\n'
            out.write(string)
        out.close()

# RWP_path = '/Users/mayankkejriwal/ubuntu-vm-stuff/home/mayankkejriwal/Downloads/lorelei/reliefWebProcessed-prepped/tokens/'
# path='/Users/mayankkejriwal/ubuntu-vm-stuff/home/mayankkejriwal/tmp/'
# TextAnalyses.generate_document_frequencies(RWP_path+'condensed-objects-lowerCase.json',
#                                            RWP_path+'condensed-objects-idf.txt')
# path+'all_tokens-part-00000-onlyLower-1-df.txt', inner_field='tokens_list')

