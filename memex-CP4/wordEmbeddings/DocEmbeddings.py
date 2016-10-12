import kNearestNeighbors
import TextAnalyses
import codecs
import json
import VectorUtils
import numpy as np

def idf_weighted_embedding(embeddings_file, tokens_file, idf_file, output_file):
    """
    Doc embeddings are computed by doing a weighted idf sum of tokens that exist in the embeddings file, following
    which we do an l2 normalization
    :param embeddings_file:
    :param tokens_file:
    :param idf_file:
    :param output_file:
    :return: None
    """
    embeddings_dict = kNearestNeighbors.read_in_embeddings(embeddings_file)
    idf_dict = TextAnalyses.TextAnalyses.read_in_and_prune_idf(idf_file, lower_prune_ratio=0.0, upper_prune_ratio=1.0)
    out = codecs.open(output_file, 'w', 'utf-8')

    count = 0
    with codecs.open(tokens_file, 'r', 'utf-8') as f:
        for line in f:
            count += 1

            tmp = dict()
            # weights = list()
            # vectors = list()
            total_weights = 0.0
            vector = list()
            obj = json.loads(line)
            for token in obj.values()[0]:
                if token not in embeddings_dict:
                    continue
                elif token in embeddings_dict and token not in idf_dict:
                    continue
                else:
                    # print idf_dict[token]
                    weight = float(idf_dict[token])
                    total_weights += weight
                    vector1 = [element * weight for element in list(embeddings_dict[token])]
                    if not vector:
                        vector = vector1
                    else:
                        vector = list(np.sum([vector, vector1], axis=0))
            if total_weights == 0.0:
                print 'no doc embedding. Skipping document...'
                continue
            tmp[obj.keys()[0]] = list(VectorUtils.normalize_vector(vector))
            print count
            json.dump(tmp, out)
            out.write('\n')
    out.close()

# data_path = '/Users/mayankkejriwal/datasets/nyu_data/'
# idf_weighted_embedding(data_path+'unigram-combined-pruned.json', data_path+'tokens_combined_ht_onlyLower.json',
#                        data_path+'combined_ht_idf.txt', data_path+'idf_weighted_combined_doc_embedding.json')
