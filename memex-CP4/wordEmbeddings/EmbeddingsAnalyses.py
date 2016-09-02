import kNearestNeighbors
from random import shuffle
from scipy import stats
import numpy as np
import warnings
import pprint

class EmbeddingsAnalyses:
    """
    The main goal of this class is to compare different sets of embeddings.
    """

    @staticmethod
    def _compute_rank_corr_coeff(l1, l2):
        """
        The two lists contain objects. We must compose the rankings ourselves, then compute the rank corr. coeff.
        I do not do the usual checks, except that the lists are the same length. Results are unexpected or an
        exception may get thrown if lists don't contain the same 'set' of objects.
        :param l1: list
        :param l2: list
        :return: float
        """
        warnings.filterwarnings("ignore")
        l1_pos = dict()
        r1 = list()
        r2 = list()
        count = 0
        for item in l1:
            l1_pos[item] = count
            r1.append(count)
            count += 1
        for item in l2:
            r2.append(l1_pos[item])
        return stats.spearmanr(r1, r2)


    @staticmethod
    def measure_embeddings_correlations(embeddings_file1, embeddings_file2, sample_size=10):
        """

        :param embeddings_file1:
        :param embeddings_file2:
        :param sample_size: randomly select this many words from the embeddings, and compute their rankings.
        That way we can compute the expected rank correlation coefficient.
        :return: None
        """
        warnings.filterwarnings("ignore")
        #First step: read in embeddings files
        embeddings1 = kNearestNeighbors.read_in_embeddings(embeddings_file1)
        embeddings2 = kNearestNeighbors.read_in_embeddings(embeddings_file2)
        #Second step: prune out the uncommon words form both embeddings
        forbidden = set()
        for k, v in embeddings1.items():
            if k not in embeddings2:
                forbidden.add(k)
        for f in forbidden:
            del embeddings1[f]
        forbidden = set()
        for k, v in embeddings2.items():
            if k not in embeddings1:
                forbidden.add(k)
        for f in forbidden:
            del embeddings2[f]
        #Third step: sample
        words = embeddings1.keys()
        shuffle(words)
        words = words[0:sample_size]
        print words
        #Fourth step: build ranked lists
        correlations = dict()
        pvalues = dict()
        for word in words:
            score_dict = kNearestNeighbors._generate_scored_dict(embeddings1, word)
            l1 = kNearestNeighbors._extract_top_k(score_dict, k=0, disable_k=True)
            score_dict = kNearestNeighbors._generate_scored_dict(embeddings2, word)
            l2 = kNearestNeighbors._extract_top_k(score_dict, k=0, disable_k=True)
            results = EmbeddingsAnalyses._compute_rank_corr_coeff(l1, l2)
            correlations[word] = results[0]
            pvalues[word] = results[1]
        print 'word: corr. coeff'
        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(correlations)
        print 'word: p_value'
        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(pvalues)
        print 'Expected spearman\'s rank corr. coeff.: ',
        print np.mean(correlations.values())


# path = '/home/mayankkejriwal/Downloads/memex-cp4-october/'
# EmbeddingsAnalyses.measure_embeddings_correlations(path+'unigram-embeddings-gt.json', path+'unigram-embeddings-10000docs.json')