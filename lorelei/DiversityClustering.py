import codecs
import json
from sklearn.feature_extraction import FeatureHasher
import FeatureFunctions
from sklearn.cluster import AgglomerativeClustering
import DiversityClusteringAnalyses

class DiversityClustering:
    """
    This class is designed to include static methods that will help us take a roughly similar set of frames
    as input and help sub-cluster them so that we have a more diverse view of the frames. Ideally, this
    code should only be run after an initial retrieval phase i.e. do not try running it on an undifferentiated
    dump
    """

    @staticmethod
    def agglomerative_diversity_on_condensed(jlines_file, output_folder, featurize='01-text-hash', num_clusters=5):
        """
        agglomerative clustering
        :param jlines_file: the jlines condensed file.
        :param featurize: the way to convert frames into feature vectors. 01-text means that we have a feature for every
        word in the loreleiJSONMapping.wordcloud attribute in the corpus, and simply give it 0 or 1 based on whether
        the word occurs in the wordcloud.
        :param num_clusters:
        :return:
        """
        data = list()
        agg = AgglomerativeClustering(n_clusters=num_clusters, affinity='cosine', linkage='complete')
        with codecs.open(jlines_file, 'r', 'utf-8') as f:
            for line in f:
                obj = json.loads(line)
                if 'text' in featurize:
                    wordcloud = obj['loreleiJSONMapping.wordcloud']
                if featurize == '01-text-hash':
                    word_dict = FeatureFunctions.convert_wordcloud_to_term_dict(wordcloud)
                    data.append(word_dict)
        feature_vectors = FeatureHasher(n_features=30).transform(data).toarray()
        agg.fit(feature_vectors)
        labels_dict = DiversityClusteringAnalyses.print_cluster_label_counts(agg)
        label_outs = dict()
        for label in set(labels_dict.keys()):
            label_outs[label] = codecs.open(output_folder+str(label)+'.json', 'w', 'utf-8')
        with codecs.open(jlines_file, 'r', 'utf-8') as f:
            count = 0
            for line in f:
                obj = json.loads(line)
                out = label_outs[agg.labels_[count]]
                json.dump(obj, out)
                out.write('\n')
                count += 1
        for value in label_outs.values():
            value.close()


# path = '/Users/mayankkejriwal/ubuntu-vm-stuff/home/mayankkejriwal/tmp/'
# DiversityClustering.agglomerative_diversity_on_condensed(path+'myanmar-italyEarthQuakeProcessed-top500.json', path+'myanmar-agg/')
# featurize='01-text-hash'
# print 'text' in featurize