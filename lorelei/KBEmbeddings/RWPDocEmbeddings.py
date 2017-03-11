from gensim.models.doc2vec import *
from gensim.utils import *
import json
import codecs

def _serialize_RWP_simple_as_tagged_documents(RWP_simple_file, wordcloud=True):
    """

    :param RWP_simple_file:
    :param wordcloud: if false, we'll use the entities field otherwise we'll use the wordcloud field
    :return: A list of tagged document elements.
    """
    docs = json.load(codecs.open(RWP_simple_file,'r'))
    tagged_docs = list()
    print 'finished reading in raw docs...'
    # count = 1
    for k,v in docs.items():
        # print 'processing document...',str(count)
        # count += 1
        tags = list()
        tags.append(k)
        # tokens = None
        if wordcloud:
            tokens = v['wordcloud']
        else:
            tokens = v['entities']
        tagged_docs.append(TaggedDocument(tokens, tags))
    return tagged_docs

def get_uuids(RWP_simple_file):
    docs = json.load(codecs.open(RWP_simple_file, 'r'))
    return docs.keys()

def generate_doc_embeddings(RWP_simple_file, output_file, wordcloud=True):
    tagged_documents = _serialize_RWP_simple_as_tagged_documents(RWP_simple_file, wordcloud)
    model = Doc2Vec(tagged_documents, size=100, window=8, min_count=5, workers=4)
    out = codecs.open(output_file, 'w', 'utf-8')
    keys = get_uuids(RWP_simple_file)
    for k in keys:
        obj = dict()
        obj[k] = model.docvecs[k].tolist()
        json.dump(obj, out)
        out.write('\n')
    out.close()


# path = '/Users/mayankkejriwal/Dropbox/lorelei/Mar_3/'
# generate_doc_embeddings(path+'RWP_simple.json', path+'RWP_entities_doc_vec.json', False)