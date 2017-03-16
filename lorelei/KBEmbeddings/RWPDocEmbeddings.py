from gensim.models.doc2vec import *
from gensim.models.word2vec import *
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

def get_tags(input_file):
    tags = list()
    with codecs.open(input_file, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line)
            tags += obj['tags']
    return tags


def _serialize_microcap_tokens_as_tagged_documents(tokens_file):
    tagged_documents = list()
    with codecs.open(tokens_file, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line)
            tokens = obj['words']
            tags = obj['tags']
            tagged_documents.append(TaggedDocument(tokens, tags))
    return tagged_documents

# def generate_doc_embeddings(RWP_simple_file, output_file, wordcloud=None, serializer=_serialize_microcap_tokens_as_tagged_documents):
#     if wordcloud is not None:
#         tagged_documents = serializer(RWP_simple_file, wordcloud)
#     else:
#         tagged_documents = serializer(RWP_simple_file)
#     model = Doc2Vec(tagged_documents, size=100, window=8, min_count=5, workers=4)
#     out = codecs.open(output_file, 'w', 'utf-8')
#     keys = get_uuids(RWP_simple_file)
#     for k in keys:
#         obj = dict()
#         obj[k] = model.docvecs[k].tolist()
#         json.dump(obj, out)
#         out.write('\n')
#     out.close()


def generate_doc_embeddings(RWP_simple_file, output_file_jl, output_file_model):
    tagged_documents = _serialize_microcap_tokens_as_tagged_documents(RWP_simple_file)
    model = Doc2Vec(tagged_documents, size=20, window=8, min_count=3, workers=15)
    model.init_sims(replace=True)
    model.docvecs.init_sims(replace=True)
    # model.delete_temporary_training_data(keep_doctags_vectors=True, keep_inference=True)
    model.save(output_file_model)
    out = codecs.open(output_file_jl, 'w', 'utf-8')
    keys = get_tags(RWP_simple_file)
    for k in keys:
        obj = dict()
        obj[k] = model.docvecs[k].tolist()
        json.dump(obj, out)
        out.write('\n')
    out.close()


def generate_pos_neg_file(doc_embedding_model, id_annotation, output_file):
    annotations = json.load(codecs.open(id_annotation, 'r', 'utf-8'))
    model = Doc2Vec.load(doc_embedding_model)

    out = codecs.open(output_file, 'w', 'utf-8')
    for k, v in annotations.items():
        docvec = model.docvecs[k].tolist()
        string = k+'\t'+str(docvec)+'\t'+str(v)+'\n'
        out.write(string)
    out.close()


def doc_embedding_tester(doc_embedding_model):
    model = Doc2Vec.load(doc_embedding_model)
    docvec= model.docvecs['e97d63f757daaf1517b1c19779b9dcf3b55d8a02']
    print docvec
    print model['escort']
    print model.docvecs.most_similar(positive=['751AC0E3C5A3F41A9C6AA73A060A5CD655869355A3E563A88A68FC614A1312FE'])
    print model.similar_by_vector(docvec)


# path = '/Users/mayankkejriwal/Dropbox/lorelei/Mar_3/'
# path = '/Users/mayankkejriwal/Dropbox/dig-microcap-data/'
# path = '/Users/mayankkejriwal/Dropbox/memex-mar-17/CP1/'
# generate_pos_neg_file(path+'serialized_train_test_doc2vec_20dims',path+'train_annotations.json',path+'train_pos_neg_20dims.tsv')
# doc_embedding_tester(path+'global_lower_non_alpha_doc2vec')
# doc_embedding_tester(path+'serialized_train_test_doc2vec')
# generate_doc_embeddings(path+'samples10.jl', path+'samples10_doc2vec.jl', path+'samples10_doc2vec')
# generate_doc_embeddings(path+'serialized_train_test.jl', path+'serialized_train_test_doc2vec_20dims.jl', path+'serialized_train_test_doc2vec_20dims')