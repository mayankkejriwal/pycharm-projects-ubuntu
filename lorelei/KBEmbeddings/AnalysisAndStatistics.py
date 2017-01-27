import codecs
import re


def getPropertyStatistics(triples_file):
    """
    We will build a dictionary
    :param triples_file:
    :return:
    """
    propertyCountDict = dict()
    with codecs.open(triples_file, 'r', 'utf-8') as f:
        for line in f:
            fields = re.split('\t',line[0:-1])
            if fields[1] not in propertyCountDict:
                propertyCountDict[fields[1]] = 0
            propertyCountDict[fields[1]] += 1
    for k,v in propertyCountDict.items():
        print k+'\t'+str(v)


# path = '/Users/mayankkejriwal/datasets/lorelei/KB-CIA/'
# getPropertyStatistics(path+'KB_Scenario.nt')