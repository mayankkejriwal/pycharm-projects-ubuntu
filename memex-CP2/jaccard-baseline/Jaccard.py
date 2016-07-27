import json, codecs


def Jaccard(joined_file1, joined_file2, output_file):
    """

    :param joined_file1: THe joined file A output in Flattener.py
    :param joined_file2: The joined file B output in Flattener.py
    :param output_file: The all-pairs scored file. Each line has three tab-delimited fields
    :return: None
    """
    data1 = []
    with codecs.open(joined_file1, 'r', 'utf-8') as f:
         for line in f:
             data1.append(json.loads(line))

    data2 = []
    with codecs.open(joined_file2, 'r', 'utf-8') as f:
         for line in f:
             data2.append(json.loads(line))
    file = codecs.open(output_file, 'w', 'utf-8')
    for item1 in data1:
        for item2 in data2:
            string = item1['userName'] + '\t' + item2['userName'] + '\t'+\
                  str( _compute_jaccard_tokensSets(item1['tokens_set'],item2['tokens_set']))
            file.write(string)
            file.write('\n')

    file.close()


def _compute_jaccard_tokensSets(tokens_set1, tokens_set2):
    """

    :param tokens_set1: A list of tokens
    :param tokens_set2: A list of tokens
    :return: Jaccard similarity score
    """
    s1 = set(tokens_set1)
    s2 = set(tokens_set2)
    return float(len(s1.intersection(s2)))/float(len(s1.union(s2)))

#upper_path = '/home/mayankkejriwal/Downloads/memex-cp2/'
#Jaccard(upper_path+'joinedA.jl', upper_path+'joinedB.jl', upper_path+'jaccard-score-file.txt')