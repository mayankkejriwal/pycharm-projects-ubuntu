import json, codecs


def userNameMatch(joined_file1, joined_file2, output_file):
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
    lines_set = set()
    with codecs.open(joined_file2, 'r', 'utf-8') as f:
         for line in f:
             data2.append(json.loads(line))

    for item1 in data1:
        for item2 in data2:
            string1 = (item1['userName'])
            string2 = (item2['userName'])
            if string1 == string2:
                string = item1['userName'] + '\t' + item2['userName'] + '\t' + '1.0'
                lines_set.add(string)

            #else:
             #   string = item1['userName'] + '\t' + item2['userName'] + '\t' + '0.0'
    file = codecs.open(output_file, 'w', 'utf-8')
    for string in lines_set:
        file.write(string)
        file.write('\n')
    file.close()

upper_path = '/home/mayankkejriwal/Downloads/memex-cp2/test-data/'
userNameMatch(upper_path+'evalUsersCFiltered.jl', upper_path+'evalUsersDFiltered.jl', upper_path+'userNameCD-score-file.txt')