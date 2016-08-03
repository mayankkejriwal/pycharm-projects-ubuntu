
import json, codecs

def print_usernames(input_file, output_file):
    """
    Prints out one username per line
    :param input_file:
    :return:
    """
    usernames_set = set()
    with codecs.open(input_file, 'r', 'utf-8') as f:
         for line in f:
             usernames_set.add(json.loads(line)['userName'])
    file = codecs.open(output_file, 'w', 'utf-8')
    for string in usernames_set:
        file.write(string)
        file.write('\n')
    file.close()

upper_path = '/home/mayankkejriwal/Downloads/memex-cp2/test-data/'
print_usernames(upper_path+'evalUsersDFiltered.jl', upper_path+'usernamesD.txt')