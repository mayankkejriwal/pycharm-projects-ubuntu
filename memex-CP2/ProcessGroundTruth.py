import json
import codecs
import re

def process_ground_truth(input_file, output_file):
    """
    Note: this only works because I've verified the input_file only contains 'class 1' jsons (i.e.
    it does not contain non-duplicates).
    :param input_file: The 16_9_matches_j.txt
    :param output_file: A file with two tab-delimited fields (userName A and userName B resp.)
    :return: None
    """
    file = codecs.open(output_file, 'w', 'utf-8')
    with codecs.open(input_file, 'r', 'utf-8') as f:
        for line in f:
            d = json.loads(line)
            string = d['site_a'] + '\t' + d['site_b']
            file.write(string)
            file.write('\n')
    file.close()

def check_ground_truth(pairs_file, joinedA_file, joinedB_file):
    """
    Will print out the userNames that are there in the ground-truth but not in the joined files
    :param pairs_file: The pairs file output by process_ground_truth
    :param joinedA_file: joinedA file
    :param joinedB_file: joined B file
    :return: None
    """

    userNamesA = _collect_userNames(joinedA_file)
    userNamesB = _collect_userNames(joinedB_file)
    with codecs.open(pairs_file, 'r', 'utf-8') as f:
        for line in f:
            tokens = re.split('\t|\n', line)

            if not tokens[0] in userNamesA:
                print 'username not in userNames A file : ',tokens[0]
            if not tokens[1] in userNamesB:
                print 'username not in userNames B file : ',tokens[1]



def _collect_userNames(joined_file):
    """

    :param joined_file: Either joinedA or joinedB
    :return: Returns the set of userNames in the file
    """
    userNames = set()
    with codecs.open(joined_file, 'r', 'utf-8') as f:
        for line in f:
            userNames.add(json.loads(line)['userName'])
    return userNames

upper_path = '/home/mayankkejriwal/Downloads/memex-cp2/'
#process_ground_truth(upper_path+'16_9_matches_j-latinized.txt', upper_path+'pairs-ground-truth.txt')
check_ground_truth(upper_path+'pairs-ground-truth.txt', upper_path+'joinedA.jl', upper_path+'joinedB.jl')