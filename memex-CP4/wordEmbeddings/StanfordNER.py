from nltk.tag import StanfordNERTagger
from nltk.tokenize import word_tokenize
import codecs
from random import shuffle
import re
import glob
import json
import TextPreprocessors

def trial1():
    """
    Just to make sure we're not screwing everything up.
    :return:
    """
    st = StanfordNERTagger('/Users/mayankkejriwal/ubuntu-vm-stuff/home/mayankkejriwal/tmp/stanford-ner-2015-12-09/annotated-cities-model.ser.gz',
                           '/Users/mayankkejriwal/ubuntu-vm-stuff/home/mayankkejriwal/tmp/stanford-ner-2015-12-09/stanford-ner.jar',
                           encoding='utf-8')

    text = 'While in France, Mrs. Christine Lagarde discussed short-term stimulus efforts in a recent interview with the Wall Street Journal.'

    tokenized_text = word_tokenize(text)
    classified_text = st.tag(tokenized_text)

    print(classified_text)

def _build_locations_true_positives_set(json_obj, list_of_correct_fields):
    answer = set()
    for field in list_of_correct_fields:
        if field in json_obj and json_obj[field]:
            for f in json_obj[field]:
                answer.add(f.lower())
    return answer

def trial2():
    """
    Let's try using the nltk and one of the readability texts
    :return:
    """
    pretrained_model_path = '/Users/mayankkejriwal/ubuntu-vm-stuff/home/mayankkejriwal/tmp/www-experiments/stanford-ner-2015-12-09/'
    all3class = pretrained_model_path+'classifiers/english.all.3class.distsim.crf.ser.gz'
    conll4class = pretrained_model_path+'classifiers/english.conll.4class.distsim.crf.ser.gz'
    muc7class = pretrained_model_path+'classifiers/english.muc.7class.distsim.crf.ser.gz'
    st_muc = StanfordNERTagger(muc7class,
                           pretrained_model_path+'stanford-ner.jar',
                           encoding='utf-8')
    st_conll = StanfordNERTagger(conll4class,
                           pretrained_model_path+'stanford-ner.jar',
                           encoding='utf-8')
    st_3class = StanfordNERTagger(all3class,
                                 pretrained_model_path + 'stanford-ner.jar',
                                 encoding='utf-8')
    annotated_cities_file = '/Users/mayankkejriwal/datasets/memex-evaluation-november/annotated-cities/ann_city_title_state_1_50.txt'
    TP = 0
    FP = 0
    FN = 0
    with codecs.open(annotated_cities_file, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line)
            text = obj['high_recall_readability_text']
            tokenized_text = word_tokenize(text)
            classified_text_muc = st_muc.tag(tokenized_text)
            classified_text_conll = st_conll.tag(tokenized_text)
            classified_text_3class = st_3class.tag(tokenized_text)
            tagged_locations = set()

            correct_locations = _build_locations_true_positives_set(obj, ['correct_cities','correct_states','correct_cities_title'])
            # if 'correct_country' in obj and obj['correct_country']:
            #     correct_locations = correct_locations.union(set(TextPreprocessors.TextPreprocessors._preprocess_tokens
            #                                                     (obj['correct_country'].split(),['lower'])))
            for i in range(0, len(classified_text_muc)):
                tag_muc = classified_text_muc[i]
                tag_conll = classified_text_conll[i]
                tag_3class = classified_text_3class[i]
                if str(tag_3class[1]) == 'LOCATION':
                # if str(tag_muc[1]) == 'LOCATION' or str(tag_conll[1]) == 'LOCATION' or str(tag_3class[1]) == 'LOCATION':
                    tagged_locations.add(tag_3class[0].lower())
            # print tagged_locations
            # print correct_locations
            TP += len(tagged_locations.intersection(correct_locations))
            FP += (len(tagged_locations)-len(tagged_locations.intersection(correct_locations)))
            FN += (len(correct_locations)-len(tagged_locations.intersection(correct_locations)))
            # print classified_text[0][1]
            # print(classified_text)
            # break
    print 'TP, FP, FN are...'
    print TP
    print FP
    print FN
    # text = 'While in France, Mrs. Christine Lagarde discussed short-term stimulus efforts in a recent interview with the Wall Street Journal.'


def generate_random_samples(tsv_file, output_path_prefix, partition_percent=30, num_samples=10):
    """

    :param tsv_file: The tsv files in StanfordNER-full-files
    :param output_path_prefix: Will dump prefix/partition_percent_{integer}.txt and prefix/(100-partition_percent)_{integer}.txt
    PArtition 'pairs' will be complementary so we can use for training testing. Integer starts from 1.
    :param partition_percent e.g. 30
    :param num_samples The number of samples to generate
    :return: None
    """
    big_list = list()
    with codecs.open(tsv_file, 'r', 'utf-8') as f:
        small_list = list()
        break_flag = False
        for line in f:
            if break_flag:
                if len(line.strip()) == 0:  # while we're encountering blank lines, continue
                    continue
                else:
                    break_flag = False
            else:
                if len(line.strip()) == 0:
                    break_flag = True
                    big_list.append(small_list)
                    del small_list
                    small_list = list()
                    continue
                else:
                    small_list.append(line)
    for i in range(1, num_samples+1):
        shuffle(big_list)
        partition_index = int(len(big_list)*(partition_percent*1.0/100.0))
        out = codecs.open(output_path_prefix+str(partition_percent)+'_'+str(i)+'.txt', 'w', 'utf-8')
        for j in range(0, partition_index):
            for element in big_list[j]:
                out.write(element)
            out.write('\n')
        out.close()
        out = codecs.open(output_path_prefix + str(100-partition_percent) + '_' + str(i) + '.txt', 'w', 'utf-8')
        for j in range(partition_index, len(big_list)):
            for element in big_list[j]:
                out.write(element)
            out.write('\n')
        out.close()

def generate_prop_file_headers(output_folder):
    for i in ['30', '70']:
        for j in range(1, 11):
            ind = str(j)
            out = codecs.open(output_folder+i+'_'+ind+'.header', 'w', 'utf-8')
            out.write('trainFile = 30-70/'+i+'_'+ind+'.txt\n')
            out.write('serializeTo = models/' + i + '_' + ind + '.ser.gz\n')
            out.close()

def generate_prop_file_cat_script(output_file):
    out = codecs.open(output_file, 'w', 'utf-8')
    for i in ['30', '70']:
        for j in range(1, 11):
            file_name = 'propfile-headers/'+i+'_'+str(j)+'.header'
            out.write('cat '+file_name+' propfile-template.prop > propfiles/propfile_'+i+'_'+str(j)+'.prop\n')
    out.close()

def generate_training_script(output_file):
    out = codecs.open(output_file, 'w', 'utf-8')
    command_root = 'java -cp stanford-ner.jar:lib/* edu.stanford.nlp.ie.crf.CRFClassifier -prop '
    for i in ['30', '70']:
        for j in range(1, 11):
            file_name = 'propfile-headers/'+i+'_'+str(j)+'.header'
            out.write(command_root+'propfiles/propfile_'+i+'_'+str(j)+'.prop\n')
    out.close()

def generate_testing_script(root_folder):
    categories = ['ages-', 'names-', 'text-cities-', 'text-states-', 'title-cities-']
    out = codecs.open(root_folder+'testing_script.sh', 'w', 'utf-8')
    # command = 'java -cp stanford-ner.jar:lib/* edu.stanford.nlp.ie.crf.CRFClassifier -loadClassifier ner-model.ser.gz -testFile jane-austen-emma-ch2.tsv'
    command_root1 = 'java -cp stanford-ner.jar:lib/* edu.stanford.nlp.ie.crf.CRFClassifier -loadClassifier '
    command_root2 = ' -testFile '

    for category in categories:
        for j in range(1, 11):
            modelfile30 = root_folder+category+'models/30_'+str(j)+'.ser.gz'
            modelfile70 = root_folder + category + 'models/70_' + str(j) + '.ser.gz'
            testfile30 = root_folder + category + '30-70/30_' + str(j) + '.txt'
            testfile70 = root_folder + category + '30-70/70_' + str(j) + '.txt'
            outputfile30 = root_folder+category+'test/test_30_'+ str(j) + '.txt'
            outputfile70 = root_folder + category + 'test/test_70_'+ str(j) + '.txt'
            command = command_root1+modelfile30+command_root2+testfile70+' > '+outputfile70+'\n'
            out.write(command)
            command = command_root1 + modelfile70 + command_root2 + testfile30+' > '+outputfile30+'\n'
            out.write(command)
    out.close()

def print_annotation_statistics(annotated_file):
    neg = 0
    pos = 0
    with codecs.open(annotated_file, 'r', 'utf-8') as f:
        for line in f:
            if len(line.strip()) == 0:
                continue
            else:
                fields = re.split('\t', line)
                if fields[-1] == '0\n':
                    neg += 1
                elif fields[-1] == 'MISC.\n':
                    pos += 1
                else:
                    print fields
                    print 'error! Neither pos. nor neg...'
                    continue
    print 'pos annotations: ',
    print pos
    print 'neg annotations: ',
    print neg

def generate_retrained_results_csv(input_folder, output_csv):
    """

    :param input_folder: we look at .txt files, where a .txt file is an annotated Stanford NER file. this function
    is for the re-trained models
    :param output_csv: results
    :return: None
    """
    listOfFiles = glob.glob(input_folder + '*.txt')  # I've tested this; it works
    out = codecs.open(output_csv, 'w', 'utf-8')
    out.write('FileName,TP,TN,FP,FN,Precision,Recall,F-Measure\n')
    for infile in listOfFiles:
        TP = 0
        TN = 0
        FP = 0
        FN = 0
        with codecs.open(infile, 'r', 'utf-8') as f:
            for line in f:
                if len(line.strip()) == 0:
                    continue
                else:
                    fields = re.split('\t', line[0:-1])
                    predicted = fields[-1]
                    actual = fields[-2]
                    if actual == '0':
                        if predicted == actual:
                            TN += 1
                        else:
                            FP += 1
                    elif actual == 'MISC.':
                        if predicted == actual:
                            TP += 1
                        else:
                            FN += 1
                    else:
                        print 'problems with labels!'
                        print line
        prf = _compute_precision_recall_f1measure(TP, TN, FP, FN)
        out.write(infile+','+str(TP)+','+str(TN)+','+str(FP)+','+str(FN)+','+str(prf[0])+','+str(prf[1])+','+str(prf[2])+'\n')
    out.close()

def generate_pretrained_results_csv(input_folder, output_csv, actual_label):
    """
    Do not use for age, or where actual_label is not defined
    :param input_folder: we look at .txt files, where a .txt file is an annotated Stanford NER file. this function
    is for the pre-trained models
    :param output_csv: results
    :param actual_label: e.g. LOCATION for cities and states, PERSON for names
    :return: None
    """
    listOfFiles = glob.glob(input_folder + '*.txt')  # I've tested this; it works
    out = codecs.open(output_csv, 'w', 'utf-8')
    out.write('FileName,TP,TN,FP,FN,Precision,Recall,F-Measure\n')
    for infile in listOfFiles:
        TP = 0
        TN = 0
        FP = 0
        FN = 0
        with codecs.open(infile, 'r', 'utf-8') as f:
            for line in f:
                if len(line.strip()) == 0:
                    continue
                else:
                    fields = re.split('\t', line[0:-1])
                    predicted = fields[-1]
                    actual = fields[-2]
                    if actual == '0':
                        if predicted != actual_label:
                            TN += 1
                        else:
                            FP += 1
                    elif actual == 'MISC.':
                        if predicted == actual_label:
                            TP += 1
                        else:
                            FN += 1
                    else:
                        print fields
                        print 'error! Neither pos. nor neg...'
                        continue

        prf = _compute_precision_recall_f1measure(TP, TN, FP, FN)
        out.write(infile+','+str(TP)+','+str(TN)+','+str(FP)+','+str(FN)+','+str(prf[0])+','+str(prf[1])+','+str(prf[2])+'\n')
    out.close()

def generate_pretrained_results_age_csv(input_folder, output_csv):
    """
    Designed for the age test files. We'll use any non-0 label as a positive label.
    :param input_folder: we look at .txt files, where a .txt file is an annotated Stanford NER file. this function
    is for the pre-trained models
    :param output_csv: results

    :return: None
    """
    listOfFiles = glob.glob(input_folder + '*.txt')  # I've tested this; it works
    out = codecs.open(output_csv, 'w', 'utf-8')
    out.write('FileName,TP,TN,FP,FN,Precision,Recall,F-Measure\n')
    for infile in listOfFiles:
        TP = 0
        TN = 0
        FP = 0
        FN = 0
        with codecs.open(infile, 'r', 'utf-8') as f:
            for line in f:
                if len(line.strip()) == 0:
                    continue
                else:
                    fields = re.split('\t', line[0:-1])
                    predicted = fields[-1]
                    actual = fields[-2]
                    if actual == '0':
                        if predicted == '0':
                            TN += 1
                        else:
                            FP += 1
                    elif actual == 'MISC.':
                        if predicted != '0':
                            TP += 1
                        else:
                            FN += 1
                    else:
                        print fields
                        print 'error! Neither pos. nor neg...'
                        continue

        prf = _compute_precision_recall_f1measure(TP, TN, FP, FN)
        out.write(infile+','+str(TP)+','+str(TN)+','+str(FP)+','+str(FN)+','+str(prf[0])+','+str(prf[1])+','+str(prf[2])+'\n')
    out.close()

def _compute_precision_recall_f1measure(TP, TN, FP, FN):
    """
    I deliberately don't check for 0 denom. except when computing f1-m.
    :param TP:
    :param TN:
    :param FP:
    :param FN:
    :return: A 3-list containing prec rec f1-m
    """
    if TP+FP > 0:
        precision = TP*1.0/(TP+FP)
    else:
        precision = 0.0
    recall = TP*1.0/(TP+FN)
    if precision == 0.0 and recall == 0.0:
        f1m= 0.0
    else:
        f1m = 2*precision*recall/(precision+recall)
    answer = [precision, recall, f1m]
    return answer


def generate_pretrained_testing_script(root_folder):
    categories = ['ages-', 'names-', 'text-cities-', 'text-states-', 'title-cities-']
    out = codecs.open(root_folder + 'testing_script.sh', 'w', 'utf-8')
    # command = 'java -cp stanford-ner.jar:lib/* edu.stanford.nlp.ie.crf.CRFClassifier -loadClassifier ner-model.ser.gz -testFile jane-austen-emma-ch2.tsv'
    command_root1 = 'java -cp stanford-ner.jar:lib/* edu.stanford.nlp.ie.crf.CRFClassifier -loadClassifier '
    command_root2 = ' -testFile '
    modelfile1 = root_folder+'classifiers/english.all.3class.distsim.crf.ser.gz'
    modelfile2 = root_folder + 'classifiers/english.conll.4class.distsim.crf.ser.gz'
    modelfile3 = root_folder + 'classifiers/english.muc.7class.distsim.crf.ser.gz'
    for category in categories:
        for j in range(1, 11):
            # modelfile30 = root_folder + category + 'models/30_' + str(j) + '.ser.gz'
            # modelfile70 = root_folder + category + 'models/70_' + str(j) + '.ser.gz'
            testfile30 = root_folder + category + '30-70/30_' + str(j) + '.txt'
            testfile70 = root_folder + category + '30-70/70_' + str(j) + '.txt'
            outputfile30_1 = root_folder + category + 'test/test_30_pretALL_' + str(j) + '.txt'
            outputfile70_1 = root_folder + category + 'test/test_70_pretALL_' + str(j) + '.txt'
            outputfile30_2 = root_folder + category + 'test/test_30_pretCONLL_' + str(j) + '.txt'
            outputfile70_2 = root_folder + category + 'test/test_70_pretCONLL_' + str(j) + '.txt'
            outputfile30_3 = root_folder + category + 'test/test_30_pretMUC_' + str(j) + '.txt'
            outputfile70_3 = root_folder + category + 'test/test_70_pretMUC_' + str(j) + '.txt'
            command = command_root1 + modelfile1 + command_root2 + testfile70 + ' > ' + outputfile70_1 + '\n'
            out.write(command)
            command = command_root1 + modelfile1 + command_root2 + testfile30 + ' > ' + outputfile30_1 + '\n'
            out.write(command)
            command = command_root1 + modelfile2 + command_root2 + testfile70 + ' > ' + outputfile70_2 + '\n'
            out.write(command)
            command = command_root1 + modelfile2 + command_root2 + testfile30 + ' > ' + outputfile30_2 + '\n'
            out.write(command)
            command = command_root1 + modelfile3 + command_root2 + testfile70 + ' > ' + outputfile70_3 + '\n'
            out.write(command)
            command = command_root1 + modelfile3 + command_root2 + testfile30 + ' > ' + outputfile30_3 + '\n'
            out.write(command)
    out.close()

# www_path='/Users/mayankkejriwal/ubuntu-vm-stuff/home/mayankkejriwal/tmp/www-experiments/stanford-ner-2015-12-09/'
# generate_pretrained_results_age_csv(www_path+'ages-test/pretrained/',
#                                 www_path+'summary_of_results/pre-trained-ages.csv')
# generate_pretrained_testing_script(www_path)
# generate_random_samples(www_path+'stanfordNER-full-files/tagged-title-cities.json', www_path+'stanfordNER-partitions/title-cities/30-70/')
# trial2()
# print_annotation_statistics(www_path+'tagged-ages.json')