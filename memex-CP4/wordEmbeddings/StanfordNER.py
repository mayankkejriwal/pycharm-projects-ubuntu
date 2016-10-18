from nltk.tag import StanfordNERTagger
from nltk.tokenize import word_tokenize
import codecs
from random import shuffle

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
# generate_pretrained_testing_script(www_path)
# generate_random_samples(www_path+'stanfordNER-full-files/tagged-title-cities.json', www_path+'stanfordNER-partitions/title-cities/30-70/')
# trial1()