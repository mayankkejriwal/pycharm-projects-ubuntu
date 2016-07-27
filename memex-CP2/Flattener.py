import json, codecs

class Flattener:
    """
    The challenge files are set up so that there can be multiple objects with the
    same user-name. This class contains methods that allow us to 'flatten' a file
    or to 'join' two files so that the user name key-constraint can be maintained
    """

    @staticmethod
    def _merge(list_a, list_b):
        set_c = set(list_a)
        for item in list_b:
            set_c.add(item)
        return list(set_c)

    @staticmethod
    def flatten_single_file(input_file, output_file):
        """

        :param input_file:
        :param output_file:
        :return: None
        """
        objects = {}
        with codecs.open(input_file, 'r', 'utf-8') as f:
         for line in f:
             d = json.loads(line)
             if d['userName'] in objects:
                 #print d['userName']
                 objects[d['userName']]['tokens_set'] = Flattener._merge(objects[d['userName']]['tokens_set'],
                                                                         d['tokens_set'])
             else:
                 objects[d['userName']] = d
        file = codecs.open(output_file, 'w', 'utf-8')
        for v in objects.itervalues():
           json.dump(v, file)
           file.write('\n')
        file.close()

    @staticmethod
    def join_two_files(input_file1, input_file2, output_file):
        """
        Both files must have been flatened previously. Note that the join is an outer join.
        If input_file2 is not flattened, the code will not print out an error message to console,
        this is a subtle error. It is the user's responsibility to ensure all inputs are
        flattened
        :param input_file1: The first file for joining
        :param input_file2: The second file for joining
        :param output_file: the file where the joined output is printed out
        :return: None
        """
        objects = {}
        with codecs.open(input_file1, 'r', 'utf-8') as f:
         for line in f:
             d = json.loads(line)
             if d['userName'] in objects:
                 print 'Error! You have not passed in a flattened file'
             else:
                 objects[d['userName']] = d


        with codecs.open(input_file2, 'r', 'utf-8') as f:
         for line in f:
             d = json.loads(line)
             if d['userName'] in objects:
                 objects[d['userName']]['tokens_set'] = Flattener._merge(objects[d['userName']]['tokens_set'],
                                                                         d['tokens_set'])
             else:
                 objects[d['userName']] = d
        file = codecs.open(output_file, 'w', 'utf-8')
        for v in objects.itervalues():
           json.dump(v, file)
           file.write('\n')
        file.close()

#upper_path = '/home/mayankkejriwal/Downloads/memex-cp2/'
#Flattener.flatten_single_file(upper_path+'postsASerialized.jl',upper_path+'postsAFlattened.jl')
#Flattener.join_two_files(upper_path+'postsBFlattened.jl', upper_path+'usersBFlattened.jl', upper_path+'joinedB.jl')