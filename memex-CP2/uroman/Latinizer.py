import os

class Latinizer:
    """


    """

    @staticmethod
    def hello_world():
        """
        This is The first thing I'm writing in PyCharm, so fingers crossed
        :return: None
        """
        print("Hello World")
        os.system('ls')

    @staticmethod
    def latinized_from_file(input_file, output_file = None):
        """
        We can call Ulf's package from here. if output_file is none
        results will simply be printed out.
        Attributes:
            input_file: the file containing the text to be latinized.
            output_file: the file where the latinized text will be written

        :return: None
        """
        uroman = "/home/mayankkejriwal/Downloads/memex-cp2/uroman-v0.5/bin/uroman.pl"
        if not output_file:
            command = uroman + " < "+input_file
        else:
            command = uroman + " < "+input_file + " > "+output_file
        os.system(command)



#Latinizer.latinized_from_file("/home/mayankkejriwal/Downloads/memex-cp2/postsAFiltered.jl",
 #                             output_file="/home/mayankkejriwal/Downloads/memex-cp2/postsAFiltered-latinized.jl")
