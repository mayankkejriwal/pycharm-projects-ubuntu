class PrintUtils:
    """Takes various elastic search outputs as input (methods always static)
    and prints something or the other
    """

    @staticmethod
    def printItemOfferedFieldFromOfferFrames(offer_frames_list, field):
        """

        """
        for offer_frame in offer_frames_list:
            print(offer_frame['_source']['itemOffered'][field])

    @staticmethod
    def printField(frames_list, field):
        """
        Handles missing fields gracefully
        """
        for frame in frames_list:
            if field in frame['_source']:
                print(frame['_source'][field])
            else:
                print('*MISSING FIELD*')

    @staticmethod
    def returnField(frames_list, field):
        """
        Handles missing fields gracefully
        """
        answer = []
        for frame in frames_list:
            if field in frame['_source']:
                answer.append(frame['_source'][field])
            else:
                print('*MISSING FIELD*')
        return answer

    @staticmethod
    def printSimpleSelect(frames_list, simpleSelectDict):
        """

        :param frames_list: the retrieved_frames
        :param simpleSelectDict: meant to represent 'simple' select instances
        :return: None
        """
        for key in simpleSelectDict:
            print(key),
        print('')
        for frame in frames_list:
            for val in simpleSelectDict.itervalues():
                for v in val:
                    if v in frame['_source']:
                        print(frame['_source'][v]),
                    else:
                        print('FIELD_NOT_FOUND'),
            print('')

    @staticmethod
    def printGroupStatistics(group):
        """
        Prints out the key of each group, and the number of elements associated with each group
        :param group: The output of Grouper.Grouper.standardGrouper
        :return: None
        """
        print 'key\tnumber of group members'
        for key,member in group.items():
            print(key),
            print len(member)

    @staticmethod
    def printExtractedSimpleSelect(select):
        """
        Prints out select. This function needs to be further developed (e.g. pretty printing).
        :param select: The output of SelectExtractors.SelectExtractors.extractSimpleSelect
        :return: None
        """
        print select

