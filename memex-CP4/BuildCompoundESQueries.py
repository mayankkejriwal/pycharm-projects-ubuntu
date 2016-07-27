class BuildCompoundESQueries:
    """
    Each (always static) method in this class will take some inputs, and
    will output a complete ES query that can then be executed in ExecuteESQueries
    
    """

    @staticmethod
    def mergeBools(*args):
        """Each arg must be a dict representing a bool query
        Merging will only occur at the top level.
        """
        must = []
        should = []
        must_not = []
        filter = []
        for arg in args:
            if 'bool' not in arg:
                print 'Error! You haven\'t passed in a bool query'
            else:
                if 'must' in arg['bool']:
                    must = must+arg['bool']['must']
                if 'should' in arg['bool']:
                    should = should+arg['bool']['should']
                if 'must_not' in arg['bool']:
                    must_not = must_not+arg['bool']['must_not']
                if 'filter' in arg['bool']:
                    filter = filter+arg['bool']['filter']
        return BuildCompoundESQueries.build_bool_arbitrary(must, must_not, should, filter)

    @staticmethod
    def build_bool_must(must):
        """Takes a must list/dict as input and builds a bool query"""
        return BuildCompoundESQueries.build_bool_arbitrary(must)

    @staticmethod
    def build_bool_arbitrary(must=None, must_not=None, should=None, filter=None):
        """Builds a bool query from the various parameters"""
        answer = {}
        answer["bool"] = {}
        if must:
            answer["bool"]["must"] = must

        if must_not:
            answer["bool"]["must_not"] = must_not

        if should:
            answer["bool"]["should"] = should

        if filter:
            answer["bool"]["filter"] = filter

        return answer

    @staticmethod
    def build_constant_score_filters(list_of_clauses):
        """Builds a list of constant_score clauses from the given clauses"""
        answer = []
        for clause in list_of_clauses:
            constant_score = {}
            constant_score['constant_score'] = {'filter':clause}
            answer.append(constant_score)

        return answer

    