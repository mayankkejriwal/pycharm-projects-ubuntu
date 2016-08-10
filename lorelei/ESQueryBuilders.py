def build_exists_clause(field):
    answer = {}
    tmp = {}
    tmp["field"] = field
    answer['exists'] = tmp
    return answer


def build_constant_score_exists_clause(field):
    exists = {}
    tmp = {}
    tmp["field"] = field
    exists['exists'] = tmp
    answer = dict()
    answer['constant_score'] = dict()
    answer['constant_score']['filter'] = exists
    return answer


def build_match_all_query():
    answer = dict()
    answer['match_all'] = dict()
    return answer


def build_term_clause(field, string):
    """
    Example: suppose field = 'status' and string = ''
    Returns something like
    {
          "term": {
            "status": "normal"
          }
    """
    answer = {}
    tmp = {}
    tmp[field] = string
    answer['term'] = tmp
    return answer
