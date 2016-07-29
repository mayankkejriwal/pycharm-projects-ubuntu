import re, SparqlTranslator, SelectExtractors


def build_exists_clause(field):
    answer = {}
    tmp = {}
    tmp["field"] = field
    answer['exists'] = tmp
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


def build_match_clause(field, string):
    """
    Best demonstrated by example. Suppose field = 'itemOffered.name' and string = 'jasmine',
    the function will return a dictionary d:
    d = {
         "match":   {
            "itemOffered.name" : "jasmine"
            }    
        }
    The intent is that this dictionary should be embedded into a valid elasticsearch query.
    """
    answer = {}
    tmp = {}
    tmp[field] = string
    answer['match'] = tmp
    return answer


def build_count_match_clause(field, string):
    """
    Intended for .*_count fields, since they usually contain xsd integers. If not, this is like
    a normal match clause.
    The intent is that this dictionary should be embedded into a valid elasticsearch query.
    """
    answer = {}
    tmp = {}
    if SelectExtractors.SelectExtractors._parseXSDIntegerLiteral(string):
        tmp[field] = SelectExtractors.SelectExtractors._parseXSDIntegerLiteral(string) # hacky, I know.
    else:
        tmp[field] = string
    answer['match'] = tmp
    return answer


def build_match_phrase_clause(field, string):
    """
    Best demonstrated by example. Suppose field = 'itemOffered.name' and string = 'jasmine',
    the function will return a dictionary d:
    d = {
         "match_phrase":   {
            "itemOffered.name" : "jasmine"
            }
        }
    The intent is that this dictionary should be embedded into a valid elasticsearch query.
    """
    answer = {}
    tmp = {}
    tmp[field] = string
    answer['match_phrase'] = tmp
    return answer


def build_phone_match_clause(field, string):
    """
    Meant for phone field Best demonstrated by example. Suppose field = 'phone' and string = '512-435-4444',
    the function will return a dictionary d:
    d = {
         "match":   {
            "phone" : "5124354444"
            }
        }
    The intent is that this dictionary should be embedded into a valid elasticsearch query.
    """
    answer = {}
    tmp = {}
    string1 = ''
    for char in string:
        if re.search("[^0-9]", char) is None:
            string1 += char
    candidates = []
    candidates.append(string1)
    if len(string1) > 10:
        candidates.append(string1[2:])
    if SparqlTranslator.SparqlTranslator._strip_initial_zeros(string1) != string1:
        candidates.append(SparqlTranslator.SparqlTranslator._strip_initial_zeros(string1))
    tmp[field] = ' '.join(candidates)
    answer['match'] = tmp
    return answer


def build_gender_match_clause(field, string):
    """
    Meant for gender field. At present we do some minimal normalization. For example, 'trans' will get
    converted to 'transgender' etc.
    The intent is that this dictionary should be embedded into a valid elasticsearch query.
    """
    answer = {}
    tmp = {}
    if string.lower() == 'trans':
        string1 = 'transgender'
    elif string.lower() == 'f':
        string1 = 'female'
    elif string.lower() == 'm':
        string1 = 'male'
    else:
        string1 = string
    tmp[field] = string1
    answer['match'] = tmp
    return answer


def build_phone_regexp_clause(field, string):
    """
    Meant for phone field. Best demonstrated by example. Suppose field = 'phone' and string = '512-435-4444',
    the function will return a dictionary d:
    d = {
         "regexp":   {
            "phone" : ".*5124354444"
            }
        }
    The intent is that this dictionary should be embedded into a valid elasticsearch query.
    """
    answer = {}
    tmp = {}
    string1 = ''
    for char in string:
        if re.search("[^0-9]", char) is None:
            string1 += char
    tmp[field] = '.*'+string1
    answer['regexp'] = tmp
    return answer


def build_match_clause_and(field, string):
    """
    Best demonstrated by example. Suppose field = 'itemOffered.name' and string = 'jasmine',
    the function will return a dictionary d:
    d = {
         "match":   {
            "itemOffered.name" :
            {
            "query":"jasmine",
            "operator":"and"
            }
          }
        }
    The intent is that this dictionary should be embedded into a valid elasticsearch query.
    """
    answer = {}
    tmp = {}
    tmp[field] = {}
    tmp[field]['query'] = string
    tmp[field]['operator'] = 'and'
    answer['match'] = tmp
    return answer


def build_match_clause_inner(field, string):
    """
    Best demonstrated by example. Suppose field = 'itemOffered.name' and string = 'jasmine',
    the function will return a dictionary d:
    d = {"meta":["inner"],
         "match":   {
            "itemOffered.name" :
            {
            "query":"jasmine"

            }
          }
        }
    The intent is that this dictionary should be PROCESSED and then embedded into a valid elasticsearch query.
    Currently, the semantics indicate that all inner matches should be placed in a bool-should, which is
    then embedded in some way in an outer bool.
    Embedding only the query is invalid!
    """
    answer = build_match_clause(field, string)
    answer['meta'] = ['inner']

    return answer


def build_match_phrase_clause_inner(field, string):
    """
    Best demonstrated by example. Suppose field = 'itemOffered.name' and string = 'jasmine',
    the function will return a dictionary d:
    d = {"meta":["inner"],
         "match_phrase":   {
            "itemOffered.name" :
            {
            "query":"jasmine"

            }
          }
        }
    The intent is that this dictionary should be PROCESSED and then embedded into a valid elasticsearch query.
    Currently, the semantics indicate that all inner matches should be placed in a bool-should, which is
    then embedded in some way in an outer bool.
    Embedding only the query is invalid!
    """
    answer = build_match_phrase_clause(field, string)
    answer['meta'] = ['inner']

    return answer


def build_phone_match_clause_inner(field, string):
    """
    Best demonstrated by example. Suppose field = 'itemOffered.name' and string = 'jasmine',
    the function will return a dictionary d:
    d = {"meta":["inner"],
         "match":   {
            "itemOffered.name" :
            {
            "query":"jasmine"

            }
          }
        }
    The intent is that this dictionary should be PROCESSED and then embedded into a valid elasticsearch query.
    Currently, the semantics indicate that all inner matches should be placed in a bool-should, which is
    then embedded in some way in an outer bool.
    Embedding only the query is invalid!
    """
    answer = build_phone_match_clause(field, string)
    answer['meta'] = ['inner']
    return answer


def build_phone_regexp_clause_inner(field, string):
    """
    Best demonstrated by example. Suppose field = 'itemOffered.name' and string = 'jasmine',
    the function will return a dictionary d:
    d = {"meta":["inner"],
         "match":   {
            "itemOffered.name" :
            {
            "query":"jasmine"

            }
          }
        }
    The intent is that this dictionary should be PROCESSED and then embedded into a valid elasticsearch query.
    Currently, the semantics indicate that all inner matches should be placed in a bool-should, which is
    then embedded in some way in an outer bool.
    Embedding only the query is invalid!
    """
    answer = build_phone_regexp_clause(field, string)
    answer['meta'] = ['inner']
    return answer


def build_range_clause(field, quantifier, string):
    """

    quantifier must be <, >, <= or >=
    if quantifier is == or != call
    """
    range_str = {}
    if quantifier == '<':
        range_str['lt'] = string
    elif quantifier == '<=':
        range_str['lte'] = string
    elif quantifier == '>':
        range_str['gt'] = string
    elif quantifier == '>=':
        range_str['gte'] = string
    elif quantifier == '!=' or quantifier == '==':
        print('Wrong function called for ', quantifier)
        return
    else:
        print('bad quantifier input')
        return
    answer = {}
    tmp = {}
    tmp[field] = range_str
    answer['range'] = tmp
    return answer

