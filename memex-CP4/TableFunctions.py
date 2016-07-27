import re


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
    tmp[field] = string1
    answer['match'] = tmp
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
