import codecs, re, json
import math
import networkx as nx
from networkx.drawing import nx_agraph
from networkx import algorithms, assortativity
from networkx import info, density, degree_histogram
import matplotlib.pyplot as plt
import glob
from sklearn import linear_model
import numpy as np
import pygraphviz as pgv
from datetime import datetime, timedelta
from dateutil.parser import parse
import math
import sys


path = '/Users/mayankkejriwal/datasets/memex/nebraska-data/data-sharing/'

def find_faulty_phones(name_phone_file=path+'adj_lists/name_phone.jl'):
    with codecs.open(name_phone_file, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            phones = obj[obj.keys()[0]].keys()
            for p in phones:
                if len(p) == 10 or (len(p)==11 and p[0]=='1'):
                    pass
                else:
                    print p

def find_faulty_phones_int(name_phone_file=path+'adj_lists/int-phones.jl'):
    with codecs.open(name_phone_file, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            phones = obj[obj.keys()[0]]
            for p in phones:
                if len(p) == 10 or (len(p)==11 and p[0]=='1'):
                    pass
                else:
                    print p

def find_faulty_postids(name_postid_file=path+'adj_lists/name_postid_faulty.jl'):
    faulty_postids = set()
    with codecs.open(name_postid_file, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            postids = obj[obj.keys()[0]].keys()
            # if len(postids) == 0:
            #     print line
            for p in postids:
                if len(p) <= 5: # turns out the blank string is the only one that shows up as faulty
                    faulty_postids.add(p)
    print faulty_postids

def find_faulty_postids_int(name_postid_file=path+'adj_lists/int-postid.jl'):
    faulty_postids = set()
    with codecs.open(name_postid_file, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            postid = obj[obj.keys()[0]]
            if not postid or len(postid) <= 5: # turns out the blank string is the only one that shows up as faulty
                faulty_postids.add(postid)
    print faulty_postids

def find_faulty_day_int(name_postid_file=path+'adj_lists/int-day.jl'):
    faulty_day = set()
    with codecs.open(name_postid_file, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            day = str(obj[obj.keys()[0]])
            if not day or len(day) != 8: # turns out the blank string is the only one that shows up as faulty
                faulty_day.add(day)
    print faulty_day

def reprocess_faulty_postids(name_postid_file=path+'adj_lists/name_postid_faulty.jl', new_file=path+'adj_lists/name_postid.jl'):
    # faulty_postids = set()
    count = 0
    out = codecs.open(new_file, 'w', 'utf-8')
    with codecs.open(name_postid_file, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            postids = obj[obj.keys()[0]].keys()
            for p in postids:
                if len(p) <= 5:
                    del obj[obj.keys()[0]][p]
                    count += 1
            if bool(obj[obj.keys()[0]]):
                json.dump(obj, out)
                out.write('\n')
            else:
                print 'discarded name ',obj.keys()[0],' because it has no postids that are non-blank'

    out.close()
    print 'deleted ',str(count),' postids in total from file.'

def reprocess_faulty_phones(name_postid_file=path+'adj_lists/name_phone_faulty.jl', new_file=path+'adj_lists/name_phone.jl'):
    """
    Because of the '1' prefix issue with phones, we need to replace.
    :param name_postid_file:
    :param new_file:
    :return:
    """
    # faulty_postids = set()
    count = 0
    out = codecs.open(new_file, 'w', 'utf-8')
    with codecs.open(name_postid_file, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            new_dict = dict()
            name = obj.keys()[0]
            phones = obj[name]
            for phone, ids in phones.items():
                if len(phone) == 11:
                    if phone[0] != '1':
                        raise Exception
                    new_dict[phone[1:]] = ids
                    count += 1
                elif len(phone) == 10:
                    new_dict[phone] = ids
                else:
                    raise Exception
            if bool(new_dict):
                obj[name] = new_dict
                json.dump(obj, out)
                out.write('\n')
            else:
                print 'discarded name ',obj.keys()[0],' because it has no phones that are non-blank'

    out.close()
    print 'replaced ',str(count),' phones in total from file.'


def find_ccs_without_values(conn_comp_map=path+'connected-component-analysis/connected-component-postid-map.jl',
                           output_file=path+'connected-component-analysis/conn-comps-without-postid.txt'):
    out = codecs.open(output_file, 'w', 'utf-8')
    count = 0
    with codecs.open(conn_comp_map, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            if len(obj[obj.keys()[0]]) == 0:
                out.write(obj.keys()[0]+'\n')
                count += 1
    out.close()
    print count

def intersect_phone_postid_ccs_without_values(postid=path+'connected-component-analysis/conn-comps-without-postid.txt',

                                              phone=path+'connected-component-analysis/conn-comps-without-phones.txt'):
    postid_set = set()
    with codecs.open(postid, 'r', 'utf-8') as f:
        for line in f:

            postid_set.add(line[0:-1])

    print len(postid_set)

    with codecs.open(phone, 'r', 'utf-8') as f:
        for line in f:
            if line[0:-1] in postid_set:
                print line[0:-1]


# find_faulty_day_int() # no faulty post ids in the re-generated int-postid file
# intersect_phone_postid_ccs_without_values() # there are no connected components that have no postid AND no phone (phew)
reprocess_faulty_phones()
# console output from running reprocess_:
# discarded name  fernan  because it has no postids that are non-blank
# deleted  102  postids in total from file.

