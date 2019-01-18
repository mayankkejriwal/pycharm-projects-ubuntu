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
import csv

folder_path = '/Users/mayankkejriwal/datasets/memex/USPS/'


def convert_csv_to_jsonl(in_file=folder_path+'recovery_escort_ad.csv', out_file=folder_path+'recovery_escort_ad.jsonl'):
    count = 0
    faulty = 0
    out = codecs.open(out_file, 'w', 'utf-8')
    with open(in_file, 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')

        schema = ['category_name', 'age', 'object_id', 'title', 'ad_content', 'user', 'email', 'mobile_number']
        for row in reader:

            obj = dict()
            if len(row) != len(schema):
                # print count
                # print row
                faulty += 1
                continue
            else:
                for i in range(0, len(row)):
                    obj[schema[i]] = row[i]
                json.dump(obj, out)
                out.write('\n')
                count += 1
    print 'num lines read and converted to json: ',str(count)
    print 'num faulty lines: ',str(faulty)
    out.close()





# convert_csv_to_jsonl()