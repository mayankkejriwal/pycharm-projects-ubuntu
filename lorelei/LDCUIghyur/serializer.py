import codecs, re, json, gzip
import csv
import glob
from dateutil.parser import parse


path = '/Users/mayankkejriwal/Dropbox/lorelei/'
def ground_truthing_format(input_folder=path+'ldc_ground_truth/GroundTruth/', output_file=path+'ldc_ground_truth_3column.csv'):
    out = codecs.open(output_file, 'w', 'utf-8')
    out.write('time,uuid,translatedText\n')
    time_dict = dict()
    files = glob.glob(input_folder + '*.json')
    for fi in files:

            obj = json.load(codecs.open(fi, 'r', 'utf-8'))

            k = obj['loreleiJSONMapping']['createdAt']+','+obj['uuid']+','+obj['loreleiJSONMapping']['translatedText'].replace(',','')+'\n'
            if parse(obj['loreleiJSONMapping']['createdAt']) not in time_dict:
                time_dict[parse(obj['loreleiJSONMapping']['createdAt'])] = list()
            time_dict[parse(obj['loreleiJSONMapping']['createdAt'])].append(k)


    times = time_dict.keys()
    times.sort()
    for t in times:
        for l in time_dict[t]:
            out.write(l)
    out.close()

ground_truthing_format()