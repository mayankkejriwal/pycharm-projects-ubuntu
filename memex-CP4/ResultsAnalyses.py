import codecs
import re
import glob
import pprint


def _aggregate_list(l, agg_func='avg'):
    if agg_func == 'avg':
        total = 0.0
        for k in l:
            total += k
        return total/len(l)


def compile_results(results_dir, latex=False):
    """
    Prints out a 'table' of results.
    :param results_dir: Contains comma-delimited files, with each file representing a single systems-result
    :param latex: if true, will print out something that can be directly copy-pasted into a latex file.
    :return: None
    """
    listOfFiles = glob.glob(results_dir+'*.txt') # I've tested this; it works
    all_results = dict()
    for f in listOfFiles:
        results = dict()
        with codecs.open(f, 'r', 'utf-8') as ff:
            pointFact = list()
            cluster = list()
            aggregation = list()
            for line in ff:
                fields = re.split(',', line)
                if len(fields) != 2:
                    print 'we have a problem in this file and line:'
                    print f
                    print line
                    raise Exception
                else:
                    fields[1] = fields[1][0:-1] # strip out the newline
                query = int(fields[0])
                if query<100:
                    pointFact.append(float(fields[1]))
                elif query>=1800:
                    aggregation.append(float(fields[1]))
                else:
                    cluster.append(float(fields[1]))
            results['pointFact'] = _aggregate_list(pointFact, agg_func='avg')
            results['cluster'] = _aggregate_list(cluster, agg_func='avg')
            results['aggregation'] = _aggregate_list(aggregation, agg_func='avg')
            results['file'] = re.split('/',f)[-1]
        all_results[results['file']] = results
    if latex:
        print 'we assume the order to be file, pointfact, cluster, aggregation'
        keys = all_results.keys()
        keys.sort()
        for key in keys:
            k = all_results[key]
            string = '{'+k['file']+'} & {'+format(k['pointFact'], '.2f')+'} & {'+format(k['cluster'],'.2f')+\
                     '} & {'+format(k['aggregation'],'.2f')+'}  \\\ \hline'
            print string


    else:
        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(all_results)

path = '/home/mayankkejriwal/Downloads/memex-cp4/all_results/'
compile_results(path, latex=True)