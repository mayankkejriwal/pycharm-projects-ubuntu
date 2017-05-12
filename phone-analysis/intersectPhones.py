import codecs, re, json


def compute_intersection_metrics(nebraska_phones, memex_phones):
    phone_set_1 = set()
    phone_set_2 = set()
    with codecs.open(nebraska_phones, 'r', 'utf-8') as f:
        for line in f:
            phones = re.split('\t', line[0:-1])
            for p in phones:
                phone_set_1.add(p)
    print 'len of unique nebraska phones is ',str(len(phone_set_1))
    with codecs.open(memex_phones, 'r', 'utf-8') as f:
        for line in f:
            phones = re.split('\t', line[0:-1])
            for p in phones:
                phone_set_2.add(p)
    print 'len of unique memex phones is ', str(len(phone_set_2))
    inter = phone_set_1.intersection(phone_set_2)
    union = phone_set_1.union(phone_set_2)
    print 'intersection length: ',str(len(inter))
    print 'union length: ', str(len(union))
    jacc = (1.0*len(inter))/len(union)
    print 'jaccard: ',str(jacc)

nebraska_path = '/Users/mayankkejriwal/datasets/memex/nebraska-data/memex_comm/'
memex_path = '/Users/mayankkejriwal/datasets/memex/'
compute_intersection_metrics(nebraska_path+'phones-2017.txt', memex_path+'2017-relaxed-phones.txt')
