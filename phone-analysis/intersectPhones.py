import codecs, re, json
import math


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
    print 'some phones in both memex and nebraska...'
    count = 0
    for p in inter:
        if count > 5:
            break
        print p
        count += 1
    union = phone_set_1.union(phone_set_2)
    print 'intersection length: ',str(len(inter))
    print 'union length: ', str(len(union))
    jacc = (1.0*len(inter))/len(union)
    print 'jaccard: ',str(jacc)


def compute_url_intersection_metrics(nebraska_phones, memex_phones):
    phone_dict_1 = dict()
    nebraska_count = 0
    phone_dict_2 = dict()
    memex_count = 0
    with codecs.open(nebraska_phones, 'r', 'utf-8') as f:
        for line in f:
            elements = re.split('\t', line[0:-1])
            if elements[0] not in phone_dict_1:
                phone_dict_1[elements[0]] = set()
                nebraska_count += 1
            for p in elements[1:]:
                phone_dict_1[elements[0]].add(p)
    print 'len of unique nebraska urls is ', str(nebraska_count)

    with codecs.open(memex_phones, 'r', 'utf-8') as f:
        for line in f:
            elements = re.split('\t', line[0:-1])
            if elements[0] not in phone_dict_2:
                phone_dict_2[elements[0]] = set()
                memex_count += 1
            for p in elements[1:]:
                phone_dict_2[elements[0]].add(p)
    print 'len of unique memex urls is ', str(memex_count)

    p1 = 0
    p12 = 0

    # print 'http://southernmaryland.backpage.com/WomenSeekMen/real-exotic-busty-beauty-five-companionship/26526979' in phone_dict_2
    # print 'http://southernmaryland.backpage.com/WomenSeekMen/real-exotic-busty-beauty-five-companionship/26526979' in phone_dict_1

    for k in phone_dict_1.keys():
        if k not in phone_dict_2:
            p1 += 1
        else:
            p12 += 1
            del phone_dict_2[k]
    p2 = memex_count-p12

    print 'num. of urls common to memex and nebraska...',str(p12)
    print 'num. of urls in nebraska but not memex',str(p1)
    print 'num. of urls in memex but not nebraska',str(p2)


def compute_phone_intersection_metrics_by_common_urls(nebraska_phones, memex_phones):
    phone_dict_1 = dict()
    nebraska_count = 0
    phone_dict_2 = dict()
    memex_count = 0
    with codecs.open(nebraska_phones, 'r', 'utf-8') as f:
        for line in f:
            elements = re.split('\t', line[0:-1])
            if elements[0] not in phone_dict_1:
                phone_dict_1[elements[0]] = set()
                nebraska_count += 1
            for p in elements[1:]:
                phone_dict_1[elements[0]].add(p)
    # print 'len of unique nebraska urls is ', str(nebraska_count)

    with codecs.open(memex_phones, 'r', 'utf-8') as f:
        for line in f:
            elements = re.split('\t', line[0:-1])
            if elements[0] not in phone_dict_1:
                continue # no need to store
            if elements[0] not in phone_dict_2:
                phone_dict_2[elements[0]] = set()
                memex_count += 1
            for p in elements[1:]:
                phone_dict_2[elements[0]].add(p)
    # print 'len of unique memex urls is ', str(memex_count)

    j = 0
    j_std = 0
    count = 0
    for k in phone_dict_2.keys():
        if k not in phone_dict_1:
            raise Exception
        else:
            jacc = 1.0*len(phone_dict_1[k].intersection(phone_dict_2[k]))/len(phone_dict_1[k].union(phone_dict_2[k]))
            j += jacc
            j_std += (jacc*jacc)
            count += 1
    avg_jacc = j/count
    j_std = j_std/count
    j_std = math.sqrt(j_std-(avg_jacc*avg_jacc))
    print 'average jaccard is ',str(avg_jacc)
    print 'std. dev. is ',str(j_std)




nebraska_path = '/Users/mayankkejriwal/datasets/memex/nebraska-data/memex_comm/'
memex_path = '/Users/mayankkejriwal/datasets/memex/'
compute_phone_intersection_metrics_by_common_urls(nebraska_path+'phones-urls-2017.txt', memex_path+'2017-relaxed-phones-urls.txt')
# compute_url_intersection_metrics(nebraska_path+'phones-urls-2017.txt', memex_path+'2017-relaxed-phones-urls.txt')
# compute_intersection_metrics(nebraska_path+'phones-2017.txt', memex_path+'2017-strict-phones.txt')
