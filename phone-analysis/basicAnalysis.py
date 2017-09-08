import codecs, re, json

path = '/Users/mayankkejriwal/datasets/memex/nebraska-data/data-sharing/'

def print_statistics_name_postid_phones(input_file = path+'data_for_memex.json'):
    name_dict = dict()
    name_dict[0] = 0
    postid_dict = dict()
    postid_dict[0] = 0
    postid_dict[1] = 0
    phone_dict = dict()
    phone_dict[0] = 0
    with codecs.open(input_file, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            if 'name' not in obj:
                name_dict[0] += 1
            else:
                k = len(obj['name'])
                if k not in name_dict:
                    name_dict[k] = 0
                name_dict[k] += 1

            if 'phone' not in obj:
                phone_dict[0] += 1
            else:
                k = len(obj['phone'])
                if k not in phone_dict:
                    phone_dict[k] = 0
                phone_dict[k] += 1

            if 'post_id' not in obj:
                postid_dict[0] += 1
            else:

                postid_dict[1] += 1
    print 'printing statistics of names...'
    print 'num names \t num jsons'
    for k, v in name_dict.items():
        print str(k)+'\t'+str(v)
    print 'printing statistics of phones...'
    print 'num phones \t num jsons'
    for k, v in phone_dict.items():
        print str(k) + '\t' + str(v)
    print 'printing statistics of post_id...'
    print 'num post_ids \t num jsons'
    for k, v in postid_dict.items():
        print str(k) + '\t' + str(v)


# print_statistics_name_postid_phones()
