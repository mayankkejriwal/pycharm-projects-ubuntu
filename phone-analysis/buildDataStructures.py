import codecs, json


def get_list_of_phones_emails_in_linhong_clusters(input_file, output_file):
    """
    Linhong's clusters treat phones and emails equally. Write another function if you must weed it out.
    :param input_file:
    :param output_file:
    :return:
    """
    set_of_phones = set()
    with codecs.open(input_file, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line)
            if 'phones' in obj:
                for phone in obj['phones']:
                    set_of_phones.add(phone['name'])
    out = codecs.open(output_file, 'w', 'utf-8')
    list_of_phones = list(set_of_phones)
    list_of_phones.sort()
    for phone in list_of_phones:
        out.write(phone)
        out.write('\n')
    out.close()


def split_phone_email(phone_email_list, phone_output_file, email_output_file):
    phone_out = codecs.open(phone_output_file, 'w', 'utf-8')
    email_out = codecs.open(email_output_file, 'w', 'utf-8')
    with codecs.open(phone_email_list, 'r', 'utf-8') as f:
        for line in f:
            element = line[0:-1]
            if element.isdigit():
                phone_out.write(line)
            else:
                email_out.write(line)

    phone_out.close()
    email_out.close()


def get_list_of_phones_CP1_ads(input_file, output_file):
    """
    Summer and november CP1 files have phone numbers in the same 'json' path.
    :param input_file:
    :param output_file:
    :return:
    """
    set_of_phones = set()
    with codecs.open(input_file, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line)
            if 'extractions' in obj and 'phonenumber' in obj['extractions']\
                    and 'results' in obj['extractions']['phonenumber'] and \
                obj['extractions']['phonenumber']['results'] is not None:
                if isinstance(obj['extractions']['phonenumber']['results'],list):
                    set_of_phones = set_of_phones.union(set(obj['extractions']['phonenumber']['results']))
                else:
                    set_of_phones.add(obj['extractions']['phonenumber']['results'])
    out = codecs.open(output_file, 'w', 'utf-8')
    list_of_phones = list(set_of_phones)
    list_of_phones.sort()
    for phone in list_of_phones:
        out.write(phone)
        out.write('\n')
    out.close()


def _read_in_list_as_set(input_file):
    set_of_items = set()
    with codecs.open(input_file, 'r', 'utf-8') as f:
        for line in f:
            set_of_items.add(line[0:-1])
    return set_of_items


def compute_jaccard_between_phone_lists(input_file_1, input_file_2):
    set1 = _read_in_list_as_set(input_file_1)
    print 'len. of set 1 is: '+str(len(set1))
    set2 = _read_in_list_as_set(input_file_2)
    print 'len. of set 2 is: ' + str(len(set2))
    if len(set1) == 0 or len(set2) == 0:
        print '0.0'
    else:
        print 'intersection is: '+str(len(set1.intersection(set2)))
        print 'jaccard sim. is: '+str(len(set1.intersection(set2))*1.0/len(set1.union(set2)))


# path = '/Users/mayankkejriwal/datasets/memex/post-november-evals/'
# compute_jaccard_between_phone_lists(path+'CP1_november_pos_neg_phones.txt', path+'CP1_summer_neg.txt')
# get_list_of_phones_CP1_ads(path+'CP1_summer_neg.jsonl', path+'CP1_summer_neg.txt')
# split_phone_email(path+'phone-email-list-GT.txt',path+'phone-list-GT.txt',path+'email-list-GT.txt')
# get_list_of_phones_emails_in_linhong_clusters(path+'phone_GT_large_5-sim-0.1.txt',path+'phone-email-list-GT.txt')
