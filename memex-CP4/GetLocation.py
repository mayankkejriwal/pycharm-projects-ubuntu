__author__ = 'majid'
import json


def prune_cities(city, candidates,states,countries):
    res = []
    candidates = sorted(candidates, key=lambda x:int(x['populationofarea']), reverse=True)
    for c in candidates:
        country = [x for x in countries if x in c['country']]
        state = [x for x in states if x in c['state']]
        if (state != []) and \
                (country != []):
            res.append({'city': city,
                        'state': c['state'][0],
                        'country': c['country'][0],
                        'pop': int(c['populationofarea'])})
    if candidates != [] and res == []:

        c = candidates[0]
        res.append({'city': city,
                    'state': c['state'][0],
                    'country': c['country'][0],
                    'pop': int(c['populationofarea'])})
        res.append({'city': city,
                    'state': '',
                    'country': '',
                    'pop': 0})
    res = sorted(res, key=lambda x: x['pop'], reverse=True)
    return res


def format_res(res):
    formatted_res = []
    for x in res:
        if x['country'] == 'united states':
            formatted_res.append('{},{}'.format(x['city'], x['state']))
        elif x['country'] == '' and x['state'] == '':
            formatted_res.append(x['city'])
        else:
            formatted_res.append('{},{}'.format(x['city'], x['country']))
    return formatted_res


def remove_duplicates(x):
    s = set()
    return [xx.lower() for xx in x if not (xx in s or s.add(xx))]


def get_location(citydict, cities, states, countries):
    try:
        cities = remove_duplicates(cities)
        states = remove_duplicates(states)
        countries = remove_duplicates(countries)
        res = []
        if len(cities) == 0:
            return []

        for c in cities:
            if c in citydict:
                temp = prune_cities(c, citydict[c], states, countries)
                if temp == []:
                    res.append({'city': c,
                                'state': '',
                                'country': ''})
                else:
                    res += temp
            else:
                res.append({'city': c,
                            'state': '',
                            'country': ''})

        return format_res(res)
    except:
        return []

# if __name__ == '__main__':
#     jobj = {'my_data':{
#         'city': ['los angeles', 'los angeles', 'los angeles', 'manhattan', 'new york'],
#         'state': ['new york', 'california'],
#         'country': ['us']
#     }}
#     citydict_path = '/Users/majid/DIG/dig-dictionaries/geonames-populated-places/city_dict_over1K.json'
#     print get_location(json.load(open(citydict_path)), jobj['my_data']['city'], jobj['my_data']['state'], jobj['my_data']['country'])
    # print get_location({}, jobj['my_data']['city'], jobj['my_data']['state'], jobj['my_data']['country'])