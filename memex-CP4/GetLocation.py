__author__ = 'majid'
import json
# from jsonpath_rw.jsonpath import JSONPath
from jsonpath_rw import jsonpath, parse
import itertools

def get_location(city2state, city2country, jobj, city_field, state_field, country_field):
    try:
        city_path = parse(city_field)
        state_path = parse(state_field)
        country_path = parse(country_field)
        cities = [x.value for x in city_path.find(jobj)]
        states = [x.value for x in state_path.find(jobj)]
        countries = [x.value for x in country_path.find(jobj)]

        res = []

        if len(cities) == 0 or \
            len(states) == 0 or \
            len(countries) == 0:
            return
        else:
            jobj['location'] = ['{},{},{}'.format(x[0], x[1], x[2])
                                if x[2] == 'united states'
                                else '{},{}'.format(x[0], x[2])
                                for x in itertools.product(cities, states, countries)]
    except:
        return

if __name__ == '__main__':
    jobj = {'my_data':{
        'city': ['los angeles'],
        'state': ['california'],
        'country': ['united states', 'canada']
    }}
    get_location(None, None, jobj, '$.my_data.city[*]', '$.my_data.state[*]', '$.my_data.country[*]')
    print(jobj)