import re, six, ResultExtractors, FieldIdentifiers
from GetLocation import get_location

class SelectExtractors:
    """
    The functions in this class should be called after executing an ES query. Some functions will
    be called if the grouper is not invoked (e.g. extractSimpleSelect), others should
    be called after the grouper is invoked.
    """

    @staticmethod
    def extractSimpleSelect(frames_list, simpleSelectDict, classifier_dict=None):
        """

        :param frames_list: the retrieved_frames
        :param simpleSelectDict: meant to represent 'simple' select instances. Each key
        maps to a list that contains exactly one value
        :return: a list of dictionaries. If a field from simpleSelectDict
        is not found in the frame, that field will not be returned in the dict.
        It is possible for dictionaries to be completely empty! The returned
        list will have same length as frames_list
        """
        answer = []
        for frame in frames_list:
            tmp = {}
            for key, val in simpleSelectDict.items():

                if FieldIdentifiers.is_location(val): # should be treated distinctly
                    tmp[key] = SelectExtractors.extract_locations(frame, classifier_dict)
                    continue

                if FieldIdentifiers.is_height(val):
                    val.add('high_precision.height.result.value.centimeter')
                    val.add('high_recall.height.result.value.centimeter')
                if FieldIdentifiers.is_weight(val):
                    val.add('high_precision.weight.result.value.kilogram')
                    val.add('high_recall.weight.result.value.kilogram')
                if FieldIdentifiers.is_price(val):
                    val.add('high_precision.price.result.value.price_per_hour')
                    val.add('high_recall.price.result.value.price_per_hour')

                if len(val) != 1:
                    new_val = SelectExtractors._prune_property_set(val)
                else:
                    new_val = list(val)

                extracted_values = dict()
                for v in new_val:
                    if ResultExtractors.ResultExtractors.is_property_in_source_frame(frame['_source'], v):
                        extracted_values[v] = ResultExtractors.ResultExtractors.\
                            get_property_from_source_frame(frame['_source'], v)
                        break # this line was added for the november evaluation, given we want to prioritize one extr.
                # print extracted_values
                cross_product = ResultExtractors.ResultExtractors._flatten_dict(extracted_values)
                # print cross_product

                # this entire segment of code assumes that Rahul's code is integrated.
                if classifier_dict and 'embeddings' in classifier_dict:
                    from rankingExtractions import rank
                    if FieldIdentifiers.is_name(val):
                            list_of_texts = SelectExtractors.extract_list_of_texts(frame)
                            # print list_of_texts
                            # print SelectExtractors._convert_cross_product_to_raw_list(cross_product)
                            if list_of_texts:
                                ranked_names = rank.rank(classifier_dict['embeddings'], list_of_texts,
                                  SelectExtractors._convert_cross_product_to_raw_list(cross_product), classifier_dict['name'])
                                if ranked_names:
                                    tmp[key] = ranked_names
                                else:
                                    print 'ranking of names did not work! Reverting to default...'
                                    tmp[key] = SelectExtractors._convert_cross_product(cross_product)
                    elif FieldIdentifiers.is_ethnicity(val):
                            list_of_texts = SelectExtractors.extract_list_of_texts(frame)
                            if list_of_texts:
                                ranked_ethnicities = rank.rank(classifier_dict['embeddings'], list_of_texts,
                                  SelectExtractors._convert_cross_product_to_raw_list(cross_product), classifier_dict['ethnicity'])
                                if ranked_ethnicities:
                                    tmp[key] = ranked_ethnicities
                                else:
                                    print 'ranking of ethnicities did not work! Reverting to default...'
                                    tmp[key] = SelectExtractors._convert_cross_product(cross_product)
                    else:
                        tmp[key] = SelectExtractors._convert_cross_product(cross_product)
                else:
                    tmp[key] = SelectExtractors._convert_cross_product(cross_product) # if we bypass Rahul's code
            answer.append(tmp)
        return answer

    @staticmethod
    def extract_list_of_texts(frame, text_props=['high_precision.description.result.value',
                'high_precision.readability.result.value', 'high_recall.readability.result.value',
                      'extracted_text']):
        """
        Designed for Rahul's classifier. Currently compatible with AdsTable-v3 fields. We convert text to lower
        case.
        :param frame:
        :param text_props:
        :return: A list of extracted texts (could be empty)
        """
        answer_list = list()
        for prop in text_props:
            tmp = list()
            if ResultExtractors.ResultExtractors.is_property_in_source_frame(frame['_source'], prop):
                tmp += ResultExtractors.ResultExtractors.get_property_from_source_frame(frame['_source'], prop)
            if not tmp:
                continue
            else:
                f = ''
                for t in tmp:
                    f += (t.lower()+'\n')
                answer_list.append(f)
        if answer_list:
            if len(answer_list) == 1:
                return answer_list
            else:
                for i in range(1, len(answer_list)):
                    answer_list[i] = (answer_list[i-1]+'\n'+answer_list[i])
                k = answer_list[-1]
                return [k]

        else:
            print 'no text props!'
            return answer_list

    @staticmethod
    def extract_locations(frame, classifier_dict=None):
        """
        Extract locations from the frame, using Majid's and possibly Rahul's code. This code is only compatible
        with MappingTale-AdsTable-v3. Change if necessary.

        Be wary of semantics. If I see high_precision city, I will only look for high_precision states
        and countries (not high_recall)
        :param frame:
        :return: A list of strings
        """
        # inferlink_flag = False
        precision_flag = False
        recall_flag = True
        city = list()
        state = list()
        country = list()

        # let's get city first.
        if ResultExtractors.ResultExtractors.is_property_in_source_frame(frame['_source'], 'inferlink_city.result.value'):
            city = ResultExtractors.ResultExtractors. \
                get_property_from_source_frame(frame['_source'], 'inferlink_city.result.value')
            # inferlink_flag = True
            precision_flag = True
        elif ResultExtractors.ResultExtractors.is_property_in_source_frame(frame['_source'], 'high_precision.city.result.value'):
            city = ResultExtractors.ResultExtractors. \
                get_property_from_source_frame(frame['_source'], 'high_precision.city.result.value')
            precision_flag = True
        else:
            if ResultExtractors.ResultExtractors.is_property_in_source_frame(frame['_source'],
                                                                          'high_recall.city.result.value'):
                city = ResultExtractors.ResultExtractors. \
                    get_property_from_source_frame(frame['_source'], 'high_recall.city.result.value')
            recall_flag = True

        # now we'll get state
        if precision_flag:
            if ResultExtractors.ResultExtractors.is_property_in_source_frame(frame['_source'],
                                                                             'high_precision.state.result.value'):
                state = ResultExtractors.ResultExtractors. \
                    get_property_from_source_frame(frame['_source'], 'high_precision.state.result.value')
        else:
            if ResultExtractors.ResultExtractors.is_property_in_source_frame(frame['_source'],
                                                                             'high_recall.state.result.value'):
                state = ResultExtractors.ResultExtractors. \
                    get_property_from_source_frame(frame['_source'], 'high_recall.state.result.value')

        # country
        if precision_flag:
            if ResultExtractors.ResultExtractors.is_property_in_source_frame(frame['_source'],
                                                                             'high_precision.country.result.value'):
                country = ResultExtractors.ResultExtractors. \
                    get_property_from_source_frame(frame['_source'], 'high_precision.country.result.value')
        else:
            if ResultExtractors.ResultExtractors.is_property_in_source_frame(frame['_source'],
                                                                             'high_recall.country.result.value'):
                country = ResultExtractors.ResultExtractors. \
                    get_property_from_source_frame(frame['_source'], 'high_recall.country.result.value')

        # call Rahul's code here and re-order strings if you have to.
        if city and classifier_dict and 'embeddings' in classifier_dict:
            list_of_texts = SelectExtractors.extract_list_of_texts(frame)
            if list_of_texts:
                from rankingExtractions import rank
                ranked_cities = rank.rank(classifier_dict['embeddings'], list_of_texts, city, classifier_dict['city'])
                if ranked_cities:
                    city = ranked_cities
                else:
                    print 'ranking of cities did not work! Reverting to default...'
        # not using Majid's code this time around.
        # try:
        #     # call Majid's code here and return the list of strings.
        #     print city
        #     print state
        #     print country
        #     locations = get_location(classifier_dict['city_dict'], city, state, country)
        #     print locations
        #     return locations
        # except:
        #     print 'Error in get location code! Returning cities only...'
        #     return city
        allowed = set(city)
        new_city = list()
        for element in city:
            if element in allowed:
                new_city.append(element)
                allowed.remove(element)
        # print new_city
        return new_city



    @staticmethod
    def extractCountSelect(grouped_frames, countSelectDict):
        """
        We don't do any post-processing on the groups at present (e.g. if field is MISSING_FIELD)
        :param grouped_frames: a dictionary where each key (the value of a 'grouped variable') maps to
         a list of (complete) frames. Typically output by a method in Grouper (e.g. standard_grouper)
        :param countSelectDict: the countSelectDict dictionary created as part of the output in
        SparqlTranslator._populate_list_set_data_structure
        :return: a dictionary with the same keys as grouped_frames, and the value as a
        dict that has count variables for keys and the actual count for values.
        """
        # at present, we throw an exception if countSelectDict contains any list values
        # with more than one element.

        if not countSelectDict:
            return None

        answer = {}

        for var, properties in countSelectDict.items():
            #if len(properties) != 1:
                countSelectDict[var] = SelectExtractors._prune_properties_set_to_singleton(properties)
                #string = var+" doesn't have exactly one mapped property in count-select!"
                #raise Exception(string)

        for key, group in grouped_frames.items():
            count_vars = {}
            SelectExtractors._init_count_vars(count_vars, countSelectDict)
            SelectExtractors._update_count_vars(count_vars, group)
            tmp = {}
            for k, v in count_vars.items():
                tmp[k] = v['count']
            answer[key] = tmp

        return answer

    @staticmethod
    def extractGroupConcatSelect(grouped_frames, groupConcatSelectDict):
        """

        :param grouped_frames: a dictionary where each key (the value of a 'grouped variable') maps to
         a list of (complete) frames
        :param groupConcatSelectDict: the groupConcatSelectDict dictionary created as part of the output in
        SparqlTranslator._populate_list_set_data_structure
        :return: a dictionary with the same keys as grouped_frames, and the value as a
        dict that has the group-concat variable for the key and the output string as value.
        """
        # at present, we throw an exception if groupConcatSelectDict contains any list values
        # with more than one element.

        if not groupConcatSelectDict:
            return None

        answer = {}
        #print groupConcatSelectDict
        for var, properties in groupConcatSelectDict.items():
            #if len(properties['properties']) != 1:
                properties['properties'] = SelectExtractors._prune_properties_set_to_singleton(properties['properties'])
                #string = var+" doesn't have exactly one mapped property in group-concat select!"
                #raise Exception(string)

        for key, group in grouped_frames.items():
            group_concat_vars = {}
            SelectExtractors._init_group_concat_vars(group_concat_vars, groupConcatSelectDict)
            SelectExtractors._update_group_concat_vars(group_concat_vars, group)
            tmp = {}
            for k, v in group_concat_vars.items():
                tmp[k] = v['group_concat_string']
            answer[key] = tmp

        return answer

    @DeprecationWarning
    @staticmethod
    def _prune_properties_set_to_singleton_old(properties_set):
        """
        This method is currently predicated on heuristics, and in support of count and group-concat
        :param properties_set: a non-empty set of properties
        :return: Another set of properties, one that is guaranteed to contain exactly 1 element. The
        set of properties returned will ALWAYS be a different reference than the one passed in, for safety.
        """
        if not properties_set:
            raise Exception('We have an empty/non-existent properties-set to prune')
        answer = set()
        p = list(properties_set)
        if len(p) == 1:
            answer.add(p[0])
            return answer
        else:
            # let the heuristics game begin
            for property in p:
                if 'raw' in property:
                    answer.add(property) # return first 'raw' field
                    return answer
            for property in p:  # no raw fields encountered
                if property == '_all' or '_text' in property:
                    continue    # the text fields are not supposed to be in here to begin with, but...
                else:
                    answer.add(property) # return first 'non-text' field
                    return answer

    @staticmethod
    def _prune_properties_set_to_singleton(properties_set):
        """
        This method calls _prune_property_set and returns the first element, since we know that
        _prune_property_set already does the sorting.
        :param properties_set: a non-empty set of properties
        :return: Another set of properties, one that is guaranteed to contain exactly 1 element. The
        set of properties returned will ALWAYS be a different reference than the one passed in, for safety.
        """
        if not properties_set:
            raise Exception('We have an empty/non-existent properties-set to prune')
        answer = set()
        p = list(properties_set)
        if len(p) == 1:
            answer.add(p[0])
            return answer
        else:
            answer_list = SelectExtractors._prune_property_set(properties_set)
            if answer_list:
                return set(answer_list[0])
            else:
                raise Exception('The pruned properties-set does not contain any elements')

    @staticmethod
    def _convert_cross_product(cross_product_list):
        """

        :param cross_product_list: List of flattened dictionaries
        :return: A list of strings. Each string is a space-delimited concatenation of the values in each dict.
        """
        answer = []
        for k in cross_product_list:
            for key, val in k.items():
               if isinstance(val, six.string_types):
                    k[key] = val
               else:
                    k[key] = str(val)
            answer.append(' '.join(k.values()))
        return list(set(answer))

    @staticmethod
    def _convert_cross_product_to_raw_list(cross_product_list):
        """

        :param cross_product_list: List of flattened dictionaries
        :return: A list of strings. Unlike _convert_cross_product, we do not do any space joining.
        """
        answer = []
        for k in cross_product_list:
            for key, val in k.items():
                if isinstance(val, six.string_types):
                    k[key] = val
                else:
                    k[key] = str(val)
            answer += k.values()
        return list(set(answer)) # order does not matter

    @staticmethod
    def _init_count_vars(count_vars, countSelectDict):
        """
        Modifies count_vars
        :param count_vars: an empty dictionary that will get initialized with all the information
        we need so that we can keep updating it as we encounter frames. It has the same keys
        as countSelectDict, and two 'inner' keys: 'properties' and 'count'.
        :param countSelectDict: the countSelectDict dictionary created as part of the output in
        SparqlTranslator._populate_list_set_data_structure
        :return: None
        """
        if count_vars:
            raise Exception('count_vars is not empty in SelectExtractors._init_count_vars')

        for var, properties in countSelectDict.items():
            tmp = {}
            tmp['properties'] = properties
            tmp['count'] = 0
            count_vars[var] = tmp

    @staticmethod
    def _init_group_concat_vars(group_concat_vars, groupConcatSelectDict):
        """
        Modifies group_concat_vars
        :param group_concat_vars: an empty dictionary that will get initialized with all the information
        we need so that we can keep updating it as we encounter frames. It has the same keys
        as groupConcatSelectDict, and four 'inner' keys: 'properties', 'group_concat_list',
        'separator' and 'distinct'. Later, when updating, 'group_concat_string' will be added.
        :param groupConcatSelectDict: the groupConcatSelectDict dictionary created as part of the output in
        SparqlTranslator._populate_list_set_data_structure
        :return: None
        """
        if group_concat_vars:
            raise Exception('group_concat_vars is not empty in SelectExtractors._init_group_concat_vars')

        for var, values in groupConcatSelectDict.items():
            tmp = {}
            tmp['properties'] = values['properties']
            tmp['group_concat_list'] = []
            tmp['separator'] = values['separator']
            tmp['distinct'] = values['distinct']
            group_concat_vars[var] = tmp

    @staticmethod
    def _update_group_concat_vars(group_concat_vars, group):
        """
        Modifies group_concat_vars. Be wary of semantics. If we encounter a list/string, we will append
        to ground_concat_list (original list structure gets merged).
        A new inner field called 'group_concat_string' will be added once we are finished processing a group.
        :param group_concat_vars: see _init_group_concat_vars for a description of the data structure
        :param group: a list of retrieved frames representing a group
        :return: None
        """
        for ds in group_concat_vars.itervalues():
            prop = list(ds['properties'])[0]  # this is a hacky line. It should be superseded,
                                        #since there could be more than one property
            for frame in group:
                if prop in frame:
                    tmp = []
                    if isinstance(frame[prop], list):
                        tmp += frame[prop]
                    elif isinstance(frame[prop], six.string_types):
                        tmp.append(frame[prop])
                    else:
                        raise Exception('Abnormal data type')
                    ds['group_concat_list'] += tmp
            if ds['distinct']:
                q = list(set(ds['group_concat_list']))
            else:
                q = ds['group_concat_list']
            ds['group_concat_string'] = ds['separator'].join(q)

    @staticmethod
    def _update_count_vars(count_vars, group):
        """
        Modifies count_vars. Be wary of semantics. The name_count is treated differently; otherwise
        for each occurrence .
        :param count_vars: see _init_count_vars for a description of the data structure
        :param group: a list of retrieved frames representing a group
        :return: None
        """
        for ds in count_vars.itervalues():
            prop = list(ds['properties'])[0]  # this is a hacky line. It should be superseded,
                                        #since there could be more than one property
            for frame in group:
                if prop in frame:
                    count = 1
                    if prop == 'name_count':    # for the 'number_of_individuals' case, an xsd:integer
                                                #change name_count to whatever 'number_...' is mapped to
                                                #in MappingTable
                        count = SelectExtractors._parseXSDIntegerLiteral(frame['name_count'])

                    ds['count'] += count

    @staticmethod
    def _parseXSDIntegerLiteral(str):
        """
        For parsing XSD integer literals to integer
        :param str: e.g. "\"5\"^^xsd:integer"
        :return: 5
        """
        l = (re.split("\^\^|\"", str))
        if l[3]=='xsd:integer':
            return int(l[1])


    @DeprecationWarning
    @staticmethod
    def _prune_property_set_old(set_of_properties):
        """
        The 'pruning' must be rule-based at the moment.
        :param set_of_properties:
        :return: A list of pruned properties
        """

        answer = []
        for property in set_of_properties:
            if property in ['high_precision.description.result.value','high_precision.readability.result.value',
                      'high_recall.readability.result.value',
                      'extracted_text', '_all']: # sync this list with text_props in MappingTable
                continue
            # elif 'raw' in property:
            #     continue
            # elif property == 'url' and 'telephone.name' in set_of_properties:
            #     continue
            else:
                answer.append(property)
        # print answer
        # if answer:
        #     answer.sort()
        return answer

    @staticmethod
    def _prune_property_set(set_of_properties):
        """
        The 'pruning' must be rule-based at the moment. We will also sort the properties in order of (desc.) priority
        :param set_of_properties:
        :return: A list of pruned properties
        """

        answer = []
        for property in set_of_properties: # remove text properties
            if property in ['high_precision.description.result.value', 'high_precision.readability.result.value',
                            'high_recall.readability.result.value',
                            'extracted_text', '_all']:  # sync this list with text_props in MappingTable
                continue
            # elif 'raw' in property:
            #     continue
            # elif property == 'url' and 'telephone.name' in set_of_properties:
            #     continue
            else:
                answer.append(property)
        # print answer
        if answer:

            return SelectExtractors._sort_property_list(answer)

    @staticmethod
    def _sort_property_list(list_of_props):
        """
        We'll give preference to inferlink properties if any, then high precision, finally high recall
        and anything else that comes after that.
        :param list_of_props:
        :return:
        """
        # print list_of_props
        priority_dict = dict()
        for prop in list_of_props:

            if 'inferlink' in prop:
                if 1 not in priority_dict:
                    priority_dict[1] = list()
                priority_dict[1].append(prop)
            elif 'precision' in prop:
                if 2 not in priority_dict:
                    priority_dict[2] = list()
                priority_dict[2].append(prop)
            elif 'recall' in prop:
                if 3 not in priority_dict:
                    priority_dict[3] = list()
                priority_dict[3].append(prop)
            else:
                if 4 not in priority_dict:
                    priority_dict[4] = list()
                priority_dict[4].append(prop)
        answer = list()
        keys = priority_dict.keys()
        keys.sort()
        for k in keys:
            if FieldIdentifiers.is_ethnicity(set(list_of_props)):   # we're making an exception for this one
                if k == 2:
                    priority_dict[k].sort(reverse=True)
                else:
                    priority_dict[k].sort()
            else:
                priority_dict[k].sort()
            answer += priority_dict[k]

        return answer


# properties = set(['inferlink_review-id.result.value',
#                                                 'high_precision.review-id.result.value.identifier',
#                                                 'high_precision.review-id.result.value.site',
#                                                 'high_recall.review-id.result.value.identifier',
#                                                 'high_recall.review-id.result.value.site'])
# print SelectExtractors._prune_property_set(properties)

