import Grouper, SelectExtractors, pprint, PrintUtils, re, math

class ResultExtractors:
    """
    Methods in here should be called from ExecuteESQueries after a query has been successfully
    processed and frames have been retrieved.
    """

    @staticmethod
    def standard_extractor(retrieved_frames, translated_query_data_structure, original_sparql_query, verbose = False,
                           classifier_dict=None):
        """

        :param retrieved_frames The frames as exactly retrieved by elasticsearch (not sub-indexed)
        :param translated_query_data_structure The data structure returned by the translated_ functions
        in SparqlTranslator
        :param original_sparql_query We use this for resolving a number of things, including the
        group-by variable
        :param verbose Only make True if you want to print out a lot of things.
        :param classifier_dict: a dictionary referencing rahul's classifiers. Should be passed into SelectExtractors
        :return: A list of results, where each result is a dictionary that contains the variables
        requested in the select. Each value in the dictionary is necessarily atomic. The list is ordered if
         there is an order-by in the groupByDict. If there is no limit, or the limit is smaller than
         |retrieved_frames| the output will be of size |retrieved_frames|, otherwise it has size limit.
        """


        group_by_var = ResultExtractors._find_group_by(original_sparql_query)
        order_by_var = ResultExtractors._find_order_by(original_sparql_query)
        order_by_properties = None  # a list of properties
        sort_order = None
        is_order_var_in_select = False
        flattened_list = None   #this is the list that must be processed for orders and limits
                                            #(but not optionals or filters)

        if translated_query_data_structure['groupByDict']:
                if 'order-variable' in translated_query_data_structure['groupByDict']: # any order-bys?
                    order_var_list = list(translated_query_data_structure['groupByDict']['order-variable'])
                    sort_order = translated_query_data_structure['groupByDict']['sorted-order']
                    if len(order_var_list) != 1:
                        order_var_list = SelectExtractors.SelectExtractors.\
                                _prune_property_set(translated_query_data_structure['groupByDict']['order-variable'])

                    order_by_properties = order_var_list

        # no grouping is required
        if translated_query_data_structure['simpleSelectDict'] and not group_by_var:
            # select is a list of dicts, with each dict being a variable (e.g. ?ethnicity) ref. a string value

            select = SelectExtractors.SelectExtractors.extractSimpleSelect(retrieved_frames['hits']['hits'],
                                                        translated_query_data_structure['simpleSelectDict'], classifier_dict)
            if order_by_var:
                for i in range(0, len(retrieved_frames['hits']['hits'])):
                    if order_by_var not in select[i]:
                        prop = []
                        for order_by_property in order_by_properties:
                            p = ResultExtractors.get_property_from_source_frame(
                                         retrieved_frames['hits']['hits'][i]['_source'],order_by_property)
                            if p is not None:
                                prop += p

                        if prop:
                            select[i][order_by_var] = prop
                    else:
                        is_order_var_in_select = True
            if verbose:
                ResultExtractors._printers(select = select)

            flattened_list = ResultExtractors._flatten(select)

        elif translated_query_data_structure['groupByDict'] \
         and 'group-variable' in translated_query_data_structure['groupByDict']:
            group = Grouper.Grouper.standard_grouper(retrieved_frames['hits']['hits'],
                                    list(translated_query_data_structure['groupByDict']['group-variable'])[0])
            if verbose:
                ResultExtractors._printers(group = group)

            unflattened_dict = dict()
            for key in group.keys():
                tmp = {}
                if group_by_var:
                    tmp[group_by_var] = key
                unflattened_dict[key] = tmp

            #process group-concat
            if translated_query_data_structure['groupConcatSelectDict']:
                groupConcatSelect= SelectExtractors.SelectExtractors.extractGroupConcatSelect(group,
                                                    translated_query_data_structure['groupConcatSelectDict'])
                if verbose:
                    ResultExtractors._printers(groupConcatSelect = groupConcatSelect)

                for key, value in groupConcatSelect.items():
                    unflattened_dict[key].update(value)


            #process count
            if translated_query_data_structure['countSelectDict']:
                countSelect= SelectExtractors.SelectExtractors.extractCountSelect(group,
                                                    translated_query_data_structure['countSelectDict'])
                if verbose:
                    ResultExtractors._printers(countSelect = countSelect)
                for key, value in countSelect.items():
                    unflattened_dict[key].update(value)
            if order_by_var:
                is_order_var_in_select = True

            flattened_list = ResultExtractors._flatten(unflattened_dict.values())

        if order_by_var:
            #print order_by_var
            flattened_list = ResultExtractors._order_flattened_list(flattened_list,order_by_var, sort_order)

            if not is_order_var_in_select: # this must be nested to distinguish between None and False
                for result in flattened_list:
                    if order_by_var in result:
                        del result[order_by_var]

        # a new addition, since we need scores in our list now
        scores = ResultExtractors.build_score_dict(retrieved_frames['hits']['hits'], 'cdr_id', None)
        # print len(flattened_list)
        # print scores
        for i in range(len(flattened_list)):
            id_val = flattened_list[i]['?ad'] # this is ad-hoc, but it should suffice.
            if id_val not in scores:
                raise Exception
            else:
                flattened_list[i]['score']=scores[id_val]
        flattened_list = ResultExtractors._limit_num_extractions_per_id(flattened_list, 50, '?ad')
        if translated_query_data_structure['groupByDict'] \
         and 'limit' in translated_query_data_structure['groupByDict']: # any limit?
            limit = translated_query_data_structure['groupByDict']['limit']
            if limit <= len(flattened_list):
                return flattened_list[0:limit]

        return flattened_list

    @staticmethod
    def _limit_num_extractions_per_id(flattened_list, extr_per_id=50, id_field='?ad'):
        """
        For each unique id, we want max. extr_per_id rows.
        :param flattened_list:
        :param extr_per_id: if None, we'll just return the original flattened list.
        :return: pruned list
        """
        if not extr_per_id:
            return flattened_list
        else:
            pruned_list = list()
            new_id = None
            count = 0
            # stop_adding = True
            for i in range(len(flattened_list)):
                # print count, i, flattened_list[i][id_field]

                if count >= extr_per_id:
                    if flattened_list[i][id_field] == new_id:
                        continue
                    else:
                        new_id = None
                        count = 0
                if not new_id:
                    new_id = flattened_list[i][id_field]
                    pruned_list.append(flattened_list[i])
                    count += 1
                else:
                    if flattened_list[i][id_field] == new_id:
                        pruned_list.append(flattened_list[i])
                        count += 1
                    else: # we're starting over
                        new_id = flattened_list[i][id_field]
                        pruned_list.append(flattened_list[i])
                        count = 1
        return pruned_list


    @staticmethod
    def build_score_dict(list_of_results, id_field, scoring_function=None):
        """

        :param list_of_results: should be retrieved_frames['hits']['hits']
        :param id_field: the (top-level) we will look for in '_source'. This will serve as key to the dictionary
        we return
        :param scoring_function: A function that will be called on the score, if it exists. e.g. sigmoid
        :return: A dictionary with id keys referencing score values
        """
        answer = dict()
        for result in list_of_results:
            if scoring_function:
                answer[result['_source'][id_field]] = scoring_function(result['_score'])
            else:
                answer[result['_source'][id_field]] = result['_score']
        return answer

    @staticmethod
    def sigmoid(x):
        return 1.0 / (1.0 + math.exp(-x))

    @DeprecationWarning
    @staticmethod
    def is_property_in_source_frame_old(source_frame, property):
        """
        In the frame, only one list is expected while navigating the property, if any. If a list is encountered,
        only one member needs to have the property
        :param source_frame: a source frame
        :param property: (possibly dot limited) property in our ontology
        :return: True or False
        """
        if '.' in property:
            l = re.split('\.',property)
            # print l
            tmp = source_frame
            for j in range(0,len(l)):
                element = l[j]
                if isinstance(tmp, list):
                    for i in range(0, len(tmp)):
                        k = tmp[i]

                        for m in range(j, len(l)):
                            inner_element = l[m]
                            if inner_element in k:
                                k = k[inner_element]
                            else:
                                k = None
                                break
                        if k:
                            return True
                    return False
                elif element not in tmp:
                    return False
                else:
                    tmp = tmp[element]
            if tmp:
                return True
        else:
            if property in source_frame:
                return True
        return False

    @staticmethod
    def is_property_in_source_frame(source_frame, property):
        """
        If lists are encountered while navigating the frame, only one member needs to have the property.
        :param source_frame: a source frame
        :param property: (possibly dot limited) property in our ontology
        :return: True or False
        """
        prop = re.split('\.', property)[0]

        if prop not in source_frame:
            return False
        inner_element = source_frame[prop]
        # either we've reached the end or we need to keep searching
        if len(re.split('\.', property)) == 1:
            return True
        rem_prop = '.'.join(re.split('\.', property)[1:])
        if type(inner_element) == dict:
            return ResultExtractors.is_property_in_source_frame(inner_element, rem_prop)
        elif type(inner_element) == list:
            for element in inner_element:
                if type(element) == dict and ResultExtractors.is_property_in_source_frame(element, rem_prop):
                    return True
        return False

    @staticmethod
    def get_property_from_source_frame(source_frame, property):
        """

        :param source_frame:
        :param property:
        :return: Always a list
        """
        prop = re.split('\.', property)[0]
        answer = list()
        if prop not in source_frame:
            return False
        inner_element = source_frame[prop]
        # either we've reached the end or we need to keep searching
        if len(re.split('\.', property)) == 1:
            if type(inner_element) == list:
                answer+=inner_element
            else:
                answer.append(inner_element)
            return answer

        rem_prop = '.'.join(re.split('\.', property)[1:])
        if type(inner_element) == dict:
            return ResultExtractors.get_property_from_source_frame(inner_element, rem_prop)
        elif type(inner_element) == list:
            for element in inner_element:
                if type(element) == dict and ResultExtractors.is_property_in_source_frame(element, rem_prop):
                    answer+=(ResultExtractors.get_property_from_source_frame(element, rem_prop))
        return answer

    @DeprecationWarning
    @staticmethod
    def get_property_from_source_frame_old(source_frame, property):
        """
        Be careful. The property could be dot-delimited.See my note on the 'is' version of this function.
        :param source_frame: A source frame
        :param property: a property in the source frame
        :return: Either the property value (always a list of homogeneous atomic values), or None if
        property doesn't exist in the frame
        """

        if '.' in property:
            l = re.split('\.',property)
            # print l
            tmp = source_frame
            for j in range(0,len(l)):
                element = l[j]
                if isinstance(tmp, list):
                    answer = []
                    for i in range(0, len(tmp)):
                        k = tmp[i]

                        for m in range(j, len(l)):
                            inner_element = l[m]
                            if inner_element in k:
                                k = k[inner_element]
                            else:
                                k = None
                                break
                        if k:
                            if isinstance(k, list):
                                answer += k
                            else:
                                answer.append(k)
                    if answer:
                        return answer
                    else:
                        return None
                elif element not in tmp:
                    return None
                else:
                    tmp = tmp[element]
            if tmp:
                if isinstance(tmp, list):
                    return tmp
                else:
                    return [tmp]

        else:
            if property in source_frame:
                if isinstance(source_frame[property], list):
                    return source_frame[property]
                else:
                    return [source_frame[property]]

        # I am backing up this piece of code in case things go seriously wrong with the modified code.
        # if '.' not in property:
        #     if property in source_frame:
        #         return source_frame[property]
        #     else:
        #         return None
        # else:
        #     l = re.split('\.',property)
        #     # print l
        #     tmp = source_frame
        #     for element in l:
        #         if element not in tmp:
        #             return None
        #         else:
        #             tmp = tmp[element]
        #     return tmp

    @staticmethod
    def _order_flattened_list(flattened_list, order_variable, sort_order):
        """

        At present, if order_variable is not there in an object in the flattened_list, that object will
        get pushed to the end. This helps us keep the semantics consistent (and the length of
        flattened_list is preserved). We break ties by placing lower-index items before higher-index items
        :param flattened_list: the unordered results-list
        :param order_variable: the variable by which to order the results-list
        :param sort_order: must be 'desc' or 'asc'
        :return: a sorted flattened list. Note that if no sorting took place, the reference to flattened list
        will simply be returned.
        """
        missing_indices = list() # this contains all indices that are missing order_property
        indices_dict = dict() # the key is order-variable value
        index = 0
        for result in flattened_list:
            if order_variable not in result:
                missing_indices.append(index)
            else:
                #print result
                if result[order_variable] not in indices_dict:
                    indices_dict[result[order_variable]] = list()
                indices_dict[result[order_variable]].append(index)
            index += 1
        if not indices_dict:
            return flattened_list
        if sort_order == 'asc':
            sort_keys = indices_dict.keys()
            sort_keys.sort()
        elif sort_order == 'desc':
            sort_keys = indices_dict.keys()
            sort_keys.sort(reverse = True)
        else:
            raise Exception('Unknown sort order')

        sort_indices = []
        for key in sort_keys:
            sort_indices += indices_dict[key]
        sort_indices += missing_indices

        return [flattened_list[i] for i in sort_indices]    # let's be pythonic for a change

    @staticmethod
    def _flatten(list_of_results):
        """
        Each inner list in list_of_results will be flattened into atomic values, so that the output list
        will only have atomic values.
        :param list_of_results: A list of dictionaries, where each value in the dictionary must reference an atomic
        value or a list (otherwise results are undefined)
        :return: A   flattened list (of dicts, with each dict consisting of atomic key-value pairs, a key
        representing a property)
        """
        output = []

        for element in list_of_results:
            output += ResultExtractors._flatten_dict(element)

        return output

    @staticmethod
    def _flatten_dict(dictionary):
        """
        E.g. if input were {'a' : [1,2,3], 'b': [4, 5], 'c': 6}, the output would be
        [{'a': 1, 'b':4, 'c':6},{'a': 2, 'b':5,'c':6},{'a': 3, 'b':4,'c':6},{'a': 1, 'b':5,'c':6},
        {'a':2 , 'b':4,'c':6},{'a': 3, 'b':5,'c':6}] This example is derived from an actual test case.
        :param dictionary: a dictionary with either atomic values or list values
        :return: A list. See description for example output
        """
        total = 1
        output = []
        atomic_dict = {}

        if not dictionary:
            return output

        for key, value in dictionary.items():
            if isinstance(value, list):
                if len(value) > 1:
                    total *= len(value)
                elif len(value) == 1:
                    atomic_dict[key] = value[0]
            else:
                atomic_dict[key] = value

        if total == 1:
            output.append(atomic_dict)
            return output

        for i in range(0, total):
            output.append(atomic_dict.copy())

        for key, value in dictionary.items():
            if isinstance(value, list):
                increment = len(value)
                for j in range(0, increment):
                    for i in range(j, total, increment):
                        output[i][key] = value[j]
        return output

    @staticmethod
    def _find_group_by(sparql_query):
        """
        The subtlety in this function is that it will return None if there is a group-by variable
        but we don't need it as part of the results. This function is for internal use only.
        :param sparql_query: The original sparql query, as produced by sqparser
        :return: a string representing the group-by variable (in original unmapped form e.g. '?ethnicity') or None
        """
        # for posterity, I'm changing sparql_query['parsed'] to sparql_query to be compatible with sqparser
        if 'group-by' in sparql_query and 'group-variable' in sparql_query['group-by']:
            var = sparql_query['group-by']['group-variable']
            for select in sparql_query['select']['variables']:
                if select['type'] == 'simple' and select['variable'] == var:
                    return var

        return None

    @staticmethod
    def _find_order_by(sparql_query):
        """
        Finds the order-by variable
        :param sparql_query: The original sparql query, as produced by sqparser
        :return: a string representing the order-by variable (in original unmapped form e.g. '?ethnicity') or None
        """
        # for posterity, I'm changing sparql_query['parsed'] to sparql_query to be compatible with sqparser
        if 'group-by' in sparql_query and 'order-variable' in sparql_query['group-by']:
            return sparql_query['group-by']['order-variable']
        return None

    @staticmethod
    def _printers(select = None, group = None, groupConcatSelect = None, countSelect = None):
        """
        if verbose is true in one of the extractors in this class, this method should be called
        :return:
        """
        pp = pprint.PrettyPrinter(indent=4)
        if select:

            print 'simple select extraction:'
            PrintUtils.PrintUtils.printExtractedSimpleSelect(select)

        if group:
            print 'group:'
            PrintUtils.PrintUtils.printGroupStatistics(group)

        if groupConcatSelect:
            print 'group concat select extraction:'
            pp.pprint(groupConcatSelect)

        if countSelect:
            print 'count select extraction:'
            pp.pprint(countSelect)

# print ResultExtractors.sigmoid(40.58288)
# source_frame = {'a':{'b':3, 'c':{'d':[{'e':[{'f':5}, 6, 8]}]}}, 'b':5}
# print ResultExtractors.get_property_from_source_frame(source_frame, 'a.c.d.e.f')
# test = {'a' : [1,2,3], 'b': [4, 5], 'c': 6}
# test1 = {'a' : [1,2]}
# print(ResultExtractors._flatten_dict(test1))
# property = 'a.b.c'
# prop = re.split('\.', property)[0]
# rem_prop = '.'.join(re.split('\.', property)[1:])
# print rem_prop