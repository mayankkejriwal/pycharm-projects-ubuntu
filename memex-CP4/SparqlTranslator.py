import codecs, json, TableFunctions, MappingTable, BuildCompoundESQueries
from elasticsearch import Elasticsearch

class SparqlTranslator:
    """
    Takes structured data structures representing various components of a sparql query and
    parses them into an elastic search query. Data structures may evolve and returned
    ES query are expected to become more complex as time goes on.
    """

    @staticmethod
    def translateQueries(sparqlDataStructure, mappingTableFile, conservativeLevel):
        """
        The function that should be called from ExecuteESQueries. Handles point fact, aggregate
        and cluster queries.
        :param sparqlDataStructure: Represents a point fact query (see Downloads/all-sparql-queries.txt for
        an example of the data structure)
        :param mappingTableFile: for now, the adsTable-v1.jl
        :param conservativeLevel: the lower levels are intended to be more conservative. the goal is
        that if level x (initialized to 0) returns no results, we go to level x+1
        :return:a dict with the 'query' field mapping to the elastic search query, and various dicts
        (possibly after some processing)
        """
        #for posterity removed 'parsed' from sparqlDataStructure
        if sparqlDataStructure['where']['type'].lower() == 'ad':
            return SparqlTranslator.translatePointFactAndAggregateQueries_v2(sparqlDataStructure,
                                                                             mappingTableFile, conservativeLevel)
        elif sparqlDataStructure['where']['type'].lower() == 'cluster':
            return SparqlTranslator.translateClusterQueries(sparqlDataStructure,
                                                            mappingTableFile, conservativeLevel)

    @staticmethod
    def translateToDisMaxQuery(sparqlDataStructure, mappingTableFile, isAggregate):
        """
        The function that should be called from ExecuteESQueries. Handles point fact, aggregate
        and cluster queries.
        :param sparqlDataStructure: Represents a point fact query (see Downloads/all-sparql-queries.txt for
        an example of the data structure)
        :param mappingTableFile: for now, the adsTable-v1.jl
        :param isAggregate: if True, the optional strategy is switched OFF (since for aggregate queries,
        we would want them to be); if False, we will use the strategy.

        :return:a dict with the 'query' field mapping to the elastic search query, and various dicts
        (possibly after some processing)
        """
        #for posterity removed 'parsed' from sparqlDataStructure
        if sparqlDataStructure['where']['type'].lower() == 'ad':
            level0DS = SparqlTranslator.translatePointFactAndAggregateQueries_v2(sparqlDataStructure,
                                                                             mappingTableFile, 0)
            level1query = SparqlTranslator.translatePointFactAndAggregateQueries_v2(sparqlDataStructure,
                                                                             mappingTableFile, 1)['query']
            if isAggregate:
                level0DS['query'] = BuildCompoundESQueries.BuildCompoundESQueries.build_dis_max_arbitrary(1.0,
                                                            0.0, level0DS['query'], level1query)
                return level0DS
            level2query = SparqlTranslator.translatePointFactAndAggregateQueries_v2(sparqlDataStructure,
                                                                             mappingTableFile, 2)['query']

            level0DS['query'] = BuildCompoundESQueries.BuildCompoundESQueries.build_dis_max_arbitrary(1.0,
                                                            0.0, level0DS['query'], level1query, level2query)
            return level0DS
        elif sparqlDataStructure['where']['type'].lower() == 'cluster':
            level0DS = SparqlTranslator.translateClusterQueries(sparqlDataStructure, mappingTableFile, 0)
            level1query = SparqlTranslator.translateClusterQueries(sparqlDataStructure,mappingTableFile, 1)['query']
            if isAggregate:
                level0DS['query'] = BuildCompoundESQueries.BuildCompoundESQueries.build_dis_max_arbitrary(1.0,
                                                            0.0, level0DS['query'], level1query)
                return level0DS
            level2query = SparqlTranslator.translateClusterQueries(sparqlDataStructure,mappingTableFile, 2)['query']

            level0DS['query'] = BuildCompoundESQueries.BuildCompoundESQueries.build_dis_max_arbitrary(1.0,
                                                            0.0, level0DS['query'], level1query, level2query)
            return level0DS

    @staticmethod
    def translateClusterQueries(sparqlDataStructure, mappingTableFile, conservativeLevel):
        """
        Handles cluster queries. First, we check for a seed constraint and run a simple bool query
        to return the list of seller uris. Then we form a new sparqlDataStructure using
        :param sparqlDataStructure: Represents a point fact query (see Downloads/all-sparql-queries.txt for
        an example of the data structure)
        :param mappingTableFile: for now, the adsTable-v1.jl
        :param conservativeLevel: the lower levels are intended to be more conservative. the goal is
        that if level x (initialized to 0) returns no results, we go to level x+1
        :return: a dict with the 'query' field mapping to the elastic search query, and various dicts
        (possibly after some processing)
        """
        seed_constraint = SparqlTranslator._find_seed_constraint(sparqlDataStructure)
        should = []
        should.append(TableFunctions.build_term_clause('telephone.name', seed_constraint))
        should.append(TableFunctions.build_term_clause('email.name', seed_constraint))

        #this is a very simple heuristic: use with caution. In essence, we strip the first 0 iff we're
        #reasonably sure this is not an email address
        if '@' not in seed_constraint:
            seed_constraint1 = SparqlTranslator._strip_initial_zeros(seed_constraint)
            if seed_constraint1 != seed_constraint:
                should.append(TableFunctions.build_term_clause('telephone.name', seed_constraint1))

        query = dict()
        query['query'] = BuildCompoundESQueries.BuildCompoundESQueries.build_bool_arbitrary(should = should)
        # print query
        index =  'dig-gt'
        url_localhost = "http://52.42.180.215:9200/"
        es = Elasticsearch(url_localhost)
        retrieved_frames = es.search(index= index, doc_type = 'seller', size = 10000, body = query) #we should set a big size
        #print(retrieved_frames['hits']['hits'][0]['_source'])
        translatedDS = SparqlTranslator.translatePointFactAndAggregateQueries_v2(sparqlDataStructure,
                                                                                 mappingTableFile, conservativeLevel)
        if retrieved_frames:
            seller_uris = SparqlTranslator._extract_seller_uris(retrieved_frames)
            seller_should = []
            for uri in seller_uris:
                seller_should.append(TableFunctions.build_term_clause('seller.uri', uri))
            seller_bool = BuildCompoundESQueries.BuildCompoundESQueries.build_bool_arbitrary(should = seller_should)
            seller_bool = BuildCompoundESQueries.BuildCompoundESQueries.build_bool_arbitrary(filter = [seller_bool])
            merge_bool = BuildCompoundESQueries.BuildCompoundESQueries.mergeBools(translatedDS['query'], seller_bool)
            translatedDS['query'] = merge_bool

        return translatedDS

    @staticmethod
    def translatePointFactAndAggregateQueries_v2(sparqlDataStructure, mappingTableFile, conservativeLevel):
        """
        see one of the _translatePointFactAndAggregateQueries_v2_levelx for the arguments explanations
        Use this for both point fact and aggregate queries.
        :param sparqlDataStructure:
        :param mappingTableFile:
        :param conservativeLevel: the lower levels are intended to be more conservative. the goal is
        that if level x (initialized to 0) returns no results, we go to level x+1
        :return:
        """
        if conservativeLevel == 0:
            return SparqlTranslator._translatePointFactAndAggregateQueries_v2_level0(sparqlDataStructure, mappingTableFile)
        elif conservativeLevel == 1:
            return SparqlTranslator._translatePointFactAndAggregateQueries_v2_level1(sparqlDataStructure, mappingTableFile)
        elif conservativeLevel == 2:
            return SparqlTranslator._translatePointFactAndAggregateQueries_v2_level2(sparqlDataStructure, mappingTableFile)

    @staticmethod
    def _extract_seller_uris(retrieved_frames):
        """

        :param retrieved_frames: The frames retrieved by running the first query as part of the cluster query
        execution strategy
        :return: a list of seller_uris
        """
        seller_uris = []
        for frame in retrieved_frames['hits']['hits']:
            seller_uris.append(frame['_source']['uri'])

        return seller_uris

    @staticmethod
    def _find_seed_constraint(sparqlDataStructure):
        """
        This function should only be called from translateClusterQueries
        :param sparqlDataStructure: The original sparql query
        :return: The seed constraint. If no seed is found, an exception will be thrown.
        """
        # remove 'parsed'
        for clause in sparqlDataStructure['where']['clauses']:
            if 'predicate' not in clause:
                continue
            elif clause['predicate'] == 'seed':
                return clause['constraint']
        raise Exception('No seed found')

    @staticmethod
    def _translatePointFactAndAggregateQueries_v2_level0(sparqlDataStructure, mappingTableFile):
        """
        Compared to v1, v2 can handle 'exists' style queries. That is, if a variable is bound to a property
        and mentioned in clauses, it will have an impact, whereas it was ignored in v1.

        Any code that is intended to be 'common' across levels has been moved to protected methods.
        Code that could change across levels stays in the main body, even if at present it is
        identical in all methods.

        :param sparqlDataStructure: Represents a point fact query (see Downloads/all-sparql-queries.txt for
        an example of the data structure)
        :param mappingTableFile: for now, the adsTable-v1.jl
        :return:a dict with the 'query' field mapping to the elastic search query, and various dicts
        (possibly after some processing)
        """
        initialDS= SparqlTranslator._populate_list_set_data_structure(sparqlDataStructure, mappingTableFile)
        # at present, do not distinguish between optional and nonoptional properties.
        propertiesSet = initialDS['optionalPropertiesSet'].union(initialDS['nonOptionalPropertiesSet'])
        for pr in propertiesSet:
            initialDS['existsList'].append(TableFunctions.build_exists_clause(pr))
        # a key difference between level 0 and level 1
        constant_score_clauses = BuildCompoundESQueries.BuildCompoundESQueries.build_constant_score_filters(
            initialDS['existsList'])
        innerOuterDS = SparqlTranslator._populate_inner_outer_data_structure(initialDS)
        if innerOuterDS['innerNonOptional']:
            innerOuterDS['outerNonOptional'].append(BuildCompoundESQueries.BuildCompoundESQueries.
                              build_bool_arbitrary(should = innerOuterDS['innerNonOptional']))
        if innerOuterDS['innerOptional']:
            innerOuterDS['outerOptional'].append(BuildCompoundESQueries.BuildCompoundESQueries.
                                 build_bool_arbitrary(should = innerOuterDS['innerOptional']))

        # right now, no distinction between nonoptional and optional triples.
        #print initialDS['filterQueries']
        where_bool = BuildCompoundESQueries.BuildCompoundESQueries.\
            build_bool_arbitrary(should = innerOuterDS['outerNonOptional'], filter = initialDS['filterQueries'])
        must_bool = BuildCompoundESQueries.BuildCompoundESQueries.\
            build_bool_arbitrary(must = [where_bool])
        optional_bool = BuildCompoundESQueries.BuildCompoundESQueries.\
            build_bool_arbitrary(should = innerOuterDS['outerOptional'], filter = constant_score_clauses)
        merge_bool = BuildCompoundESQueries.BuildCompoundESQueries.\
            mergeBools(where_bool, optional_bool, must_bool) #We want to ensure we have all the bindings we need;
                                                             #hence, the must_bool.

        #postprocess initialDS
        SparqlTranslator._postprocess_initialDS_v1(initialDS)

        answer = {}
        answer['query'] = merge_bool
        answer['simpleSelectDict'] = initialDS['simpleSelectDict']
        answer['groupByDict'] = initialDS['groupByDict']
        answer['countSelectDict'] = initialDS['countSelectDict']
        answer['groupConcatSelectDict'] = initialDS['groupConcatSelectDict']
        return answer

    @staticmethod
    def _translatePointFactAndAggregateQueries_v2_level1(sparqlDataStructure, mappingTableFile):
        """
        Compared to v1, v2 can handle 'exists' style queries. That is, if a variable is bound to a property
        and mentioned in clauses, it will have an impact, whereas it was ignored in v1.

        Any code that is intended to be 'common' across levels has been moved to protected methods.
        Code that could change across levels stays in the main body, even if at present it is
        identical in all methods.
        :param sparqlDataStructure: Represents a point fact query (see Downloads/all-sparql-queries.txt for
        an example of the data structure)
        :param mappingTableFile: for now, the adsTable-v1.jl
        :return: a dict with the 'query' field mapping to the elastic search query, and various dicts
        (after some processing)
        """
        initialDS= SparqlTranslator._populate_list_set_data_structure(sparqlDataStructure, mappingTableFile)
        # do we distinguish between optional and nonoptional properties in terms of exists clauses?
        propertiesSet = initialDS['optionalPropertiesSet'].union(initialDS['nonOptionalPropertiesSet'])

        for pr in propertiesSet:
            initialDS['existsList'].append(TableFunctions.build_exists_clause(pr))
        filter_clauses = [BuildCompoundESQueries.BuildCompoundESQueries.build_bool_arbitrary(
                should = initialDS['existsList'])]
        innerOuterDS = SparqlTranslator._populate_inner_outer_data_structure(initialDS)
        if innerOuterDS['innerNonOptional']:
            innerOuterDS['outerNonOptional'].append(BuildCompoundESQueries.BuildCompoundESQueries.
                              build_bool_arbitrary(should = innerOuterDS['innerNonOptional']))
        if innerOuterDS['innerOptional']:
            innerOuterDS['outerOptional'].append(BuildCompoundESQueries.BuildCompoundESQueries.
                                 build_bool_arbitrary(should = innerOuterDS['innerOptional']))
        # right now, no distinction between nonoptional and optional triples.
        where_bool = BuildCompoundESQueries.BuildCompoundESQueries.\
            build_bool_arbitrary(should = innerOuterDS['outerNonOptional'], filter = initialDS['filterQueries'])
        must_bool = BuildCompoundESQueries.BuildCompoundESQueries.\
            build_bool_arbitrary(must = [where_bool])
        optional_bool = BuildCompoundESQueries.BuildCompoundESQueries.\
            build_bool_arbitrary(should = innerOuterDS['outerOptional'], filter = filter_clauses)
        #We want to ensure we have all the bindings we need; hence, the must_bool.
        merge_bool = BuildCompoundESQueries.BuildCompoundESQueries.\
            mergeBools(where_bool, optional_bool, must_bool, initialDS['bindQuery'])

        #postprocess initialDS
        SparqlTranslator._postprocess_initialDS_v1(initialDS)

        #populate answer data structure
        answer = {}
        answer['query'] = merge_bool
        answer['simpleSelectDict'] = initialDS['simpleSelectDict']
        answer['groupByDict'] = initialDS['groupByDict']
        answer['countSelectDict'] = initialDS['countSelectDict']
        answer['groupConcatSelectDict'] = initialDS['groupConcatSelectDict']
        return answer

    @staticmethod
    def _translatePointFactAndAggregateQueries_v2_level2(sparqlDataStructure, mappingTableFile):
        """
        Level 2 does the same thing as level 1 except we make all variables except ad and cluster
        optional. This is so we are not prevented from returning a relevant answer even if
        some bindings are missing. There's just a one-line difference technically
        :param sparqlDataStructure: Represents a point fact query (see Downloads/all-sparql-queries.txt for
        an example of the data structure)
        :param mappingTableFile: for now, the adsTable-v1.jl
        :return: a dict with the 'query' field mapping to the elastic search query, and various dicts
        (after some processing)
        """
        SparqlTranslator._turn_on_optional_variables(sparqlDataStructure) # diff. between level 1 and 2
        initialDS= SparqlTranslator._populate_list_set_data_structure(sparqlDataStructure, mappingTableFile)
        # do we distinguish between optional and nonoptional properties in terms of exists clauses?
        propertiesSet = initialDS['optionalPropertiesSet'].union(initialDS['nonOptionalPropertiesSet'])

        for pr in propertiesSet:
            initialDS['existsList'].append(TableFunctions.build_exists_clause(pr))
        filter_clauses = [BuildCompoundESQueries.BuildCompoundESQueries.build_bool_arbitrary(
                should = initialDS['existsList'])]
        innerOuterDS = SparqlTranslator._populate_inner_outer_data_structure(initialDS)
        if innerOuterDS['innerNonOptional']:
            innerOuterDS['outerNonOptional'].append(BuildCompoundESQueries.BuildCompoundESQueries.
                              build_bool_arbitrary(should = innerOuterDS['innerNonOptional']))
        if innerOuterDS['innerOptional']:
            innerOuterDS['outerOptional'].append(BuildCompoundESQueries.BuildCompoundESQueries.
                                 build_bool_arbitrary(should = innerOuterDS['innerOptional']))
        # right now, no distinction between nonoptional and optional triples.
        where_bool = BuildCompoundESQueries.BuildCompoundESQueries.\
            build_bool_arbitrary(should = innerOuterDS['outerNonOptional'], filter = initialDS['filterQueries'])
        must_bool = BuildCompoundESQueries.BuildCompoundESQueries.\
            build_bool_arbitrary(must = [where_bool])
        optional_bool = BuildCompoundESQueries.BuildCompoundESQueries.\
            build_bool_arbitrary(should = innerOuterDS['outerOptional'], filter = filter_clauses)
        #We want to ensure we have all the bindings we need; hence, the must_bool.
        merge_bool = BuildCompoundESQueries.BuildCompoundESQueries.\
            mergeBools(where_bool, optional_bool, must_bool, initialDS['bindQuery'])

        #postprocess initialDS
        SparqlTranslator._postprocess_initialDS_v1(initialDS)

        #populate answer data structure
        answer = {}
        answer['query'] = merge_bool
        answer['simpleSelectDict'] = initialDS['simpleSelectDict']
        answer['groupByDict'] = initialDS['groupByDict']
        answer['countSelectDict'] = initialDS['countSelectDict']
        answer['groupConcatSelectDict'] = initialDS['groupConcatSelectDict']
        return answer

    @staticmethod
    def _postprocess_initialDS_v1(initialDS):
        """
        Modifies initialDS. Right now, our strategies are simple (e.g. process
        :param initialDS: As returned by _populate_list_set_data_structure
        :return: None
        """
        # process simpleSelectDict, groupByDict (remove readability_text). In future, we may want to be
        # more ambitious with how we process simpleSelectDict
        for properties in initialDS['simpleSelectDict'].itervalues():
            properties.discard('readability_text')
            properties.discard('_all')
        if 'group-variable' in initialDS['groupByDict']:
            initialDS['groupByDict']['group-variable'].discard('readability_text')
            initialDS['groupByDict']['group-variable'].discard('_all')
        if 'order-variable' in initialDS['groupByDict']:
            initialDS['groupByDict']['order-variable'].discard('readability_text')
            initialDS['groupByDict']['order-variable'].discard('_all')

    @staticmethod
    def _populate_inner_outer_data_structure(initialDS):
        """

        :return: a dictionary with the following fields: outerOptional, innerOptional, outerNonOptional,
        innerNonOptional
        """

        outerWhere = []
        innerWhere = []
        outerOptional = []
        innerOptional = []

        for match in initialDS['nonOptionalList']:
            if 'meta' in match:
                if 'inner' in match['meta']:

                    del(match['meta'])

                    innerWhere.append(match)
            else:
                outerWhere.append(match)

        for match in initialDS['optionalList']:
            if 'meta' in match:
                if 'inner' in match['meta']:
                    del(match['meta'])

                    innerOptional.append(match)
            else:
                outerOptional.append(match)

        answer = {}
        answer['outerNonOptional'] = outerWhere
        answer['innerNonOptional'] = innerWhere
        answer['outerOptional'] = outerOptional
        answer['innerOptional'] = innerOptional

        return answer

    @staticmethod
    def _populate_list_set_data_structure(sparqlDataStructure, mappingTableFile):
        """

        :return: A dictionary with the following fields: existsList, nonOptionalPropertiesSet,
        optionalPropertiesSet, optionalList, nonoptionalList, simpleSelectDict, countSelectDict,
        groupConcatSelectDict, groupByDict, filterQueries, bindQuery

        Each field (with variable as key)
        in {simple, count}SelectDict maps to a set of the 'properties' that the variable
        maps to. The class variable (e.g. Ad), if asked for, will be mapped to a singleton
        set containing 'identifier'

        groupConcatSelectDict has three inner fields: 'properties' maps to a set of the 'properties' that the variable
        maps to, while 'distinct' and 'separator' are carried over from the original data structure. The key
        in the groupConcatSelectDict is the original dependent-variable

        groupByDict, at present, only contains a 'group-variable' field, mapped to the set of our properties.

        filterQueries is a list of boolean queries that should be embedded in a filter query eventually.

        bindQuery is a bool query with filters. It must be satisfied for the non-optional predicates
        in select to bind.

        We do not purge any text variables (e.g. readability_text) from any of the sets.
        """
        mappingTable = MappingTable.MappingTable.readMappingTable(mappingTableFile)
        optionalTriples = []
        whereTriples = [] #non-optional
        filterQueries = []  #a list of bool queries. They must be (event.) embedded in a bool filter
        var_to_property = {}
        var_to_property_optional = {}
        simpleSelectDict = {}
        countSelectDict = {}
        groupConcatSelectDict = {}
        groupByDict = {}
        type_var = None
        #remove 'parsed'
        if 'type' in sparqlDataStructure['where'] and 'variable' in sparqlDataStructure['where']:
                type_var = sparqlDataStructure['where']['variable']
                if sparqlDataStructure['where']['type'].lower() == 'ad':
                    var_to_property[type_var] = ['identifier']
                elif sparqlDataStructure['where']['type'].lower() == 'cluster':
                    var_to_property[type_var] = ['seller.uri']

        for clause in sparqlDataStructure['where']['clauses']:
            tmp = []
            if 'variable' in clause:

                l = list(mappingTable[clause['predicate']])
                #print(l)
                properties = []
                for d in l:
                    for el in list(d):
                        properties.append(el)
                if 'isOptional' in clause and clause['isOptional']:
                    var_to_property_optional[clause['variable']] = properties
                else:
                    var_to_property[clause['variable']] = properties
            else:
                tmp.append('subject')
                tmp.append(clause['predicate'])
                tmp.append(clause['constraint'])

                if 'isOptional' in clause and clause['isOptional']:
                    optionalTriples.append(tmp)
                else:
                    whereTriples.append(tmp)

        if 'filters' in sparqlDataStructure['where']:
            for clause_expr in sparqlDataStructure['where']['filters']:
                filterQueries.append(SparqlTranslator._translateFilterClauseToBool(clause_expr,
                                                var_to_property, var_to_property_optional))

        #print var_to_property
        nonOptionalList = SparqlTranslator._translateTriplesToList(whereTriples, mappingTableFile)
        optionalList = SparqlTranslator._translateTriplesToList(optionalTriples, mappingTableFile)
        existsList = []
        nonOptionalPropertiesSet = set()
        optionalPropertiesSet = set()
        for list_of_properties in var_to_property.itervalues():
            for v in list_of_properties:
                nonOptionalPropertiesSet.add(v)

        for list_of_properties in var_to_property_optional.itervalues():
            for v in list_of_properties:
                optionalPropertiesSet.add(v)

        if 'group-by' in sparqlDataStructure:
            if 'group-variable' in sparqlDataStructure['group-by']:
                var = sparqlDataStructure['group-by']['group-variable']
                if var in var_to_property:
                    groupByDict['group-variable'] = set(var_to_property[var])
                elif var in var_to_property_optional:
                    groupByDict['group-variable'] = set(var_to_property_optional[var])
                elif var == type_var:
                    groupByDict['group-variable'] = set(['identifier'])
                else:
                    raise Exception('Unmapped group-variable in group-by')
            if 'order-variable' in sparqlDataStructure['group-by']:
                var = sparqlDataStructure['group-by']['order-variable']
                if var in var_to_property:
                    groupByDict['order-variable'] = set(var_to_property[var])
                elif var in var_to_property_optional:
                    groupByDict['order-variable'] = set(var_to_property_optional[var])
                elif var == type_var:
                    groupByDict['order-variable'] = set(['identifier'])
                elif SparqlTranslator._is_dependent_variable(var, sparqlDataStructure):
                    groupByDict['order-variable'] = set([var])
                else:
                    raise Exception('Unmapped order-variable in group-by')
                #we should only check for limit if there's an order-variable
                if 'limit' in sparqlDataStructure['group-by'] and sparqlDataStructure['group-by']['limit'] >= 1:
                    groupByDict['limit'] = sparqlDataStructure['group-by']['limit']
                #sorted order will always be there if order-variable is there.
                if 'sorted-order' in sparqlDataStructure['group-by']:
                    groupByDict['sorted-order'] = sparqlDataStructure['group-by']['sorted-order']
                else:
                    groupByDict['sorted-order'] = 'asc' #the default, if nothing is specified

        for vars in sparqlDataStructure['select']['variables']:
            if vars['type'] == 'group-concat':
                var = vars['variable']
                dependent_var = vars['dependent-variable']
                tmp = {}
                if var in var_to_property:
                    tmp['properties'] = set(var_to_property[var])
                elif var in var_to_property_optional:
                    tmp['properties'] = set(var_to_property_optional[var])
                elif var == type_var:
                    tmp['properties'] = set(['identifier'])
                else:
                    raise Exception(var+' :Unmapped variable in group-concat select')
                tmp['distinct'] = vars['distinct']
                tmp['separator'] = vars['separator']
                groupConcatSelectDict[dependent_var] = tmp
            elif vars['type'] == 'count':
                var = vars['variable']
                dependent_var = vars['dependent-variable']
                if var in var_to_property:
                    countSelectDict[dependent_var] = set(var_to_property[var])
                elif var in var_to_property_optional:
                    countSelectDict[dependent_var] = set(var_to_property_optional[var])
                elif var == type_var:
                    countSelectDict[dependent_var] = set(['identifier'])
                else:
                    raise Exception(var+' :Unmapped variable in count select')
            elif vars['type'] == 'simple':
                var = vars['variable']
                if var in var_to_property:
                    simpleSelectDict[var] = set(var_to_property[var])
                elif var in var_to_property_optional:
                    simpleSelectDict[var] = set(var_to_property_optional[var])
                elif var == type_var:
                    simpleSelectDict[var] = set(['identifier'])
                else:
                    raise Exception(var+' :Unmapped variable in simple select')
            else:
                raise Exception('Bad select type')


        answer = {}
        answer['existsList'] = existsList
        answer['nonOptionalPropertiesSet'] = nonOptionalPropertiesSet
        answer['optionalPropertiesSet'] = optionalPropertiesSet
        answer['optionalList'] = optionalList
        answer['nonOptionalList'] = nonOptionalList
        answer['simpleSelectDict'] = simpleSelectDict
        answer['countSelectDict'] = countSelectDict
        answer['groupConcatSelectDict'] = groupConcatSelectDict
        answer['groupByDict'] = groupByDict
        answer['filterQueries'] = filterQueries
        answer['bindQuery'] = SparqlTranslator._computeBindFilter(sparqlDataStructure['select']['variables'],
                                                                  var_to_property)
        return answer

    @staticmethod
    def _is_dependent_variable(var, sparqlDataStructure):
        """

        :param var: some variable e.g. ?ads
        :param sparqlDataStructure: checks if ?ads is a dependent-variable in the select clause
        :return: True or False
        """
        # remove 'parsed' from sparqlDataStructure
        for vars in sparqlDataStructure['select']['variables']:
            if 'dependent-variable' in vars:
                if vars['dependent-variable'] == var:
                    return True
        return False

    @staticmethod
    def _translateTriplesToList(triples, mappingTableFile):
        """
        

        Attributes:
            whereTriples: a list, where each element is a list of three strings. Each inner list
                represents a triple. The second element always has a special meaning, and will be mapped.
                At present, we ignore the first element. If the third element
                begins with a question mark, it is a variable (and at present, we ignore it) 
                otherwise it is a string
                that will go into the elasticsearch query.
            mappingTable: This is a mapping table file. 
            
        Returns:
                The list
        """
        mappingTable = MappingTable.MappingTable.readMappingTable(mappingTableFile)
        list = []
        for triple in triples:
            if triple[1] not in mappingTable:
                raise Exception('Error! Unmapped Ontology Property: ',triple[1])
            elif triple[2][0] == '?':
                continue
            else:
                for mapping in mappingTable[triple[1]]:
                    for k, v in mapping.items():
                        list.append(getattr(TableFunctions, v)(k, triple[2]))
        return list

    @staticmethod
    def _mapVarsToProperties(variable, *args):
        """

       :param variable: a variable string
       :param *args: Each arg is a dictionary mapping variables to properties in our ontology

        :return: a set of properties in our ontology that the variable maps to
        """
        properties = set()
        for var_to_property in args:
            if variable in var_to_property:
                properties = properties.union(var_to_property[variable])

        return properties

    @staticmethod
    def _useFilterTripleToFormShouldQuery(tripleDict, var_to_property, var_to_property_optional):
        """
        Modifies should. called by _translateFilterClauseToBool.
        :param tripleDict: A filter triple dictionary
        :param should: a list
        :return: a bool query with only 'should' non-empty
        """
        should = []
        if 'operator' in tripleDict:    #we have a 'constraint' triple
                properties = SparqlTranslator._mapVarsToProperties(tripleDict['variable'], var_to_property,
                                                                  var_to_property_optional)
                if not properties:
                    raise Exception('No mapping for variable in filter!')
                operator = tripleDict['operator']
                for property in properties:
                    if operator == '>' or operator == '>=' or operator == '<=' or operator == '<':
                        should.append(
                            BuildCompoundESQueries.BuildCompoundESQueries.build_bool_arbitrary
                            (must = [TableFunctions.build_range_clause(property, operator, tripleDict['constraint'])]))
                    elif operator == '=':
                        should.append(
                            BuildCompoundESQueries.BuildCompoundESQueries.build_bool_arbitrary
                        (must = [TableFunctions.build_match_clause(property, tripleDict['constraint'])]))
                    elif operator == '!=':
                        should.append(
                            BuildCompoundESQueries.BuildCompoundESQueries.build_bool_arbitrary
                            (must_not = [TableFunctions.build_match_clause(property, tripleDict['constraint'])]))
        else:
                properties = SparqlTranslator._mapVarsToProperties(tripleDict['bind'], var_to_property,
                                                                  var_to_property_optional)
                if not properties:
                    raise Exception('No mapping for variable in filter!')
                for property in properties:
                    should.append(
                            BuildCompoundESQueries.BuildCompoundESQueries.build_bool_arbitrary
                            (must = [TableFunctions.build_exists_clause(property)]))
        return BuildCompoundESQueries.BuildCompoundESQueries.build_bool_arbitrary(should = should)

    @staticmethod
    def _computeBindFilter(select_clauses,var_to_property):
        """
        This function guarantees that any query result retrieved will contain the 'select' bindings we want.
        The output is a filter-bool that should be merged into other bools. A query strategy function
        should call this directly.

        :param select: A list of dictionaries corresponding to select clauses
        :param var_to_property: A dictionary mapping variables to properties in our ontology

        :return: A bool-filter query
        """
        filter = []
        for clause in select_clauses:
            properties = SparqlTranslator._mapVarsToProperties(clause['variable'],var_to_property)
            properties.discard('readability_text')
            should = [] #careful; this is not the 'should' in a bool
            for property in properties:
               should.append(TableFunctions.build_exists_clause(property))
            if should:
                filter.append(BuildCompoundESQueries.BuildCompoundESQueries.build_bool_arbitrary(filter = should))
        return BuildCompoundESQueries.BuildCompoundESQueries.build_bool_arbitrary(filter = filter)

    @staticmethod
    def _strip_initial_zeros(string):
        count = 0
        for i in range(0, len(string)):
            if string[i] == '0':
                count += 1
            else:
                break
        return string[count:]

    @staticmethod
    def _translateFilterClauseToBool(filterClause, var_to_property, var_to_property_optional):
        """


        Attributes:
            :param filterClause: a clause which either contains one of the following:
                (1) a 'constraint' triple, e.g. ?nationality = 'Indian'
                (2) a 'bind' triple e.g. ?nationality
                Or a combination of the above using boolean operators ('or' and 'and')
            :param var_to_property: A dictionary mapping variables to properties in our ontology
            :param var_to_property_optional: same as the above. Because of the way we set up these dictionaries
            we treat them as two separate arguments for convenience

        Returns:
                A bool query (WITHOUT filters or must_nots) that should be nested AS a filter in another bool
        """

        #There will be no filters
        if len(filterClause)< 2 and 'operator' in filterClause:
            raise Exception('Operator present in filter clause with fewer than 2 clauses')

        queries = []
        for tripleDict in filterClause['clauses']:
            queries.append(SparqlTranslator._useFilterTripleToFormShouldQuery(tripleDict, var_to_property,
                                                                              var_to_property_optional))
        #print queries
        if 'operator' not in filterClause:
            return queries[0]
        elif filterClause['operator'][0] == 'and':
            return BuildCompoundESQueries.BuildCompoundESQueries.build_bool_arbitrary(must = queries)
        elif filterClause['operator'][0] == 'or':
            return BuildCompoundESQueries.BuildCompoundESQueries.build_bool_arbitrary(should = queries)

    @staticmethod
    def _turn_on_optional_variables(sparqlDataStructure):
        """
        Modifies sparqlDataStructure. Systematically goes through the where clause, and whenever it sees
        a 'variable' key, will turn
        :param sparqlDataStructure:
        :return:
        """
        for dictionary in sparqlDataStructure['where']['clauses']:
            if 'variable' in dictionary and dictionary['predicate'] != 'ad':
                #print dictionary
                dictionary['isOptional'] = True

    @DeprecationWarning
    @staticmethod
    def translateReadabilityTextToQuery(whereTriples, mappingTableFile):
        """
        Designed for the case where we only have where triples and all
        mappings are to readability or inferlink text fields. Used for
        executing Pedro's queries.
        """
        mappingTable = MappingTable.MappingTable.readMappingTable(mappingTableFile)
        should = SparqlTranslator._translateTriplesToList(whereTriples, mappingTableFile)
        return BuildCompoundESQueries.BuildCompoundESQueries.build_bool_arbitrary(should = should)

    @DeprecationWarning
    @staticmethod
    def translateFilterWhereOptionalToBool(whereTriples, filterTriples, optionalTriples, mappingTableFile):
        """
        Typical usage is for the offer_table.jl. Trial only!
        """
        mappingTable = MappingTable.MappingTable.readMappingTable(mappingTableFile)
        must = SparqlTranslator._translateTriplesToList(whereTriples, mappingTableFile)
        should = None
        if optionalTriples:
            should = SparqlTranslator._translateTriplesToList(optionalTriples, mappingTableFile)

        if not filterTriples:
            return BuildCompoundESQueries.BuildCompoundESQueries.build_bool_arbitrary(must = must, should = should)

        filter_bool = []
        filter_bool_must= []
        filter_bool_must_not = []
        var_to_property = {}
        for triple in whereTriples:
            if triple[2][0] == '?':
                l = list(mappingTable[triple[1]])
                #print(l)
                properties = []
                for d in l:
                    for el in list(d):
                        properties.append(el)
                var_to_property[triple[2]] = properties

        for triple in optionalTriples:
            if triple[2][0] == '?':
                l = list(mappingTable[triple[1]])
                #print(l)
                properties = []
                for d in l:
                    for el in list(d):
                        properties.append(el)
                var_to_property[triple[2]] = properties
        #print(var_to_property)
        for triple in filterTriples:
            if triple[0][0] == '?':
                for property in var_to_property[triple[0]]:
                    if triple[1] == '>' or triple[1] == '>=' or triple[1] == '<=' or triple[1] == '<':
                        filter_bool.append(TableFunctions.build_range_clause(property, triple[1], triple[2]))
                    elif triple[1] == '==':
                        filter_bool_must.append(TableFunctions.build_match_clause(property, triple[2]))
                    elif triple[1] == '!=':
                        filter_bool_must_not.append(TableFunctions.build_match_clause(property, triple[2]))
        filter_bool.append(BuildCompoundESQueries.BuildCompoundESQueries.build_bool_arbitrary(must = filter_bool_must,
                                                                            must_not = filter_bool_must_not))
        return BuildCompoundESQueries.BuildCompoundESQueries.build_bool_arbitrary(must = must,
                                                                                  should = should,
                                                                                  filter = filter_bool)

    @DeprecationWarning
    @staticmethod
    def translateFilterWhereToBool(whereTriples, filterTriples, mappingTableFile):
        """

        """
        mappingTable = MappingTable.MappingTable.readMappingTable(mappingTableFile)
        must = SparqlTranslator._translateTriplesToList(whereTriples, mappingTableFile)

        if not filterTriples:
            return BuildCompoundESQueries.BuildCompoundESQueries.build_bool_arbitrary(must = must)

        filter_bool = []
        filter_bool_must= []
        filter_bool_must_not = []
        var_to_property = {}
        for triple in whereTriples:
            if triple[2][0] == '?':
                l = list(mappingTable[triple[1]])
                #print(l)
                properties = []
                for d in l:
                    for el in list(d):
                        properties.append(el)
                var_to_property[triple[2]] = properties
        #print(var_to_property)
        for triple in filterTriples:
            if triple[0][0] == '?':
                for property in var_to_property[triple[0]]:
                    if triple[1] == '>' or triple[1] == '>=' or triple[1] == '<=' or triple[1] == '<':
                        filter_bool.append(TableFunctions.build_range_clause(property, triple[1], triple[2]))
                    elif triple[1] == '==':
                        filter_bool_must.append(TableFunctions.build_match_clause(property, triple[2]))
                    elif triple[1] == '!=':
                        filter_bool_must_not.append(TableFunctions.build_match_clause(property, triple[2]))
        filter_bool.append(BuildCompoundESQueries.BuildCompoundESQueries.build_bool_arbitrary(must = filter_bool_must,
                                                                            must_not = filter_bool_must_not))
        return BuildCompoundESQueries.BuildCompoundESQueries.build_bool_arbitrary(must = must, filter = filter_bool)

    @DeprecationWarning
    @staticmethod
    def translatePointFactQueries_v1(sparqlDataStructure, mappingTableFile):
        """

        :param sparqlDataStructure: Represents a point fact query (see Downloads/sparql-queries.txt for
        an example; see Downloads/sparql-data-structure.txt for an example of the data structure itself)
        :param mappingTableFile: for now, the adsTable-v1.jl
        :return:the elastic search query as a dict
        """
        #mappingTable = MappingTable.MappingTable.readMappingTable(mappingTableFile)
        optionalTriples = []
        whereTriples = [] #non-optional
        for clause in sparqlDataStructure['clauses']:
            tmp = []
            if 'variable' in clause:
                continue
            else:
                tmp.append('subject')
                tmp.append(clause['predicate'])
                tmp.append(clause['constraint'])

            if 'isOptional' in clause and clause['isOptional']:
                optionalTriples.append(tmp)
            else:
                whereTriples.append(tmp)

        whereList = SparqlTranslator._translateTriplesToList(whereTriples, mappingTableFile)
        optionalList = SparqlTranslator._translateTriplesToList(optionalTriples, mappingTableFile)

        outerWhere = []
        innerWhere = []
        outerOptional = []
        innerOptional = []

        for match in whereList:
            if 'meta' in match:
                if 'inner' in match['meta']:

                    del(match['meta'])

                    innerWhere.append(match)
            else:
                outerWhere.append(match)

        for match in optionalList:
            if 'meta' in match:
                if 'inner' in match['meta']:
                    del(match['meta'])

                    innerOptional.append(match)
            else:
                outerOptional.append(match)
        if innerWhere:
            outerWhere.append(BuildCompoundESQueries.BuildCompoundESQueries.
                              build_bool_arbitrary(should = innerWhere))
        if innerOptional:
            outerOptional.append(BuildCompoundESQueries.BuildCompoundESQueries.
                                 build_bool_arbitrary(should = innerOptional))


        where_bool = BuildCompoundESQueries.BuildCompoundESQueries.\
            build_bool_arbitrary(should = outerWhere)

        optional_bool = BuildCompoundESQueries.BuildCompoundESQueries.\
            build_bool_arbitrary(should = outerOptional)
        return BuildCompoundESQueries.BuildCompoundESQueries.\
            mergeBools(where_bool, optional_bool)
