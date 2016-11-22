import codecs, json

class MappingTable:
    """
    description of class
    """
    @staticmethod
    def readMappingTable(input_file):
        """ """
        mappingTable = {}
        with codecs.open(input_file, 'r', 'utf-8') as f:
            for line in f:
                entry = json.loads(line)
                mappingTable[entry['onto_prop']] = entry['mappings']
        return mappingTable


    @staticmethod
    def buildAdsTable_v1(output_file = None):
        """
        Written for Summer QPR
        Note that the table logic is hard-coded in this function. Ideally, there should be one
        table per class.

        if output_file is not None, will write out each dictionary as a json

        This is version 1.0 of the table that we will be using in the eval.
        The dat is 1/19/2016
        """
        ads_table = []
        text_props = ['readability_text', '_all']
        onto_props_with_mapping = {'phone':['telephone.name', 'telephone.name.raw'], 'email': ['email.name', 'email.name.raw'],
                                   'posting_date':['inferlink_date', 'readability_date', 'high_recall_readability_date'],
                                   'price':['price'], 'location':['addressLocality'],
                                   'name':['name'],
                                   'ethnicity':['ethnicity'],
                                   'eye_color':['eyeColor'], 'title':['title'],
                                   'hair_color':['hairColor'], 'nationality':['nationality'],
                                   'business_type':['business_type'],
                                   'business_name':['streetAddress'], 'services':['serviceType'],
                                   'business': ['streetAddress'],
                                   'physical_address': ['streetAddress'],
                                   'gender':['gender'], 'top_level_domain':['top_level_domain'],
                                   'obfuscation':['telephone.isObfuscated', 'email.isObfuscated'],
                                   'age':['age'], 'hyperlink:':['relatedLink'], 'drug_use':['drug_use'],
                                   'review_site':['review_site'], 'review_id':['review_id'],
                                   'number_of_individuals':['name_count'],
                                   'ad': ['identifier'],
                                   'multiple_phone': ['telephone_count'],
                                   'cluster': ['seller.uri'],
                                   'seed': ['seller.telephone.name', 'seller.email.name']
                                 }
        non_readability_props = ['number_of_individuals', 'ad', 'multiple_phone', 'cluster', 'phone', 'posting_date', 'email']
        onto_props_without_mapping = ['image_with_email', 'image_with_phone']
        for property, value_list in onto_props_with_mapping.iteritems():
            dict = {}
            dict['onto_prop'] = property
            mappings = []
            tmp = {}
            for v in value_list:
                if property == 'phone' or v == 'seller.telephone.name':
                    tmp[v] = 'build_phone_match_clause'
                    tmp['_all'] = 'build_phone_match_clause'
                    tmp['url'] = 'build_phone_regexp_clause'
                elif v == 'email.name':
                    tmp[v] = 'build_email_match_clause'
                    tmp['_all'] = 'build_match_phrase_clause'
                elif property == 'ad':
                    tmp[v] = 'build_term_clause'
                elif '_count' in v:
                    tmp[v] = 'build_count_match_clause'
                elif property == 'gender':
                    tmp[v] = 'build_gender_match_clause'
                elif property == 'posting_date':
                    tmp[v] = 'build_match_phrase_clause'
                else:
                    tmp[v] = 'build_match_clause'
            if property not in non_readability_props:
                for v in text_props:    # will overwrite for seller.telephone.name
                    tmp[v] = 'build_match_clause_inner'
            mappings.append(tmp)
            dict['mappings'] = mappings
            ads_table.append(dict)

        for property in onto_props_without_mapping:
            dict = {}
            dict['onto_prop'] = property
            mappings = []
            tmp = {}
            for v in text_props:
                tmp[v] = 'build_match_clause_inner'
            mappings.append(tmp)
            dict['mappings'] = mappings
            ads_table.append(dict)

        if output_file:
            file = codecs.open(output_file, 'w', 'utf-8')
            for entry in ads_table:
                json.dump(entry, file)
                file.write('\n')
            file.close()

    @staticmethod
    def buildAdsTable_v3(output_file=None):
        """
        This is the table we're using for the november evals, so make sure its all prim and proper.
        :param output_file:
        :return:
        """
        ads_table = []
        text_props = ['high_precision.description.result.value','high_precision.readability.result.value',
                      'high_recall.readability.result.value',
                      'extracted_text', '_all']
        attributes = set(['phone', 'age', 'email', 'ethnicity', 'eye_color', 'hair_color',
            'name', 'posting_date', 'service', 'street_address'])
        onto_props_with_mapping = {'location':['high_precision.city.result.value',
                                               'high_precision.state.result.value',
                                               'high_precision.country.result.value',
                                               'high_precision.location.result.value',
                                               'high_recall.city.result.value',
                                               'high_recall.state.result.value',
                                               'high_recall.country.result.value',
                                               'high_recall.location.result.value'],
                                   'ad': ['cdr_id'],

                                   'review_id':['inferlink_review-id.result.value',
                                                'high_precision.review-id.result.value.identifier',
                                                'high_precision.review-id.result.value.site',
                                                'high_recall.review-id.result.value.identifier',
                                                'high_recall.review-id.result.value.site'],
                                   'social-media_id': ['high_precision.social-media-id.result.value.instagram',
                                                       'high_precision.social-media-id.result.value.twitter',
                                                       'high_recall.social-media-id.result.value.instagram',
                                                       'high_recall.social-media-id.result.value.twitter'
                                                       ],
                                    'title': ['high_precision.title.result.value'],
                                   'cluster': ['CDRIDs.uri'],
                                   'seed': ['centroid_phone'] # centroid_phone can also contain emails
                                   }
        non_readability_props = ['ad','cluster', 'phone', 'posting_date','email', 'seed']
        # onto_props_without_mapping = ['image_with_email', 'image_with_phone']
        unmapped_props = ['height', 'weight', 'price', 'tattoo', 'multiple_providers'] # it is important to keep track of this
        for attribute in attributes:
            m = list()
            # if attribute not in ['phone', 'email']:
            #     m.append('inferlink_'+attribute+'.result.value')
            m.append('high_precision.'+attribute+'.result.value')
            m.append('high_recall.' + attribute + '.result.value')
            if attribute == 'ethnicity':
                # print attribute
                m.append('high_precision.nationality.result.value')
                m.append('high_recall.nationality.result.value')
            onto_props_with_mapping[attribute] = m

        for unmapped_prop in unmapped_props:
            m = list()
            m.append('inferlink_' + unmapped_prop + '.result.value')
            onto_props_with_mapping[unmapped_prop] = m

        for property, value_list in onto_props_with_mapping.iteritems():
            dict = {}
            dict['onto_prop'] = property
            mappings = []
            tmp = {}
            for v in value_list:
                # if property in unmapped_props:
                #     tmp['cdr_id'] = 'build'
                if property == 'seed':
                    tmp[v] = 'build_phone_or_email_match_clause'
                elif property == 'phone':
                    tmp[v] = 'build_phone_match_clause'
                    tmp['_all'] = 'build_phone_match_clause'
                    tmp['url'] = 'build_phone_regexp_clause'
                elif property == 'email':
                    tmp[v] = 'build_email_match_clause'
                    tmp['_all'] = 'build_match_phrase_clause'
                elif property == 'ad':
                    tmp[v] = 'build_term_clause'
                elif property == 'posting-date':
                    tmp[v] = 'build_match_phrase_clause'
                else:
                    tmp[v] = 'build_match_clause'
            if property not in non_readability_props:
                for v in text_props:    # will overwrite for seller.telephone.name
                    tmp[v] = 'build_match_clause_inner'
            mappings.append(tmp)
            dict['mappings'] = mappings
            ads_table.append(dict)
        if output_file:
            out = codecs.open(output_file, 'w', 'utf-8')
            for entry in ads_table:
                json.dump(entry, out)
                out.write('\n')
            out.close()

    @staticmethod
    def buildAdsTable_v2(output_file=None):
        """
        Written in November for the final QPR of 2016

        Note that the table logic is hard-coded in this function. Ideally, there should be one
        table per class.

        if output_file is not None, will write out each dictionary as a json

        This is version 1.0 of the table that we will be using in the eval.
        The dat is 1/19/2016
        """
        ads_table = []
        text_props = ['extracted_text', '_all', 'tokens']

        attributes = set(['email', 'service', 'name', 'nationality', 'location', 'review-id', 'ethnicity',
                      'street-address', 'price', 'hair-color', 'phone', 'breast', 'age', 'posting-date'])
        onto_props_with_mapping = dict()
        for attribute in attributes:
            m = list()
            m.append(attribute+'.result.value')
            onto_props_with_mapping[attribute] = m
        onto_props_with_mapping['text'] = text_props
        onto_props_with_mapping['loose_text'] = ['_all']
        # non_readability_props = ['number_of_individuals', 'ad', 'multiple_phone', 'cluster', 'phone', 'posting_date',
        #                          'email']
        # onto_props_without_mapping = ['image_with_email', 'image_with_phone']
        for property, value_list in onto_props_with_mapping.iteritems():
            onto_prop_dict = dict()
            onto_prop_dict['onto_prop'] = property
            mappings = list()
            tmp = dict()
            for v in value_list:
                tmp[v] = 'build_match_clause'

            mappings.append(tmp)
            onto_prop_dict['mappings'] = mappings
            ads_table.append(onto_prop_dict)

        if output_file:
            out = codecs.open(output_file, 'w', 'utf-8')
            for entry in ads_table:
                json.dump(entry, out)
                out.write('\n')
            out.close()

    @staticmethod
    @DeprecationWarning
    def buildOfferTableFromScratch(output_file = None):
        """
        Note that the table logic is hard-coded in this function. Ideally, there should be one
        table per class.
        
        if output_file is not None, will write out each dictionary as a json

        We invent our own property names by using the correct
        name from Offer, and appending a '1' to it. Since this is only a prototype, we use some
        of the more important properties (see our_properties)

        """
        
        offer_table = []
        our_properties = ['itemOffered.name', 'itemOffered.age','itemOffered.ethnicity', 'mainEntityOfPage.description']
        for property in our_properties:
            dict = {}
            dict['onto_prop'] = property+'1'
            mappings = []
            tmp = {}
            tmp[property] = 'build_match_clause'
            mappings.append(tmp)
            dict['mappings'] = mappings

            offer_table.append(dict)


        
        if output_file:
            file = codecs.open(output_file, 'w', 'utf-8')
            for entry in offer_table:
                json.dump(entry, file)
                file.write('\n')
            file.close()

    @staticmethod
    @DeprecationWarning
    def buildJustReadabilityTextTableFromScratch(output_file = None):
        """
        Note that the table logic is hard-coded in this function. Ideally, there should be one
        table per class.

        if output_file is not None, will write out each dictionary as a json

        We use the property names from the Google docs file and map everything to readability_text

        """

        table = []
        properties = [':phone', ':email', ':physical_address', ':posting_date', ':location', ':name', ':service']
        for property in properties:
            dict = {}
            dict['onto_prop'] = property
            mappings = []
            tmp = {}
            if property == ':phone':
                tmp['readability_text'] = 'build_match_clause_and'
            else:
                tmp['readability_text'] = 'build_match_clause'
            mappings.append(tmp)
            dict['mappings'] = mappings

            table.append(dict)



        if output_file:
            file = codecs.open(output_file, 'w', 'utf-8')
            for entry in table:
                json.dump(entry, file)
                file.write('\n')
            file.close()

    @staticmethod
    @DeprecationWarning
    def buildJustInferlinkTextTableFromScratch(output_file = None):
        """
        Note that the table logic is hard-coded in this function. Ideally, there should be one
        table per class.

        if output_file is not None, will write out each dictionary as a json

        We use the property names from the Google docs file and map everything to inferlink_text

        """

        table = []
        properties = [':phone', ':email', ':physical_address', ':posting_date', ':location', ':name', ':service']
        for property in properties:
            dict = {}
            dict['onto_prop'] = property
            mappings = []
            tmp = {}
            if property == ':phone':
                tmp['inferlink_text'] = 'build_match_clause_and'
            else:
                tmp['inferlink_text'] = 'build_match_clause'
            mappings.append(tmp)
            dict['mappings'] = mappings

            table.append(dict)



        if output_file:
            file = codecs.open(output_file, 'w', 'utf-8')
            for entry in table:
                json.dump(entry, file)
                file.write('\n')
            file.close()


# MappingTable.buildAdsTable_v3('/Users/mayankkejriwal/datasets/memex-evaluation-november/adsTable-v3.jl')