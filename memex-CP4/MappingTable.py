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

    @DeprecationWarning
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
            'name', 'post_date', 'services', 'street_address'])
        onto_props_with_mapping = {'location':['inferlink_city.result.value','high_precision.city.result.value',
                                               'high_precision.state.result.value',
                                               'high_precision.country.result.value',
                                               'high_precision.location.result.value',
                                               'high_recall.city.result.value',
                                               'high_recall.state.result.value',
                                               'high_recall.country.result.value',
                                               'high_recall.location.result.value'],
                                   'ad': ['cdr_id'],
                                   'services': ['high_precision.service.result.value', 'high_recall.service.result.value'],
                                   'review_site_id':['inferlink_review-id.result.value',
                                                'high_precision.review-id.result.value.identifier',
                                                'high_precision.review-id.result.value.site',
                                                'high_recall.review-id.result.value.identifier',
                                                'high_recall.review-id.result.value.site'],
                                   'social_media_id': ['high_precision.social-media-id.result.value.instagram',
                                                       'high_precision.social-media-id.result.value.twitter',
                                                       'high_recall.social-media-id.result.value.instagram',
                                                       'high_recall.social-media-id.result.value.twitter',
                                                       'tokens_extracted_text.result.value'
                                                       ],
                                    'title': ['high_precision.title.result.value'],
                                   'content': ['high_precision.description.result.value',
                                               'high_precision.readability.result.value','high_recall.readability.result.value','extracted_text'],
                                   'cluster': ['CDRIDs.uri'],
                                   'seed': ['centroid_phone', 'high_precision.phone.result.value','high_recall.phone.result.value',
                                            'high_precision.email.result.value', 'high_recall.email.result.value',
                                            'tokens_extracted_text.result.value'] # centroid_phone can also contain emails
                                   }
        non_readability_props = ['ad','cluster', 'phone', 'post_date','email']
        keyword_expansion_props = ['ethnicity','eye_color','hair_color']
        # onto_props_without_mapping = ['image_with_email', 'image_with_phone']
        unmapped_props = ['height', 'weight', 'price', 'tattoos', 'multiple_providers'] # it is important to keep track of this
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
        onto_props_with_mapping['nationality'] = list(onto_props_with_mapping['ethnicity'])
        for unmapped_prop in unmapped_props:
            m = list()
            m.append('inferlink_' + unmapped_prop + '.result.value')
            # m += text_props
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
                    # tmp['url'] = 'build_phone_regexp_clause'
                elif property == 'email':
                    tmp[v] = 'build_email_match_clause'
                    tmp['_all'] = 'build_match_phrase_clause'
                elif property == 'ad':
                    tmp[v] = 'build_term_clause'
                elif property == 'post_date':
                    tmp[v] = 'build_match_phrase_clause'
                elif property in keyword_expansion_props:
                    tmp[v] = 'build_match_clause_inner_with_keyword_expansion'
                elif property == 'social_media_id':
                    tmp[v] = 'build_social_media_match_clause'
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

    @DeprecationWarning
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


    @staticmethod
    def lattice_buildAdsTable_v1(output_file=None):
        """
        This is the table we're using for the november lattice evals, so make sure its all prim and proper.
        we will ONLY be using lattice extractions in this table. For combining extractions, we use a different
        approach.
        :param output_file:
        :return:
        """
        ads_table = []
        text_props = ['lattice_extractions.lattice-content.results.value',
                      'extracted_text']
        attributes = set(['phone', 'age', 'email', 'content', 'name', 'title'])
        onto_props_with_mapping = {'location':['lattice_extractions.lattice-location.results.context.location.name'],
                                   'post_date': ['lattice_extractions.lattice-postdatetime.results.value'],
                                   'price': ['lattice_extractions.lattice-rate.results.value'],
                                   'social_media_id': ['lattice_extractions.lattice-username.results.value'],
                                     'ad': ['cdr_id'],
                                   'cluster': ['CDRIDs.uri'],
                                   'seed': ['centroid_phone','lattice_extractions.lattice-phone.results.value'] # centroid_phone can also contain emails
                                   }
        non_readability_props = ['ad','cluster', 'phone', 'post_date','email']
        unmapped_props = ['height', 'weight', 'tattoos', 'multiple_providers','ethnicity','eye_color','hair_color',
                          'nationality', 'services', 'street_address','review_site_id']
        for attribute in attributes:
            m = list()
            m.append('lattice_extractions.lattice-' + attribute + '.results.value')
            onto_props_with_mapping[attribute] = m
        for unmapped_prop in unmapped_props:
            m = list(text_props)
            onto_props_with_mapping[unmapped_prop] = m

        for property, value_list in onto_props_with_mapping.iteritems():
            dict = {}
            dict['onto_prop'] = property
            mappings = []
            tmp = {}
            for v in value_list:
                if property == 'seed':
                    tmp[v] = 'build_phone_or_email_match_clause'
                elif property == 'phone':
                    tmp[v] = 'build_phone_match_clause'
                    tmp['extracted_text'] = 'build_phone_match_clause'
                    # tmp['url'] = 'build_phone_regexp_clause'
                elif property == 'email':
                    tmp[v] = 'build_email_match_clause'
                    tmp['_all'] = 'build_match_phrase_clause'
                elif property == 'ad':
                    tmp[v] = 'build_term_clause'
                elif property == 'post_date':
                    tmp[v] = 'build_match_phrase_clause'
                elif property == 'social_media_id':
                    tmp[v] = 'build_social_media_match_clause'
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

# MappingTable.buildAdsTable_v3('/home/mayankkejriwal/Downloads/memex-cp2/nov-2016/adsTable-v3.jl')
# MappingTable.lattice_buildAdsTable_v1('/home/mayankkejriwal/Downloads/memex-cp2/nov-2016/lattice-adsTable-v1.jl')