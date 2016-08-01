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
        Note that the table logic is hard-coded in this function. Ideally, there should be one
        table per class.

        if output_file is not None, will write out each dictionary as a json

        This is version 1.0 of the table that we will be using in the eval.
        The dat is 1/19/2016
        """
        ads_table = []
        text_props = ['readability_text', '_all']
        onto_props_with_mapping = {'phone':['telephone.name', 'telephone.name.raw'], 'email': ['email.name', 'email.name.raw'],
                                   'posting_date':['inferlink_date', 'readability_date'],
                                   'price':['price'], 'location':['addressLocality'],
                                   'name':['name'],
                                   'ethnicity':['ethnicity'],
                                   'eye_color':['eyeColor'], 'title':['title'],
                                   'hair_color':['hairColor'], 'nationality':['nationality'],
                                   'business_type':['business_type'],
                                   'business_name':['business_name'], 'services':['serviceType'],
                                   'business': ['business_name', 'physical_address'],
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

#MappingTable.buildAdsTable_v1('/home/mayankkejriwal/Downloads/adsTable-v1.jl')