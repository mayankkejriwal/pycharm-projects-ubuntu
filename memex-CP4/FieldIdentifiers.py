
"""
Code for taking a set of properties and identifying if that set (unambiguously) corresponds to
a particular property in the schema e.g. location. This code is tied to the MappingTable code, so
be careful! Right now, it's designed for AdsTable-v3.
"""

def is_location(set_of_properties):
    if 'inferlink_city.result.value' in set_of_properties:
        return True
    else:
        return False

def is_name(set_of_properties):

    if 'high_precision.name.result.value' in set_of_properties:
        return True
    else:
        return False

def is_ethnicity(set_of_properties):
    if 'high_precision.ethnicity.result.value' in set_of_properties:
        return True
    else:
        return False

def is_height(set_of_properties):
    if 'inferlink_height.result.value' in set_of_properties:
        return True
    else:
        return False

def is_weight(set_of_properties):
    if 'inferlink_weight.result.value' in set_of_properties:
        return True
    else:
        return False

def is_price(set_of_properties):
    if 'inferlink_price.result.value' in set_of_properties:
        return True
    else:
        return False