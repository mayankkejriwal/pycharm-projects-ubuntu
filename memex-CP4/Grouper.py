import six


class Grouper:
    """
    This is a class because we may have to come up with 'robust' strategies for grouping
    """
    @staticmethod
    def standard_grouper(retrieved_frames, group_variable):
        """
        A simple map-reduce style function. Each value of the group_variable in the list serves as
        a key, and the values are the elements in the lists themselves.
        :param retrieved_frames The frames. Be wary of missing fields. We will treat missing value with special
        key 'MISSING_FIELD'
        :param group_variable: this is a 'mapped' variable (e.g. 'ethnicity'), not the original (e.g. '?ethnicity')
        :return: a dictionary in the style described above.
        """
        answer = {}
        for element in retrieved_frames:
            if group_variable in element['_source']:
                val = element['_source'][group_variable]
                # at present val must be a string or list
                if isinstance(val, six.string_types):
                    Grouper._add_key_value_to_dict(val, element['_source'], answer)
                elif isinstance(val, list):
                    for atom in val:
                        Grouper._add_key_value_to_dict(atom, element['_source'], answer)
                else:

                    raise Exception('group-variable references unrecognized type: not string or list!')

            else:
                Grouper._add_key_value_to_dict('', element['_source'], answer)
        return answer


    @staticmethod
    def _add_key_value_to_dict(key, value, dict):
        """
        Every value in the dict must be a list, but the passed in value is atomic
        If the key doesn't exist, it will be created in the dict

        Modified dict

        :param key: key
        :param value: 'atomic' value
        :param dict: each value intended to be a list
        :return: None
        """
        if not key in dict:
            dict[key] = []
        dict[key].append(value)