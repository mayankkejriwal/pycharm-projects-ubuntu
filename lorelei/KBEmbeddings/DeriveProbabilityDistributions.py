import math


def inverse_log_max(edge_dict):
    """
    take max(e, weight) of each weight in the dict, then take the natural log and invert
    :param edge_dict: a key represents a node, while the weight (an int) is a 'distance'.
    :return:
    """
    e = math.exp(1.0)
    l1_norm = 0.0
    for k in edge_dict.keys():
        v = edge_dict[k]
        if v < e:
            v = e
        v = 1.0/math.log(v, e)
        l1_norm += v
        edge_dict[k] = v
    for k in edge_dict.keys():
        edge_dict[k] /= l1_norm
