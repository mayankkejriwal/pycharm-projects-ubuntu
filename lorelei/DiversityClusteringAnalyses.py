def print_cluster_label_counts(cluster_model):
    """
    Takes the model and uses the labels_ attribute to determine the labels, and the number of instances per label.
    Prints these out and also returns the count dict
    :param cluster_model:
    :return: the label_count dict
    """
    label_count_dict = dict()
    labels = cluster_model.labels_
    for label in labels:
        if label not in label_count_dict:
            label_count_dict[label] = 0
        label_count_dict[label] += 1
    print label_count_dict
    return label_count_dict