from lxml import etree
import csv
import os

# DTD_PATH="/home/rkapoor/Documents/ISI/data/NYT/nyt_corpus/dtd/nitf-3-3.dtd"
directory = "/Users/mayankkejriwal/datasets/NYT_corpus/nyt_corpus/data/1987/01/01/01/"

extractor_list = list()
with open('NYT_desc.csv', 'r') as csvfile:
    reader = csv.reader(csvfile, delimiter='\t')
    for row in reader:
        extractor_list.append(row)
# print extractor_list

for subdir, dirs, files in os.walk(directory):
    for filename in files:
        if filename.endswith(".xml"):
            filepath = os.path.join(subdir, filename)
            # filepath = subdir + os.sep + file
            print(filepath)
            tree = etree.parse(filepath)
            output_doc = {}
            for extractor in extractor_list:
                extracted_list = tree.xpath(extractor[2])
                if len(extracted_list) > 0:
                    # print(extractor)
                    if extractor[1] == 'Multiple':
                        value = list()
                        for extracted_value in extracted_list:
                            if isinstance(extracted_value, etree._Element):
                                text_iter = extracted_value.itertext()
                                value_ins = ""
                                for text in text_iter:
                                    value_ins += text
                                value.append(value_ins)
                            else:
                                value.append(str(extracted_value))
                    else:
                        value = ""
                        extracted_value = extracted_list[0]
                        if isinstance(extracted_value, etree._Element):
                            text_iter = extracted_value.itertext()
                            for text in text_iter:
                                value += text
                        else:
                            value = str(extracted_value)
                    output_doc[extractor[0]] = value
                    # print extractor[0]
                    # print value

            print(output_doc)
            # break