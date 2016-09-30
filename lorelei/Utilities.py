import json
import codecs
import pprint

def pretty_print_lines_from_jlines(jlines_file, num_lines=1, indent=4):
    pp = pprint.PrettyPrinter(indent=indent)
    with codecs.open(jlines_file, 'r', 'utf-8') as f:
        count = 0
        for line in f:
            if count == num_lines:
                break
            obj = json.loads(line)
            pp.pprint(obj)
            print obj['loreleiJSONMapping']['rpiInformationExtraction']
            count += 1

# file = '/Users/mayankkejriwal/ubuntu-vm-stuff/home/mayankkejriwal/Downloads/lorelei/reliefWebProcessed-prepped/condensed-objects-allFields.json'
# pretty_print_lines_from_jlines(file)