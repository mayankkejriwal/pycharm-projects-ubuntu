path = '/Users/mayankkejriwal/datasets/memex/vagrant-memex/hadoop/'

import codecs

out = codecs.open(path+'copyToLocal2.sh', 'w')
line_prefix = 'sudo hadoop dfs -copyToLocal /user/worker/dig3/full/phase4/2016-2/2/part-00'
line_postfix = ' .\n'
var = 278
while var <= 999:
    out.write(line_prefix+str(var)+line_postfix)
    var += 1
out.close()
