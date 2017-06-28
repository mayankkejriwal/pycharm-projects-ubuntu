path = '/Users/mayankkejriwal/datasets/memex/user-worker-dig3-full-phase4-t-2015-1/'

import codecs

out = codecs.open(path+'convertToGunzip.sh', 'w')
for i in range(0, 10):
    out.write('~/software/hadoop/hadoop-2.7.3/bin/hadoop fs -text part-0000'+str(i)+'> part-0000'+str(i)+'.txt\n')
    out.write('gzip part-0000'+str(i)+'.txt\n')
    out.write('rm part-0000'+str(i)+'\n')
for i in range(10, 100):
    out.write('~/software/hadoop/hadoop-2.7.3/bin/hadoop fs -text part-000'+str(i)+'> part-000'+str(i)+'.txt\n')
    out.write('gzip part-000'+str(i)+'.txt\n')
    out.write('rm part-000' + str(i) + '\n')
for i in range(100, 1000):
    out.write('~/software/hadoop/hadoop-2.7.3/bin/hadoop fs -text part-00'+str(i)+'> part-00'+str(i)+'.txt\n')
    out.write('gzip part-00'+str(i)+'.txt\n')
    out.write('rm part-00' + str(i) + '\n')
# for i in range(1000, 2000):
#     out.write('~/software/hadoop/hadoop-2.7.3/bin/hadoop fs -text part-0'+str(i)+'> part-0'+str(i)+'.txt\n')
#     out.write('gzip part-0'+str(i)+'.txt\n')
#     out.write('rm part-0' + str(i) + '\n')

out.close()