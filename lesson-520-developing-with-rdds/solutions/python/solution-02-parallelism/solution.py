from operator import add
from datetime import datetime

# get the word with bigger count
def getMax(r, c):
  if (r[1] > c[1]):
    return r
  else:
    return c

srcDir = <set srcDir to the absolute path of the source data directory location>
file = srcDir + "/sherlock-holmes.txt"

# read text and cache
text = sc.textFile(file).persist()

minCores = 2
numCores = 8

for prt in range(minCores, numCores+1, 2):

    rept = text.repartition(prt)

    dt1 = datetime.now()
    words = rept.flatMap(lambda w : w.split(' '))
    dict = words.map(lambda w : (w, 1))
    counts = dict.reduceByKey(add)
    max = counts.reduce(lambda r, c: getMax(r, c))
    dt2 = datetime.now()
    print(str(max) + " partition count " + str(prt) + " time " + str((dt2-dt1).microseconds/1000))
