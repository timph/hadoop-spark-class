from numpy import array
from math import pow

from pyspark.mllib.clustering import KMeans, KMeansModel

srcDir = "file:///Users/kmusial/hadoopupdate/data"

file = srcDir + "/tx.csv"
text = sc.textFile(file)

txns = text.map(lambda st: st.split(",")).map(lambda el: (el[0], el[1], float(el[2])))

# get the amounts for clustering training
amnts = txns.map(lambda v: array([v[2]]))

clusterCount = 10
iterationCount = 20
model = KMeans.train(amnts, clusterCount, iterationCount)

print("Cluster centers")
for c, value in enumerate(model.centers):
    print(c, value[0])

# generate the amounts for clustering: +- 5*10^i, i=0..3
vals = sc.parallelize(xrange(4)).map(lambda i: pow(10, i)).flatMap(lambda x: array([-5*x, 5*x])).map(lambda v: array([v]))

# predict the clusters for the amounts
clsd = vals.map(lambda s: (s, model.predict(s)))

print("Predictions")
for value, c in clsd.collect():
    print(value[0], c)
