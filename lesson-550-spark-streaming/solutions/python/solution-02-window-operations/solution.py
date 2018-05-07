from operator import add
from pyspark import SparkContext
from pyspark.streaming import StreamingContext


# amount reduce function for the transactions that entered the window
def add(r, c):
   count  = r[0]+c[0]
   amount = r[1]+c[1]
   mean   = amount/count if count != 0 else 0

   return (count, amount, mean)

# inverse amount reduce function for the transactions that left the window
def sub(r, c):
   count  = r[0]-c[0]
   amount = r[1]-c[1]
   mean   = amount/count if count != 0 else 0

   return (count, amount, mean)

# all durations below in seconds, windowDuration and slidingDuration must be multiples of intervalDuration
intervalDuration = 2                   # batch interval duration
windowDuration   = 5*intervalDuration  # window interval duration
slidingDuration  = 1*intervalDuration  # sliding duration

hostname = "localhost"
port = 9999

fraud_factor = 1.33

# creating a StreamingContext with the batch interval of interval seconds
ssc = StreamingContext(sc, intervalDuration)

# checkpoint for backups
ssc.checkpoint("checkpoint")

# create a DStream that will connect to hostname:port
lines = ssc.socketTextStream(hostname, port)

# parsing transactions
rawTxns = lines.map(lambda st: st.split(",")).map(lambda el: (el[0], el[1], float(el[2])))

# filtering transactions for purchases only
debitTxns = rawTxns.filter(lambda s: s[2] < 0)

# we need to add the key to transactions to be able to compare with the amount mean
# in the real application it would be more natural to use the account id as the key
keyedTxns = debitTxns.map(lambda s: (1, s))

# getting transaction amounts and reducing them to get the mean for the window
amounts = debitTxns.map(lambda s: (1, (1, s[2], s[2])))
meanAmount = amounts.reduceByKeyAndWindow(lambda r, c: add(r, c), lambda r, c: sub(r, c), windowDuration, slidingDuration)

# joining two streams with the purchase transactions and the mean
joinedTxns = keyedTxns.join(meanAmount)

# getting suspicious purchases
suspiciousTxns = joinedTxns.map(lambda (k, v): v).filter(lambda (t, m): t[2] < m[2]*fraud_factor).map(lambda (t, m): t)

suspiciousTxns.pprint()

ssc.start()             # starting the computation
ssc.awaitTermination()  # waiting for the computation to terminate
