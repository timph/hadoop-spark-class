from operator import add
from random import random
from pyspark import SparkContext
from pyspark.streaming import StreamingContext


# update functon for updating the state
def updateFunc(new_values, last_state):

    # handling the state that hasn't been created yet
    state = (0, 0, 0) if (last_state is None)  else last_state
    lst = list(state)
    for new_value in new_values:
      lst[0] = lst[0]+new_value[0]
      lst[1] = lst[1]+new_value[1]
    lst[2]=0 if (lst[0] == 0) else lst[1]/lst[0]

    return tuple(lst)


# batch interval duration in seconds
intervalDuration = 2

fraud_factor = 1.33

min_amount = 0.01
max_amount = 10.0
suspicious_amount = 10000.0

# creating a StreamingContext with the batch interval of intervalDuration seconds
ssc = StreamingContext(sc, intervalDuration)

# checkpoint for backups
ssc.checkpoint("checkpoint")

# create a RDD queue to feed the stream
rddQueue = []
for i in range(1, 6):
    txns = []
    for j in range(16):
        txns += [("date", "description", -random()*(max_amount-min_amount)+min_amount)]
    txns += [("date", "description", -i*suspicious_amount)]
    rddQueue += [ssc.sparkContext.parallelize(txns)]

# create the QueueInputDStream
debitTxns = ssc.queueStream(rddQueue)

# we need to add the key to transactions to be able to compare with the amount mean
# in the real application it would be more natural to use the account id as the key
keyedTxns = debitTxns.map(lambda s: (1, s))

# getting transaction amounts and updating the state holding amount mean
amounts = debitTxns.map(lambda s: (1, (1, s[2], s[2])))
meanAmount = amounts.updateStateByKey(updateFunc)

# joining two streams with the purchase transactions and the mean
joinedTxns = keyedTxns.join(meanAmount)

# getting suspicious purchases
suspiciousTxns = joinedTxns.map(lambda (k, v): v).filter(lambda (t, m): t[2] < m[2]*fraud_factor).map(lambda (t, m): t)

suspiciousTxns.pprint()

ssc.start()             # starting the computation
ssc.awaitTermination()  # waiting for the computation to terminate
