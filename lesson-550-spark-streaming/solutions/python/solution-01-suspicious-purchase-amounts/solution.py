from operator import add
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
