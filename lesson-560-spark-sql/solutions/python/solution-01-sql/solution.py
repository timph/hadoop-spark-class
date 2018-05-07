from operator import add

srcDir = <set srcDir to the absolute path of the source data directory location>

file = srcDir + "/tx.csv"
text = sc.textFile(file)

txns = text.map(lambda st: st.split(",")).map(lambda el: (el[0], el[1], float(el[2]))).toDF(["date", "desc", "amount"])
txns.printSchema()
txns.show()

txns.createOrReplaceTempView("txns")

print("Total count: " + str(txns.count()))

print("Big credits:")
bigCredits = spark.sql("SELECT * FROM txns WHERE amount > 0 ORDER BY amount DESC LIMIT 10")
bigCredits.show()

print("Big debits:")
bigDebits = spark.sql("SELECT * FROM txns WHERE amount < 0 ORDER BY amount LIMIT 10")
bigDebits.show()

# define string length function
def strlen(s):
  return len(s)
spark.udf.register("len", strlen)

print("Long descriptions:")
longDesc = spark.sql("SELECT len(desc), desc FROM txns WHERE len(desc) >= 100 ORDER BY len(desc) DESC")
longDesc.show()

print("Big credits from JSON:")
jsonTxns = spark.read.json(srcDir + "/tx.jsons")
jsonBigCredits = spark.sql("SELECT * FROM txns WHERE amount > 0 ORDER BY amount DESC LIMIT 10")
jsonBigCredits.show()
