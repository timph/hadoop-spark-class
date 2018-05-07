from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType

# create DataFrame with people described by name, sex and age
people = sc.parallelize([("Jane", "F", 25), ("Frank", "M", 30), ("Eve", "F", 45), ("Andrew", "M", 18)], 2).toDF(
    ["name", "sex", "age"])

print("List of all people")
people.show()
people.createOrReplaceTempView("people")

# find pairs of all males and females where male is same age or older but no more than 5 years
pairs = spark.sql("SELECT * FROM people p1 JOIN people p2 ON p1.sex='M' AND p2.sex='F' AND p1.age-p2.age BETWEEN 0 AND 5")

print("List of all maale and female pairs where male is same age or older but no more than 5 years")
pairs.show()

# define and register UDF to identify teenagers, registering needed to use it in SQL
def teenager(s):
  return (s >= 13 and s <= 19)
spark.udf.register("teenager", teenager)

teens = spark.sql("SELECT name, teenager(age) AS teen FROM people")

print("List of all names with teenage flag")
teens.show()

teenager_yn = udf(lambda s: "y" if teenager(s) else "n")

# apply UDF in DataFrame
teens2 = people.select(people["name"], teenager_yn(people["age"]).alias("teen"))

print("List of all names with teenage indicator")
teens2.show()

teenager_bool = udf(teenager, BooleanType())

# apply UDF in DataFrame filter
teens3 = people.filter(teenager_bool(people["age"]))

print("List teenagers only")
teens3.show()
