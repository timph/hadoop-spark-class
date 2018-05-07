# Spark SQL

In this lab, you will take your first steps using Spark's support for SQL and relational concepts.

## Objectives

1. Open and interact with Spark via Spark's interactive shell.
2. Use the `SqlContext` to query data as relational tables using different APIs.
3. Create a user-defined SQL function and use it in a SQL query.

## Prerequisites

This lab assumes that the student is familiar with the course environment, in particular, the Spark distribution.

## Instructions

### Open the Spark shell

First, we need to open the Spark shell.  Simply change directory to your Spark home and issue the command  `bin/spark-shell` command:

``` sh
$ cd $SPARK_HOME
$ bin/spark-shell --master local[*]
```

Next, you'll see output like the following.  Don't worry if the versions of things shown below don't *exactly* match what you have on your machine, and you may have more or less logging than what's shown.

``` scala
log4j:WARN No appenders could be found for logger (org.apache.hadoop.metrics2.lib.MutableMetricsFactory).
log4j:WARN Please initialize the log4j system properly.
log4j:WARN See http://logging.apache.org/log4j/1.2/faq.html#noconfig for more info.
Using Spark's repl log4j profile: org/apache/spark/log4j-defaults-repl.properties
To adjust logging level use sc.setLogLevel("INFO")
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 1.5.2
      /_/

Using Scala version 2.10.4 (Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_31)
Type in expressions to have them evaluated.
Type :help for more information.
15/12/18 15:10:18 WARN MetricsSystem: Using default name DAGScheduler for source because spark.app.id is not set.
Spark context available as sc.
SQL context available as sqlContext.

scala>
```

Awesome — we're now ready to slice & dice data using Spark SQL!

### Get a SQLContext

In order to work with Spark's SQL support, we need to first get our hands on a special context called `SQLContext`.  First, let's start with some appropriate imports.  Issue the following commands in the Spark shell:

``` scala
import scala.util._
import org.apache.spark.sql._
```

Next, let's get a `SQLContext`.  Some Spark shells include one by default; to handle these cases, issue the following command:

``` scala
val ctx: SQLContext = Try(sqlContext).getOrElse(new SQLContext(sc))
```

This ensures that we've got a `SQLContext` called `ctx` whether or not one was defined.  Next, we want to make sure that we're getting all the nice Spark SQL implicits that the `SQLContext` provides.  Issue the following command:

``` scala
import ctx.implicits._
```

Now, we're ready to roll with Spark SQL.

### Define a case class to represent your data

One of the easiest ways to get going with Spark SQL is to define the schema of your data via a case class.  Since we're using our sample transaction data, we'll define a simple case class called `Tx`.  Enter the following command into your shell:

``` scala
case class Tx(date: String, desc: String, amount: Double)
```

> Note:  To use Spark SQL effectively, your data must be structured.


### Get some data

Let's get some data to work with by creating a simple `RDD` from a sample transaction data file called `tx.csv` found in this course's `lesson-5xx-resources` directory.  

Define a variable called srcDir which contains the full path to the lesson-5xx-resources directory, wherever that may be on your machine. It should look something like this:

```scala
val srcDir = "/Users/.../lesson-5xx-resources"
```

Issue the following command:

``` scala
val rdd = sc.textFile(srcDir + "/tx.csv").map(_.split(",")).map(x => Tx(x(0), x(1), x(2).toDouble)).cache
```

Notice that we added the `cache` method call to keep the `RDD` in memory once it's loaded; this will improve performance as we query the data in subsequent steps during this lab.


### Get a `DataFrame`

The primary, table-like artifact in Spark SQL that represents data is called a `DataFrame`.  Thanks to some clever implicits defined by `ctx.implicits`, there's a new method on `RDD` called `toDF()` which returns a `DataFrame` with its dataset being that of the `RDD` from whence the `DataFrame` came.

> Note:  *Any* `RDD` can serve as the basis for a `DataFrame`.

A `DataFrame` can essentially be thought of as a table, with columns and rows.  Often, the schema of the table, that is, the names and types of the columns, can be inferred, but it can also be programmatically defined.

Let's get a `DataFrame` of our transaction data now.  Issue the command

``` scala
val table = rdd.toDF()
```

in your shell.  This returns to us a `DataFrame` whose schema has been inferred from our `Tx` case class.  Remember, the original `RDD` was of type `RDD[Tx]`, so the `DataFrame` can inspect the `Tx` class for the names & types of its fields and assume that these define corresponding columns.

Take a look at the inferred schema by issuing command

``` scala
table.printSchema
```

You should see output like the following:

``` shell
root
 |-- date: string (nullable = true)
 |-- desc: string (nullable = true)
 |-- amount: double (nullable = false)
```

As you can see, these columns are derived from our case class `Tx`.

### Register the `DataFrame` as a table

Now, let's give our `DataFrame` a name so that we can query it via SQL.  We do this by "registering" a name for the `DataFrame` with the `SQLContext`.  Issue the following command in your shell:

``` scala
table.registerTempTable("tx")
```

This will cause the `DataFrame` to register itself in its `SQLContext` with the given name, in this case, `tx`.  That's all it takes in order for us to be able to query data in the Spark cluster via SQL!

### Query some data with SQL

Let's do something simple to start out with:  count the number of rows in the table.  Issue the following command:

``` scala
ctx.sql("SELECT COUNT(*) FROM tx").collect.foreach(println(_))
```

Your output should simply be

```
[1265]
```

Nice!  You should start realizing the possibilities here.  Any program that speaks JDBC (or ODBC) can connect to a Spark cluster and start querying data, including COTS business intelligence tools (BIRT, etc) and custom programs.

Let's sink our teeth into Spark SQL's juicy SQL flesh by finding the top 10 credits in our transaction data:

``` scala
ctx.sql("SELECT * FROM tx WHERE amount > 0 ORDER BY amount DESC LIMIT 10") .collect.foreach(println(_))
```

Your output should be the following:

```
[2015-12-15,Deposit - Online Banking Transfer from XXXXXXXXXX SAV,2209.03]
[2015-08-05,Deposit - Shared Branch 5011 W SLAUGHTER LN BLD AUSTIN        TX,1520.0]
[2015-12-15,Deposit - Online Banking Transfer from XXXXXXXXXX CK,1360.48]
[2015-12-07,Deposit - Online Banking Transfer from XXXXXXXXXX CK,1000.0]
[2015-12-11,Deposit - Online Banking Transfer from XXXXXXXXXX CK,925.0]
[2015-12-04,Deposit - Online Banking Transfer from XXXXXXXXXX CK,800.0]
[2015-06-19,Deposit - Online Banking Transfer from XXXXXXXXXX SAV,750.31]
[2015-07-26,Deposit - Online Banking Transfer from XXXXXXXXXX CK,650.0]
[2015-07-10,Deposit - Online Banking Transfer from XXXXXXXXXX SAV,500.0]
[2015-07-07,Deposit - Online Banking Transfer from XXXXXXXXXX SAV,500.0]
```

It's a Thing of Beauty!  This data is stored in who-knows-what format in who-knows-what-kind-of-distributed-cluster, but we can query it like it's a table!  Let's keep going.

### Query some data with Spark SQL's query DSL

For those of you who groan at the sight of strings carrying program logic, this might just be for you.  Spark SQL includes a simple but effective query DSL that can mitigate errors in otherwise stringy SQL that can't be checked at compile-time.  Let's perform the same query, only this time, let's use the query DSL.  Issue the following command in your shell:

``` scala
table.filter("amount > 0") .orderBy(table.col("amount").desc).limit(10).collect.foreach(println(_))
```

We've greatly reduced the portion of our code that is contained in strings that are opaque to the compiler!

> Note:  Spark 1.6 will take type safety even farther with a new artifact called a `DataSet`, which allows your query DSL-based code to be *completely* type-safe.

Your output should be identical to your earlier query.  Let's keep going with the complementary query.  What are the biggest debits in our transaction data?

``` scala
ctx.sql("SELECT * FROM tx WHERE amount < 0 ORDER BY amount LIMIT 10") .collect.foreach(println(_))
```

Your output should be the following:

```
[2015-12-14,External Withdrawal - PAYPAL INSTANT TRANSFER - INST XFER,-925.0]
[2015-12-07,ATM Withdrawal - CAPITAL ONE 7200 NORTH MOPAC       AUSTIN       TXUS - Card Ending In 2000,-803.0]
[2015-12-04,ATM Withdrawal - BECU 35105 ENCHANTED PARKWAYFEDERAL WAY  WAUS - Card Ending In 7090,-800.0]
[2015-06-18,POS Withdrawal - 1921254 DISCOUNT  DISCOUNT TIR AUSTIN       TXUS - Card Ending In 7090,-750.31]
[2015-06-17,Withdrawal - Online Banking Transfer To XXXXXXXXXX CK,-511.36]
[2015-07-28,External Withdrawal - PAYPAL INSTANT TRANSFER - INST XFER,-408.71]
[2015-06-27,ATM Withdrawal - ULTRON PROCESSI HARRAH'S NEW ORLEANS   NEW ORLEANS  LAUS - Card Ending In 2000,-405.99]
[2015-06-18,External Withdrawal - FCU  -  Bac,-350.0]
[2015-07-06,Withdrawal - Shared Branch 5029 KYLE CENTER DR     KYLE          TX,-350.0]
[2015-08-14,POS Withdrawal - 879257 HEB #611               DRIPPING SPRITXUS - Card Ending In 7090,-335.12]
```

And now, via the query DSL:

``` scala
table.filter("amount < 0") .orderBy(table.col("amount")).limit(10).collect.foreach(println(_))
```

Again, the results should be identical.

### Extend SQL with your own functions

Many SQL databases allow you to define your own functions and call them in your queries; these are called "user-defined functions", or UDFs.  Spark also supports these, and you can write them in Scala, Java, Python, or R.  Let's check it out.

Our custom function will be very simple:  it calculates the trimmed length of a string.  If you've ever done this in pure SQL, you've felt The Pain.  However, it's trivial in Scala:

``` scala
val strlen = (s: String) => s.trim.length
```

Enter that command in your shell, then register it as a UDF in your `SQLContext`:

``` scala
ctx.udf.register("len", strlen)
```

Here, we've registered our function `strlen` in the `SQLContext` with the name `len`.  Now, let's use it to find extremely long transaction descriptions.

``` scala
ctx.sql("SELECT len(desc), desc FROM tx WHERE len(desc) >= 100 ORDER BY len(desc) DESC") .collect.foreach(println(_))
```

Here are our results:

```
[107,External Deposit - AMAZON.COM ID3LXQ2E - Marketplac  payments.amazon.com ID#U6X42QZ19K9LLOW U6X42QZ19K9LLOW]
[107,External Deposit - AMAZON.COM IESC1WFF - Marketplac  payments.amazon.com ID#QNQK9Z43WLNNVIB QNQK9Z43WLNNVIB]
```

There are only two transactions whose length exceeds 100 characters.  Notice that we've used our UDF in the `SELECT` clause, the `WHERE` clause, and the `ORDER BY` clause!  Nice, right?

### Create tables from other types of data

Spark SQL supports directly creating `DataFrame`s from several different formats natively, including JSON, Parquet, and even JDBC.  That's right:  you can create your own tables in the cluster from data in some *external* JDBC data source!  Let's have a look at reading some JSON data.

Let's read the sample transaction data that's been provided in JSON format.  Here's a snippet of what it looks like:

``` json
{ "date": "2015-06-16", "desc": "POS Withdrawal - 75901 CORNER STORE 13        DRIPPING SPRITXUS - Card Ending In 7090", "amount":-66.21 }
{ "date": "2015-06-16", "desc": "Withdrawal - Online Banking Transfer To XXXXXXXXXX CK", "amount":-50.0 }
{ "date": "2015-06-16", "desc": "POS Withdrawal - 879042 HEB GAS #404           AUSTIN       TXUS - Card Ending In 7090", "amount":-45.49 }
```

> Warning:  There's one thing to note about Spark SQL's JSON support:  each line must be a complete JSON object.  That's not usually the case with JSON.  Most blocks of JSON data are multiline.  The data above may be wrapping due to current margins, but each JSON object above is actually on one line.

To read the JSON data, issue the following commands in your shell:

``` scala
val jt = ctx.read.json(srcDir + "/tx.jsons")
jt.registerTempTable("jtx")
jt.printSchema
```

Your output should look like the following:

```
scala> val jt = ctx.read.json(srcDir + "/tx.jsons")
jt: org.apache.spark.sql.DataFrame = [amount: double, date: string, desc: string]

scala> jt.registerTempTable("jtx")

scala> jt.printSchema
root
 |-- amount: double (nullable = true)
 |-- date: string (nullable = true)
 |-- desc: string (nullable = true)
```

Hey, where did the `DataFrame` get that schema from?  Well, since the source is JSON, there is type information implicitly available!  In JSON, everything is a string that's wrapped in double-quotes.  Anything else has a definite type that can be inferred!  Here, we see that `amount` has been inferred to be a number, so the `DataFrame` is using `double`.

Now, let's issue the same query as before:  what are the biggest credits in the transaction data?

``` scala
[2209.03,2015-12-15,Deposit - Online Banking Transfer from XXXXXXXXXX SAV]
[1520.0,2015-08-05,Deposit - Shared Branch 5011 W SLAUGHTER LN BLD AUSTIN        TX]
[1360.48,2015-12-15,Deposit - Online Banking Transfer from XXXXXXXXXX CK]
[1000.0,2015-12-07,Deposit - Online Banking Transfer from XXXXXXXXXX CK]
[925.0,2015-12-11,Deposit - Online Banking Transfer from XXXXXXXXXX CK]
[800.0,2015-12-04,Deposit - Online Banking Transfer from XXXXXXXXXX CK]
[750.31,2015-06-19,Deposit - Online Banking Transfer from XXXXXXXXXX SAV]
[650.0,2015-07-26,Deposit - Online Banking Transfer from XXXXXXXXXX CK]
[500.0,2015-07-07,Deposit - Online Banking Transfer from XXXXXXXXXX SAV]
[500.0,2015-07-10,Deposit - Online Banking Transfer from XXXXXXXXXX SAV]
```

Notice that this data is the same data as we got before.  The only difference is the column order.  Pretty cool, n'est-ce pas?

## Conclusion

In this lab, you've seen how you can use conventional SQL to query data stored in a Spark cluster, and that you can import data from many different sources, including other SQL data sources.  This is just the tip of the iceberg:  you can not only query data, but you can modify it, too, or even store `DataFrames` as permanent SQL tables in your cluster.  Further, Spark SQL even supports Hive & HiveQL via, you guessed it, `HiveContext`!  The sky's the limit now!
