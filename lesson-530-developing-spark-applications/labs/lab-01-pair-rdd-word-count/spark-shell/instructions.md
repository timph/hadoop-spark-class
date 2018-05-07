## Developing Spark Applications:  PairRDDFunctions

In this lab, you will take your first steps using `PairRDDFunctions` using the Spark REPL.

## Objectives

1. Open and interact with Spark via Spark's interactive shell.
2. Use the `SparkContext` to bootstrap `RDD`s of `Tuple`s in order to use `PairRDDFunctions`.
3. Use the `PairRDDFunctions` to more easily calculate values in the `RDD`.

## Prerequisites

This lab assumes that the student is familiar with the course environment, in particular, the Spark distribution.

## Instructions

### Open the Spark shell

To open the Spark shell, simply change directory to your Spark home and issue the `spark-shell` command.  You should see output similar to the following.

``` sh
$ cd $SPARK_HOME
$ bin/spark-shell
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
15/12/18 15:10:20 WARN Connection: BoneCP specified but not present in CLASSPATH (or one of dependencies)
15/12/18 15:10:20 WARN Connection: BoneCP specified but not present in CLASSPATH (or one of dependencies)
15/12/18 15:10:24 WARN ObjectStore: Version information not found in metastore. hive.metastore.schema.verification is not enabled so recording the schema version 1.2.0
15/12/18 15:10:24 WARN ObjectStore: Failed to get database default, returning NoSuchObjectException
15/12/18 15:10:25 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
15/12/18 15:10:25 WARN Connection: BoneCP specified but not present in CLASSPATH (or one of dependencies)
15/12/18 15:10:25 WARN Connection: BoneCP specified but not present in CLASSPATH (or one of dependencies)
SQL context available as sqlContext.

scala>
```

We're now ready to slice & dice data using Spark!

### Consider `Tuple`s

Scala's support for arbitrary pairs (and triplets, quadruplets, and so on) of data is reflected in Spark.  If an `RDD`'s element is of a `Tuple` type, then, through Spark `implicit`s, that `RDD` gets enriched with `PairRDDFunctions`, which provide convenient means of performing calculations on the `Tuple`s.

### Identify directory containing source data

Check with your course instructor to get the directory containing the source data for this lab.  It's probably at `../../lesson-5xx-resources` relative to this document, but the instructor may have placed it elsewhere.  Once known, declare a value named `srcDir` to be the absolute path to the directory containing the test data.

``` scala
scala> val srcDir = "/.../lesson-5xx-resources"
srcDir: String = /.../lesson-5xx-resources
```

In this example, you'll have something in place of `…`.

### Get an `RDD` of `Tuple`s

Our goal is to count how many ifs, ands & buts there are in the file, so if we could somehow get a `Map` of word counts keyed on the word, we'd be golden.

The first thing we need to do is to create an `RDD` that contains the words in the file.  Open the file `lab.scala` in a text editor; notice that is already has loaded the file for you in the variable called `lines`.  Your next step is to split each line with the provided `delimiters` so that you end up with a flat collection of words in the file.  Whenever we want to flatten a collection of collections, we can use `flatMap` to do so.

``` scala
val words = lines.flatMap(_.split(delimiters));
```

Load this file into the Spark shell and see what we have so far.  It should look something like the following.

``` scala
scala> :load /.../lesson-530-developing-spark-applications-with-scala/lab-01-pair-rdd-word-count/lab.scala
Loading /.../lesson-530-developing-spark-applications-with-scala/lab-01-pair-rdd-word-count/lab.scala...
file: String = /.../lesson-5xx-resources/sherlock-holmes.txt
delimiters: Array[Char] = Array( , ,, ., :, ;, ?, !, -, @, #, (, ), ", *)
lines: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[99] at textFile at <console>:52
words: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[100] at flatMap at <console>:56
```

It just so happens that `PairRDDFunctions` has a method called `countByKey`, which returns a `Map` of counts of the occurrence of each key value among the `Tuple`s in the collection by the key.  In our case, the key in the `Tuple` is the word, and the value will be the count.  All we need to do is to convert our collection of words into a collection of `Tuple`s, and it doesn't even matter what the value is, since we're only going to count keys!  Let's create an `RDD` whose element type is `(String,None)` by `map`ping the collection of words to `Tuple`s and, for better accuracy, converting words to lower case while we're at it:

``` scala
val pairs = words.map(word => (word.toLowerCase, None));
```

Load this file again into the Spark shell and see what we have so far.  You should an additional line like the following:

``` scala
pairs: org.apache.spark.rdd.RDD[(String, None.type)] = MapPartitionsRDD[104] at map at <console>:58
```

### Use `countByKey`

Now that we have an `RDD` of `Tuple`s of type `(String,None)`, we get `PairRDDFunctions` via an `implicit`.  Let's use `countByKey` to get the count of each key (or word), then see how many ifs, ands & buts there are.

``` scala
val counts = pairs.countByKey();

val ifs = counts("if");
val ands = counts("and");
val buts = counts("but");
```

Since we have our desired solution, let's run it.  You should see the counts at the end of Spark shell's output.

``` scala
counts: scala.collection.Map[String,Long] = Map(professed -> 6, hyoscin -> 1, metacarpals -> 2, chary -> 1, _cystic -> 5, phosphates -> 1, incident -> 27, serious -> 141, brink -> 4, ferociously -> 1, regularizing -> 1, youthful -> 16, sinister -> 16, comply -> 19, ebb -> 2, breaks -> 17, forgotten -> 64, precious -> 38, boxers -> 2, compliment -> 7, inflammatory -> 45, oesophagus -> 5, unkempt -> 1, fiancee's -> 2, embedded -> 26, respecting -> 15, cystica_ -> 1, 710 -> 1, lover -> 24, jena -> 5, plentiful -> 7, evanescent -> 8, malignant -> 80, pasture -> 4, cweep -> 1, speaker -> 22, interossei -> 2, htm -> 1, periodicity -> 1, terrible -> 195, lion -> 3, latterly -> 9, rate -> 67, morio -> 4, ellet -> 1, 205 -> 11, pepper -> 2, inevitable -> 61, nonetheless -> 1, perceives -> 1, met...
ifs: Long = 2366
ands: Long = 38288
buts: Long = 5639
```

## Conclusion

In this lab, we saw how an `RDD` of `Tuple` types receives via an `implicit` additional methods from `PairRDDFunctions` to make calculating certain values easier.