# Developing with RDDs:  Parallelism

In this lab, you will take your first steps using RDDs in Spark using the Spark REPL.

## Objectives

1. Open and interact with Spark via Spark's interactive shell.
2. Use the `SparkContext` to bootstrap `RDD`s.
3. Use the `RDD`s to explore parallelism & its benefits when processing data.

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

### Consider parallelism

Whenever we process big data, we need to consider the degree to which we'll be able to use parallelism.  The general idea is that if we can break our job down into smaller and smaller chunks that can execute in parallel, we should perform better.  In this lab, we're going to test that theory by methodically increasing the degree of parallelism when performing a word count calculation to get the most frequently used word in some text file.

### Identify directory containing source data

Check with your course instructor to get the directory containing the source data for this lab.  It's probably at `../../lesson-5xx-resources` relative to this document, but the instructor may have placed it elsewhere.  Once known, declare a value named `srcDir` to be the absolute path to the directory containing the test data.

``` scala
scala> val srcDir = "/.../lesson-5xx-resources"
srcDir: String = /.../lesson-5xx-resources
```

In this example, you'll have something in place of `…`.

### Add code to control the degree of parallelism

Now, open the lab source file, `lab.scala` in any text editor.  Notice the line beginning with `val original = sc.textFile(srcDir + "/" + file);`.  That gives us an `RDD` with a default number of partitions, which is directly related to the degree of parallel computation that this `RDD` has.

Next, look at the line following the first `TODO` comment; it begins with `val partitioned = ` and is left unimplimented.  Notice that this code is encased in a `foreach` loop that varies in the variable `n` the number of partitions under test.  Replace `???` with a call to `original.repartition(n)`, which will cause Spark to create a new `RDD` with the given number of partitions, which effectively determine the number of cores that we'll be executing on in this standalone example.  Your line should look like the following.

``` scala
val partitioned = original.repartition(n);
```

### Add the word count `RDD` code

Now that we are iterating on the parallelism of our `RDD`, we need to actually do something that's going to leverage it.  This lab will use the quintessential big data problem known as "word count" to find the word used the most in a given file of text.

Look for the second `TODO` comment.  Below it, you'll see the line `var result = partitioned.???`.  Replace it with our word count code, given below:

``` scala
val result = partitioned.flatMap(_.split(delimiters)).filter(_.trim.length > 0).map(x => (x, 1)).reduceByKey(_ + _).sortBy(_._2, false).first;
```

As you can see, we're doing a fairly straightforward word count and returning the most frequently used word.  The difference is that in each iteration, we're increasing the degree of parallelism that's used each time we calculate it.

### Execute and observe timings

We're ready to roll now.  Execute the Spark script by identifying the absolute path of your `lab.scala` file, and issuing the command

``` scala
scala> :load /.../lesson-520-developing-with-rdds/lab-02-parallelism/lab.scala
```

Note again that `…` will be something specific to your machine.  You should see something similar to the following.

``` scala
Loading /.../lesson-520-developing-with-rdds/lab-02-parallelism/lab.scala...
minCores: Int = 2
nCores: Int = 8
partitions: scala.collection.immutable.Range = Range(2, 4, 6, 8)
files: Array[String] = Array(byu-corpus.txt)
delimiters: Array[Char] = Array( , ,, ., :, ;, ?, !, -, @, #, (, ))
File: byu-corpus.txt
Partitions: 2
Took: 5848 ms
Partitions: 4
Took: 3601 ms
Partitions: 6
Took: 3580 ms
Partitions: 8
Took: 2979 ms
```

### Conclusion

What do you notice about our timings?  Do they confirm or refute our hypothesis that more parallelism generally means better performance?  If so, great!  If not, what do you suppose are other factors that are influencing our outcome?  Discuss with your instructor and classmates!
