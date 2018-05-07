# Hello, Spark!

In this lab, you will take your first steps using Spark.

## Objectives

1. Open and interact with Spark via Spark's interactive shell.
2. Use the `SparkContext` to read a file.
3. Use the dataset from the file to perform a count of each word in the file.
4. Get setup using Jupyter Notebooks
5. Run Jupyter Notebooks with Python and Scala

## Prerequisites

This lab assumes that the student is familiar with the course environment, in particular, the Spark distribution.

## Instructions

### Open the Spark shell

In our first endeavor to interact with Spark, we need to open the Spark shell.  Simply change directory to your Spark home and issue the command  `bin/spark-shell`:

``` sh
$ cd $SPARK_HOME
$ bin/spark-shell --master local[*]
```
The option `--master local[*]` instructs Spark to use as many threads as there are cores.

**Note:** On some PayPal machines, there is an exception when you start Spark. The remedy is to add an environment variable:

```
export SPARK_LOCAL_IP=127.0.0.1
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

Awesome — we're now ready to slice & dice data using Spark!

## Hello, World!

The classic hello world exercise in big data has got to be word count, where you count the words in a given body of text.  Unless you've been living under a rock, we've got just the text for you:  Adele's song, "Hello", which had over 770 million views on [YouTube](https://www.youtube.com/watch?v=YQHsXMglC9A) in just its first month.  I guess that means our exercise should really be called "Hello, Adele!"

We've put the lyrics of the song into a text file for you.  It's called [`hello-adele.txt`](hello-adele.txt), and it is in your lab folder.  Have a quick look at it, maybe sing it if you're inclined, then pop back here, where we'll do our "Hello, Adele" exercise.

### Load the file

In order to work with data using Spark, we need to get our hands on that data.  Spark calls such a thing a "resilient distributed dataset", or `RDD`.  There are several ways to get your hands on an `RDD`.  In most cases, however, you're going to use a `SparkContext` to do that.  

Did you notice the rather unassuming message in your REPL with the text `Spark context available as sc.`?  That means that Spark has already created the  `SparkContext` for you and has bound it to the variable `sc`.  Let's use it to turn Adele's lyrics into a Spark `RDD` by calling `SparkContext`'s `textFile` method to load the text file containing the lyrics.

To make things easy, copy the file `hello-adele.txt` into the `$SPARK_HOME` directory. This will avoid a possibly lengthy path to the file.

``` scala
scala> val lines = sc.textFile("hello-adele.txt")
lines: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[1] at textFile at <console>:21
```

We called the value `lines` because `textFile` returns an `RDD` that represents a collection of strings, one for each line of the text file.  Let's confirm this by getting some of the values from the `RDD`:

``` scala
scala> lines.collect()(0)
res4: String = Hello, it's me, I was wondering

scala> lines.collect()(1)
res5: String = If after all these years you'd like to meet to go over everything
```

As you can see, the `RDD` represents a collection of lines in the text file.

The `collect()` method brings the values to the driver program. This is needed when we run on a cluster and an `RDD` is spread over multiple machines.

### Start counting

Ok, now let's start counting.

``` scala
scala> lines.map(_.split(" ")).count
res7: Long = 55
```

Here, we've used the `map` method to transform the contents of each element in the source collection (lines of text) into an `Array` of words.  Now, does 55 words sound right?  Not really.  How many *lines* are in the file, anyway?  Coincidentally, it's 55!  Wait a minute.  What kind of collection is `map` returning, anyway?  Let's have a look.

``` scala
scala> lines.map(line => line.split(" ")).collect()(0)
res8: Array[String] = Array(Hello,, it's, me,, I, was, wondering)

scala> lines.map(line => line.split(" ")).collect()(1)
res9: Array[String] = Array(If, after, all, these, years, you'd, like, to, meet, to, go, over, everything)
```

Aha!  Our `map` operation is transforming each element of the source `RDD`  (a line of text) into the return type of the function we're giving to `map`, which is `Array[String]`, because `split` returns `Array[String]`!  How do we flatten out all of the arrays so that we end up with what we wanted in the first place?  It turns out that the `flatMap` method does just that.  Let's try it.

``` scala
scala> lines.flatMap(line => line.split(" ")).collect()(0)
res13: String = Hello,
```

That's better!  We now have transformed a collection of `String`s into a collection of `Array[String]`s, then flattened those into a new collection of `String`s, one per word.  Let's refine our word delimiters a bit and then count things up.

``` scala
scala> lines.flatMap(line => line.split(Array(' ',',','.','?','!',';'))).count
res14: Long = 348
```

Et voila!  There are 348 words in the file.  But wait, we wanted to know the count of *each* word in the file; in other words, how many times did each word show up in the file?

### Count by word

We're going to need a new set of operations to do that.  Let's have a look.

``` scala
scala> lines.flatMap(line => line.split(Array(' ',',','.','?','!',';'))).map(word => (word.toLowerCase,1)).reduceByKey((val1,val2) => val1+val2).collect()

res19: Array[(String, Int)] = Array((hear,1), (call,3), (there's,1), (healing,1), (town,1), (tear,3), (miles,1), (never,3), (doesn't,3), (tried,3), (we,2), (california,1), (who,1), (talk,1), (hello,9), (over,1), (ever,2), (they,1), (go,1), (make,1), (years,1), (heart,3), (out,2), (from,6), (other,3), (well,1), (me,3), (world,1), (us,2), (free,1), (are,2), (after,1), (don't,3), (million,1), (typical,1), (hope,1), (can,4), (heal,1), (times,3), (when,4), (feet,1), (forgotten,1), (between,1), (how,2), (secret,1), (our,1), (done,4), ("",29), (seem,3), (apart,3), (thousand,3), (you'd,1), (ooh,3), (so,2), (myself,1), (clearly,3), (it,8), (yeah,1), (about,2), (least,3), (outside,3), (nothing,1), (i,12), (such,1), (meet,1), (before,1), (difference,1), (at,4), (in,1), (side,3), (supposed,1), (for...
```

Ok.  Now, we're getting somewhere!  We added another transformation via `map`, this time that transforms each word into a `Tuple` of type `(String,Int)` of its lower case form and the constant `1`, then, used `reduceByKey` to take each `Tuple` and add the values for each key, which in this case is the word.  This results in the count of each word.

### Ordering

Now that we've gotten our word counts, let's see which word appears the most in the file by adding some ordering.

``` scala
scala> lines.flatMap(line => line.split(Array(' ',',','.','?','!',';'))).map(word => (word.toLowerCase,1)).reduceByKey((val1,val2) => val1+val2).sortBy(tuple => tuple._2, ascending = false).collect()

res24: Array[(String, Int)] = Array(("",29), (you,15), (to,14), (i,12), (that,10), (hello,9), (it,8), (i'm,8), (the,8), (anymore,7), (sorry,7), (i've,7), (but,7), (from,6), (for,6), (tell,6), (a,5), (can,4), (when,4), (done,4), (at,4), (be,4), (say,4), (everything,4), (of,4), (call,3), (tear,3), (never,3), (doesn't,3), (tried,3), (heart,3), (other,3), (me,3), (don't,3), (times,3), (seem,3), (apart,3), (thousand,3), (ooh,3), (clearly,3), (least,3), (outside,3), (side,3), (called,3), (your,3), (breaking,3), (matter,3), (home,3), (must've,3), (it's,3), (we,2), (ever,2), (out,2), (us,2), (are,2), (how,2), (so,2), (about,2), (and,2), (hear,1), (there's,1), (healing,1), (town,1), (miles,1), (california,1), (who,1), (talk,1), (over,1), (they,1), (go,1), (make,1), (years,1), (well,1), (world,1)...
```

Notice that we added a `sortBy` call, which takes a function that takes an element and returns the data on which we're going to sort.  In this case, we want to sort by the count, which is the second element in the `Tuple` (hence the method `_2`), and do it in a descending manner (hence `ascending = false`).

Our sorting shows us that there are more words composed of blank strings than there are any other words in the file.  That's not what we meant!  Let's filter out empty strings and recalculate.

### Filtering

To filter out empty strings from our result, we'll use the aptly named `RDD` method `filter`.

``` scala
scala> lines.flatMap(line => line.split(Array(' ',',','.','?','!',';'))).filter(word => word.trim.length > 0).map(word => (word.toLowerCase,1)).reduceByKey((c1,c2) => c1+c2).sortBy(tuple => tuple._2, ascending = false).collect

res27: Array[(String, Int)] = Array((you,15), (to,14), (i,12), (that,10), (hello,9), (it,8), (i'm,8), (the,8), (anymore,7), (sorry,7), (i've,7), (but,7), (from,6), (for,6), (tell,6), (a,5), (can,4), (when,4), (done,4), (at,4), (be,4), (say,4), (everything,4), (of,4), (call,3), (tear,3), (never,3), (doesn't,3), (tried,3), (heart,3), (other,3), (me,3), (don't,3), (times,3), (apart,3), (seem,3), (thousand,3), (ooh,3), (clearly,3), (least,3), (outside,3), (side,3), (called,3), (your,3), (breaking,3), (matter,3), (home,3), (must've,3), (it's,3), (we,2), (ever,2), (out,2), (us,2), (are,2), (how,2), (so,2), (about,2), (and,2), (hear,1), (there's,1), (healing,1), (town,1), (miles,1), (california,1), (who,1), (talk,1), (over,1), (they,1), (go,1), (make,1), (years,1), (well,1), (world,1), (free,1...
```

We added the transformation `filter(word => word.trim.length > 0)`, which includes only those elements for which the filter condition evaluates to `true`.  Now, we see that the word `you` is the most often used word in the song.  Let's reduce some verbosity by leveraging Scala's wildcard character, `_`, and take only the top 5 results.

``` scala
scala> lines.flatMap(_.split(Array(' ',',','.','?','!',';'))).filter(_.trim.length > 0).map(w => (w.toLowerCase, 1)).reduceByKey(_ + _).sortBy(_._2, false).take(5)
res30: Array[(String, Int)] = Array((you,15), (to,14), (i,12), (that,10), (hello,9))
```

There you have it.  A big data "Hello, World!"


## Conclusion

In this exercise, we saw that we can process raw data in a straightforward and readable manner using a combination of transformations like `flatMap`, `map`, `filter` & `sortBy` and actions like `collect` and `take`.

We also played with the Spark Shell. We generally recommend the use of Notebooks for this class, but it is your choice. You can use the Spark Shell or the Notebooks. It is entirely up to you.
