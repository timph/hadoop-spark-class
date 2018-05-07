# Developing With RDDs:  Laziness

In this lab, you will familarize with Spark's core abstraction, the `RDD`, and how it defers data processing until absolutely necessary.

## Objectives

1. Open and interact with Spark via Spark's interactive shell.
2. Use the `SparkContext` to get an `RDD`.
3. Use the dataset to familiarize yourself with its basic capabilities, including deferred data processing.

## Prerequisites

This lab assumes that the student is familiar with the course environment, in particular, the Spark distribution.

## Instructions

In order to work with data using Spark, we need to get our hands on a dataset.  Spark calls such a dataset a "resilient distributed dataset", or `RDD`.  There are several ways to get your hands on an `RDD`.  In most cases, however, you're going to use a `SparkContext` to do that.

Let's start with something simple:  a plain, ol' Scala collection of the integers from 1 to 10,000.

``` scala
scala> val numbers = 1 to 10000
numbers: scala.collection.immutable.Range.Inclusive = Range(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, ...
scala>
```

Ok.  We now have our plain, ol' Scala collection.  Next, take a look at your shell.  Did you notice the rather unassuming message `Spark context available as sc.`?  That means that the Spark shell has already created a `SparkContext` instance for you and has bound it to the variable `sc`.  Let's use it to turn our Scala collection into a Spark `RDD`.

``` scala
scala> val rdd = sc.parallelize(numbers)
rdd: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[0] at parallelize at <console>:23
```

That didn't hurt at all, did it?  Ok, we are now ready to slice & dice like a true [data scientist](https://hbr.org/2012/10/data-scientist-the-sexiest-job-of-the-21st-century/).  What are some of the things we might want to do with such a dataset?  Well, let's *see* what we can do.  If you type `rdd.` in the shell, then hit `TAB`, you'll get a list of the methods available to you:

``` scala
scala> rdd.
++                         aggregate                  asInstanceOf               cache
cartesian                  checkpoint                 coalesce                   collect
compute                    context                    count                      countApprox
countApproxDistinct        countByValue               countByValueApprox         dependencies
distinct                   filter                     filterWith                 first
flatMap                    flatMapWith                fold                       foreach
foreachPartition           foreachWith                getCheckpointFile          getStorageLevel
glom                       groupBy                    id                         intersection
isCheckpointed             isEmpty                    isInstanceOf               iterator
keyBy                      localCheckpoint            map                        mapPartitions
mapPartitionsWithContext   mapPartitionsWithIndex     mapPartitionsWithSplit     mapWith
max                        min                        name                       name_=
partitioner                partitions                 persist                    pipe
preferredLocations         randomSplit                reduce                     repartition
sample                     saveAsObjectFile           saveAsTextFile             setName
sortBy                     sparkContext               subtract                   take
takeOrdered                takeSample                 toArray                    toDebugString
toJavaRDD                  toLocalIterator            toString                   top
treeAggregate              treeReduce                 union                      unpersist
zip                        zipPartitions              zipWithIndex               zipWithUniqueId

scala> rdd.
```

Take a moment to look at all of the crazy things we can do.  There are many, but first things `first`:

``` scala
scala> rdd.first
res3: Int = 1
```

Not surprisingly, first returns the first item in the RDD.  What if you want the first n items?  Easy.

``` scala
scala> rdd.take(5)
res4: Array[Int] = Array(1, 2, 3, 4, 5)
```

We can also find out all sorts of other things, like the total number of items or the sum of the elements.

``` scala
scala> rdd.count
res5: Long = 10000

scala> rdd.sum
res6: Double = 5.0005E7
```

### Transformations

All of the operations we've performed so far are called "actions", because they cause Spark to actually process the data.  Another set of methods on `RDD`s is called "transformations", because they don't cause any data processing to happen.  This is one of the key design features of Spark:  it's as lazy as it can be, deferring calculation until it's absolutely required.  What are some of these transformations?  Let's have a look, starting with one of the most basic, `map`.

``` scala
scala> val strings = rdd.map("#" + _)
strings: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[9] at map at <console>:25
```

We just told Spark that we want to convert the `RDD` into a collection of `String`s from its current form of `Int`s.  Now, we haven't actually converted them yet, because we haven't invoked any of the action methods.  This is important, because Spark can save a lot of processing by examining the transformations and only processing the necessary items.  Let's use `first` again to see what happens with this transformation.

``` scala
scala> strings.first
res16: String = #1
```

What's important here is that Spark didn't convert all 10,000 entries into strings with a "#" in front of them, then return the first one.  It recognized that we only needed the first item, so only converted the first one to a String and returned it.  How do we know?  Let's time it, then compare to what would happen if we forced Spark to convert all of the entries to Strings, then return the first one.

``` scala
scala> val start = System.currentTimeMillis; println(rdd.map("#" + _).first); println("took " + (System.currentTimeMillis - start) + " ms")
#1
took 15 ms
start: Long = 1450478213569
```

Ok.  That took around 15 ms.  Now, let's force all elements to be converted before taking the first.

``` scala
scala> val start = System.currentTimeMillis; println(rdd.map("#" + _).collect.head); println("took " + (System.currentTimeMillis - start) + " ms")
#1
took 56 ms
start: Long = 1450478719206
```

Notice how we called `collect` this time, which forced Spark to convert each item, return the collection of Strings to us, then head to return the first item from the collection.  This time it took around 56 ms, which is not a huge difference.  Let's start seeing if we can exaggerate the results by iterating, increasing the size of the original set of numbers each time.  We used 10,000 before; let's go to 100,000.

``` scala
scala> val n = 100000
n: Int = 100000

scala> val numbers = sc.parallelize(1 to n)
numbers: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[11] at parallelize at <console>:23

scala> val start = System.currentTimeMillis; println(numbers.map("#" + _).first); println("took " + (System.currentTimeMillis - start) + " ms")
#1
took 12 ms
start: Long = 1450479577244

scala> val start = System.currentTimeMillis; println(numbers.map("#" + _).collect.head); println("took " + (System.currentTimeMillis - start) + " ms")
#1
took 45 ms
start: Long = 1450479598918
```

Hmm.  12 ms versus 45 ms.  That's not much different.  What do you get on your machine?  Iterate again, using 1,000,000.

``` scala
scala> val n = 1000000
n: Int = 1000000

scala> val numbers = sc.parallelize(1 to n)
numbers: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[14] at parallelize at <console>:23

scala> val start = System.currentTimeMillis; println(numbers.map("#" + _).first); println("took " + (System.currentTimeMillis - start) + " ms")
#1
took 14 ms
start: Long = 1450479614203

scala> val start = System.currentTimeMillis; println(numbers.map("#" + _).collect.head); println("took " + (System.currentTimeMillis - start) + " ms")
#1
took 672 ms
start: Long = 1450479618162
```

Now we're starting to see a difference!  Let's go to 5,000,000.

``` scala
scala> val n = 5000000
n: Int = 5000000

scala> val numbers = sc.parallelize(1 to n)
numbers: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[17] at parallelize at <console>:23

scala> val start = System.currentTimeMillis; println(numbers.map("#" + _).first); println("took " + (System.currentTimeMillis - start) + " ms")
#1
took 16 ms
start: Long = 1450479640772

scala> val start = System.currentTimeMillis; println(numbers.map("#" + _).collect.head); println("took " + (System.currentTimeMillis - start) + " ms")
#1
took 3972 ms
start: Long = 1450479644311
```

Holy huge difference, Batman!  What kind of differences are you seeing on your machine?  Notice how, despite the increase in the size of the collection, the time it took using `first` remained fairly constant.  Spark is clearly being smart about how much work really needs to be done, only transforming and returning a single element instead of the whole one!

### Conclusion

What we've seen in this lab is how Spark is deferring execution until it's absolutely required, and only processing the data that must be processed in order to achieve the desired result.