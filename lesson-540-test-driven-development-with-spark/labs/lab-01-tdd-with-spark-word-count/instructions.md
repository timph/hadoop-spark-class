## Test-Driven Development with Spark

In this lab, you will see how to employ the strategy of test-driven development (TDD) with Spark.

## Objectives

1. Use SBT to test & build a Scala Spark project.
2. Write tests that ensure your `RDD`s are behaving properly.

## Prerequisites

This lab assumes that the student is familiar with the course environment, in particular, the Spark distribution.  The student may use any text editor; a Scala IDE is optional.

## Instructions

If we're employing TDD dogmatically, then we should start by writing tests first, then our application code.  This lab includes a complete Scala `sbt`-based project.  The boilerplate code has been provided already; all that you'll have to do is to fill in the important parts.

### Understand the Problem

Before we begin coding, it's important that you understand a little bit about its premise.  We're going to use the familiar word count problem to illustrate how you can test your `RDD`s before using them in your applications.  In this problem space, there are 3 high-level things that we'll need to do:

1. Read lines from a text file.
2. Split lines into words.
3. Count the number of times each word appears.

Keeping these three things in mind, let's begin!

### Write tests

Dogmatic TDD demands that we start by writing tests.  Considering the three tasks above, let's not worry about reading lines from a text file.  That functionality is provided by the `SparkContext` itself, so we'll assume that it's working fine and that the Spark development team has done their job testing it.  That brings us to task number two:  split lines into words.

#### Write a test for splitting lines into words

Open file `src/test/scala/com/example/wordcount/CountWordsTestSpec.scala`, which contains the declaration for our test suite class, `CountWordsTestSpec`.  Notice that it already has definitions for our test `SparkContext` and some dummy test data, `lines` and `words`, as well as some housekeeping code to shut down our `SparkContext` cleanly in the method `afterAll`.

Our first test, `"wordsFrom" should "convert lines into words"`, has been stubbed out for us.  Here's the implementation that we'll need to fill it in with:

``` scala
CountWords.wordsFrom(lines).count should be (6)
```

Let's take a moment to look at this.  `CountWords` is the name of the `object` under test here, and `wordsFrom` is the name of the method that we're calling in the test, and we're going to pass it some lines of text, specifically, our dummy data of text lines called `lines`.  Since that method is going to return an `RDD` when we're done, we need to actually have it process data to ensure that it's behaving as expected, hence the call to `count`.  Why are we expecting `count` to return 6?  Well, that's how many words there are in our dummy data,  `lines`!

Ok.  We're ready to roll.  Open a terminal window and change to this lab's directory and run the command `sbt test`.  This will cause `sbt` to build our project and run all tests found in it.  You should output similar to the following.

``` shell
$ sbt test
[info] Loading project definition from /.../lab-01-tdd-with-spark-word-count/project
[info] Set current project to lab-01-tdd-with-spark-word-count (in build file:/.../lab-01-tdd-with-spark-word-count/)
[info] Compiling 1 Scala source to /.../lab-01-tdd-with-spark-word-count/target/scala-2.11/test-classes...
[error] /.../lab-01-tdd-with-spark-word-count/src/test/scala/com/example/wordcount/CountWordsTestSpec.scala:18: value count is not a member of Nothing
[error]     CountWords.wordsFrom(lines).count should be (6)
[error]                                 ^
[error] one error found
[error] (test:compile) Compilation failed
```

>  On some machines, running Spark will result in an exception `java.net.BindException: Failed to bind to...`. You can fix that by running the following commands on your terminal:
```
export  SPARK_MASTER_IP=127.0.0.1
export  SPARK_LOCAL_IP=127.0.0.1
```
Then  run `sbt test` again.

Oh, no!  An error!  Let's figure out what happened.  We're getting an error that `count` is not a member of `Nothing`.  That's correct:  `count` isn't a member of `Nothing`!  Compilers are rarely, if ever wrong anyway, so let's assume that the error is due to our code.  Where is `Nothing` coming from?  Well, whatever is being returned from `CountWords.wordFrom` is returning `Nothing`, so lets' go have a look.

Open file `src/main/scala/com/example/wordcount/CountWords.scala`, where we're defining our application `object` `CountWords`, and have a look at the `def`inition of the `wordsFrom` method.  It's set to `???`, which is a Scala shortcut for, you guessed it, `Nothing`.  We're effectively saying that the method isn't implemented yet!  Let's implement it now.

#### Implement the method under test

Here's where things get good when using TDD.  Instead of writing the usual code for the first step of a word count problem as simply `sc.textFile("...").flatMap(_.split(" ")).filter(_.trim.length > 0)`, we're going to make a named function out of it.  First, realize that the point of this method is to split lines into words, so we need to take an `RDD` of lines instead of calling `sc.textFile("...")`.  This also lets us pass our dummy test data in easier, too.  Now, replace the `???` with an actual implementation, so that your code looks like this:

``` scala
def wordsFrom(lines: RDD[String]) =
  lines.flatMap(_.split(delimiters)).filter(_.trim.length > 0)
```

Nice, right?  This has the added benefits of not only being testable, but also of being more descriptive for the next guy reading our code!

Let's pop over to the terminal and run `sbt test` again to see what happens this time:

``` shell
[info] CountWordsTestSpec:
[info] wordsFrom
[info] - should convert lines into words
[info] wordCountsFrom
[info] - should count each word *** FAILED ***
[info]   scala.NotImplementedError: an implementation is missing
[info]   at scala.Predef$.$qmark$qmark$qmark(Predef.scala:225)
[info]   at com.example.wordcount.CountWordsTestSpec$$anonfun$2.apply(CountWordsTestSpec.scala:22)
[info]   at com.example.wordcount.CountWordsTestSpec$$anonfun$2.apply(CountWordsTestSpec.scala:22)
[info]   at org.scalatest.Transformer$$anonfun$apply$1.apply$mcV$sp(Transformer.scala:22)
[info]   at org.scalatest.OutcomeOf$class.outcomeOf(OutcomeOf.scala:85)
[info]   at org.scalatest.OutcomeOf$.outcomeOf(OutcomeOf.scala:104)
[info]   at org.scalatest.Transformer.apply(Transformer.scala:22)
[info]   at org.scalatest.Transformer.apply(Transformer.scala:20)
[info]   at org.scalatest.FlatSpecLike$$anon$1.apply(FlatSpecLike.scala:1647)
[info]   at org.scalatest.Suite$class.withFixture(Suite.scala:1122)
[info]   ...
[info] fileWordCounts
[info] - should count words in file *** FAILED ***
[info]   scala.NotImplementedError: an implementation is missing
[info]   at scala.Predef$.$qmark$qmark$qmark(Predef.scala:225)
[info]   at com.example.wordcount.CountWordsTestSpec$$anonfun$3.apply(CountWordsTestSpec.scala:25)
[info]   at com.example.wordcount.CountWordsTestSpec$$anonfun$3.apply(CountWordsTestSpec.scala:25)
[info]   at org.scalatest.Transformer$$anonfun$apply$1.apply$mcV$sp(Transformer.scala:22)
[info]   at org.scalatest.OutcomeOf$class.outcomeOf(OutcomeOf.scala:85)
[info]   at org.scalatest.OutcomeOf$.outcomeOf(OutcomeOf.scala:104)
[info]   at org.scalatest.Transformer.apply(Transformer.scala:22)
[info]   at org.scalatest.Transformer.apply(Transformer.scala:20)
[info]   at org.scalatest.FlatSpecLike$$anon$1.apply(FlatSpecLike.scala:1647)
[info]   at org.scalatest.Suite$class.withFixture(Suite.scala:1122)
[info]   ...
...
[info] Run completed in 3 seconds, 306 milliseconds.
[info] Total number of tests run: 3
[info] Suites: completed 1, aborted 0
[info] Tests: succeeded 1, failed 2, canceled 0, ignored 0, pending 0
[info] *** 2 TESTS FAILED ***
[error] Failed tests:
[error] 	com.example.wordcount.CountWordsTestSpec
[error] (test:test) sbt.TestsFailedException: Tests unsuccessful
```

Foiled again!  There are still errors.  Wait a minute:  one test *didn't* fail.  Which one was it?  The output shows

``` shell
[info] CountWordsTestSpec:
[info] wordsFrom
[info] - should convert lines into words
```

with no errors.  Hey, that's the one we just fixed!  So why are the other two tests failing?  If you noticed the message `an implementation is missing`, then you got it.  We simply haven't implemented those two tests yet!  Look again in class `CountWordsTestSpec` and notice that our other two tests using the Scala "unimplemented" placeholder, `???` .  All we have to do now is plug & chug, implementing the rest of our tests & application code.

#### Implement the next test & the application code for it

Let's implement the next test and its corresponding application code for `wordCountsFrom`.  Replace the `???` after `"wordCountsFrom" should "count each word" in` with the actual test body, so that the declaration looks like this:

``` scala
  "wordCountsFrom" should "count each word" in {
    val counts = CountWords.wordCountsFrom(words).collect
    counts should contain ("word1", 2)
    counts should contain ("word2", 1)
    counts should contain ("word3", 1)
  }
```

Next, implement the application code for the method we're testing, in class `CountWords`, so that it looks like this:

``` scala
def wordCountsFrom(words: RDD[String]) =
  words.map((_, 1)).reduceByKey(_ + _).sortBy(_._2, false)
```

Save all of your work and run `sbt test` again.  You should see something similar to the following.

``` shell
[info] CountWordsTestSpec:
[info] wordsFrom
[info] - should convert lines into words
[info] wordCountsFrom
[info] - should count each word
[info] fileWordCounts
[info] - should count words in file *** FAILED ***
[info]   scala.NotImplementedError: an implementation is missing
[info]   at scala.Predef$.$qmark$qmark$qmark(Predef.scala:225)
[info]   at com.example.wordcount.CountWordsTestSpec$$anonfun$3.apply(CountWordsTestSpec.scala:30)
[info]   at com.example.wordcount.CountWordsTestSpec$$anonfun$3.apply(CountWordsTestSpec.scala:30)
[info]   at org.scalatest.Transformer$$anonfun$apply$1.apply$mcV$sp(Transformer.scala:22)
[info]   at org.scalatest.OutcomeOf$class.outcomeOf(OutcomeOf.scala:85)
[info]   at org.scalatest.OutcomeOf$.outcomeOf(OutcomeOf.scala:104)
[info]   at org.scalatest.Transformer.apply(Transformer.scala:22)
[info]   at org.scalatest.Transformer.apply(Transformer.scala:20)
[info]   at org.scalatest.FlatSpecLike$$anon$1.apply(FlatSpecLike.scala:1647)
[info]   at org.scalatest.Suite$class.withFixture(Suite.scala:1122)
[info]   ...
...
[info] Run completed in 3 seconds, 544 milliseconds.
[info] Total number of tests run: 3
[info] Suites: completed 1, aborted 0
[info] Tests: succeeded 2, failed 1, canceled 0, ignored 0, pending 0
[info] *** 1 TEST FAILED ***
[error] Failed tests:
[error] 	com.example.wordcount.CountWordsTestSpec
[error] (test:test) sbt.TestsFailedException: Tests unsuccessful
[error] Total time: 10 s, completed Dec 29, 2015 10:12:11 AM
```

Ok.  Looking better.  Only 1 failed test, and it's because it's not implemented.  Let's finish up.

#### Write final test & application code

Change our last test method, `"fileWordCounts" should "count words in file"`, to be the following:

``` scala
  "fileWordCounts" should "count words in file" in {
    val results = CountWords.wordCounts(lines).collect
    results should contain ("line", 3)
    results should contain ("one", 1)
    results should contain ("two", 1)
    results should contain ("three", 1)
  }
```

For our last step, we're going to use Scala's functional composition and support for partial functions to define our last method, which combines our other two functions to achieve our goal.  In `CountWords`, replace the declaration of `wordCounts` to be

``` scala
def wordCounts = wordsFrom _ andThen wordCountsFrom _
```

This is Scala syntax for combining two functions.  Make sure you include the trailing `_` character, which is Scala's way of saying that you're defining a partial function.  Now, if we pop back out to the terminal and run `sbt test` again, we should see nothing but rainbows & butterflies:

``` shell
[info] CountWordsTestSpec:
[info] wordsFrom
[info] - should convert lines into words
[info] wordCountsFrom
[info] - should count each word
[info] fileWordCounts
[info] - should count words in file
...
[info] Run completed in 3 seconds, 698 milliseconds.
[info] Total number of tests run: 3
[info] Suites: completed 1, aborted 0
[info] Tests: succeeded 3, failed 0, canceled 0, ignored 0, pending 0
[info] All tests passed.
```

Hooray!   All tests passed!

#### Bonus step:  expose word count functionality

Now that we've got this great word count functionality implemented, let's expose it in a nice Scala way.  Let's add the following code to the end of our `CountWords` class:

``` scala
    // default method with required parameters
  def apply(path:String)(implicit sc:SparkContext) = wordCounts(sc.textFile(path))

  // main method
  def main(args: Array[String]) {
    implicit val sc = new SparkContext(
      new SparkConf().setAppName("CountWords").setMaster("local[*]"))
    val counts = Try(apply(args(0)).collect)
    sc.stop
    counts match {
      case Success(wc) => wc.foreach(println(_))
      case Failure(e) => println("error: " + e.getMessage)
    }
  }
```

Here, we've added a nice `apply` method that invokes our functionality on a given file.  Next, we've added a `main` method to invoke it from the command line.  We've included a convenient script called `wc` that invokes the main class for you.  Go ahead and give it a try, like this:

``` shell
$ ./wc
```

You should see output similar to the following:

``` shell
[info] Loading project definition from /Users/matthew/Documents/github/SciSpike/SciSpike-Courses/big_data_paypal/lesson-540-test-driven-development-with-spark/solution-01-tdd-with-spark-word-count/project
[info] Set current project to solution-01-tdd-with-spark-word-count (in build file:/Users/matthew/Documents/github/SciSpike/SciSpike-Courses/big_data_paypal/lesson-540-test-driven-development-with-spark/solution-01-tdd-with-spark-word-count/)
[info] Running com.example.wordcount.CountWords ../../lesson-5xx-resources/sherlock-holmes.txt
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
...
(the,79393)
(of,40023)
(and,38287)
(to,28752)
(in,22020)
(a,21095)
(he,12249)
(that,12194)
(was,11409)
(it,10250)
(his,10033)
(is,9763)
...
```

You can also run the script giving some other path.  Try it out!

## Conclusion

What we've seen in this lab is that, by using test-driven development techniques, we can ensure that our `RDD`s are behaving as expected, and that we have much more descriptive names for each step in our data processing pipeline.
