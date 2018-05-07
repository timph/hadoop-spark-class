# Suspicious Purchase Amounts

In this lab, you will see how to use Spark Streaming to identify suspicious activity from a stream of purchase data.

## Objectives

1. Use SBT to build a Scala Spark project.
2. Use Spark Streaming to watch an incoming stream of transaction data to determine the presence of anomalies.

## Prerequisites

This lab assumes that the student is familiar with the course environment, in particular, the Spark distribution and `sbt`, Scals's simple build tool.  The student may use any text editor; an IDE (Scala IDE, JetBrains IntelliJ, etc) is recommended, but not required.

## Instructions

We're going to take a look (ok, a *naive* look) at how Spark Streaming can be used to watch for fraudulent financial activity.  To do so, we're going to stream simplified transaction data to our program, which will, in turn, continuously calculate statistical information about the stream and use it to identify potentially fraudulent activity.

### Open the project

First off, we have to open the project.  If you're using a plain text editor like Atom or Sublime, use it to open the lab directory, `lab-01-suspicious-purchase-amounts`.  If you're using an IDE like Scala IDE (based on eclipse) or IntelliJ, create or import a project based on the file `lab-01-suspicious-purchase-amounts/build.sbt`.

In either case, we'll be building & running from the command line, so go ahead and open a new terminal window and change to the lab directory.

### Familiarize yourself with the project

This is a standard single-module `sbt` project, with the project definition in `build.sbt` and the `project` directory, application sources in `src/main` and test sources in `src/test`.  The application is contained primarily in the file `src/main/scala/com/example/transactions/SuspiciousPurchaseAmounts.scala`. Our build definition, seen in `build.sbt`, includes `spark-core` as well as `spark-streaming` as dependencies.

In that file, you'll find two `case class`es, `Tx` & `State`, and the application `object`, `SuspiciousPurchaseAmounts`.  You should see a `// TODO` comment in the `main` method of `object`  `SuspiciousPurchaseAmounts`; this is where we're going to do all of our work in this lab.

#### Helper classes

First, let's understand the two helper classes that have been provided for you.  The first is `case class Tx`.  It's just a simple case class that represents our transaction data, which (hopefully obviously) simply has a date called `date` as a `String` for simplicity, a description called `desc`, and an amount called `amount` of type `Double`, again, for simplicity.

> Note:  In real applications, don't use `Float` or `Double` to represent monetary amounts due to rounding.  Instead, use `BigDecimal` or a currency library to represent financial data for their arbitrary precision!

The second helper class we have is called `State`.  This represents the state that we're going to maintain, calculated from our stream of `Tx`s.  All it contains is a running count and sum of the of the data seen, and lastly, the mean of that data.  We've also provided a couple of handy-dandy methods called `+` that we'll use later on.

The last helper class that you don't really need to understand is called `TxPump`, and it serves as a temporary, self-contained source of streaming `Tx` objects.

#### Main class

As mentioned before, our main object is `SuspiciousPurchaseAmounts`.  In its `main` method, you can see that we're doing some initial setup, argument processing, and finally bootstrapping our context.  There are a couple of things to note about our context.

First, the context is not a vanilla `SparkContext`; instead, it's a `StreamingContext`.  This enables us to handle data that's streaming into our cluster.

Second, we're using a temporary, self-contained means to stream data to our app that queues up `RDD`s of `Tx` objects, which the `StreamingContext` will ultimately pick up thanks to our `TxPump`.

Last, note that we've already provided the lifecycle calls, `start` & `stop`, on the `StreamingContext`.  Now, let's get to work!

#### Do a little bookkeeping

Before we can get to the meat of our program, we have to take care of one minor bookkeeping item:  checkpointing.  Since we're maintaining state in our streaming application, we need to ensure that we've told our `StreamingContext` where to save its state, or `checkpoint`, while running.

> Note:  In stateless streaming applications, checkpointing is optional.

Checkpointing periodically saves data & metadata about the currently executing context and allows the driver program, if terminated, to resume from where it left off.  In our example, just to keep it simple, we're going to start fresh each time.  Add the following call, right after our declaration of the `checkpointDir`, to ensure checkpointing is turned on in our streaming application:

``` scala
ctx.checkpoint(checkpointDir)
```

#### Identify our `DStream`

Next, take a look at the line

``` scala
val rawTxs = ctx.queueStream(queue)
```

Notice that `queueStream` returns a `DStream`.  That should make perfect sense, since a `DStream` is internally represented as a sequence of `RDD`s, and that's exactly what the variable `queue` is!  Ok, so `rawTxs` is our first `DStream`.

#### Create function to update running stats

The first thing we'll need to in our data processing pipeline is to figure out how we're going to maintain our state.  It turns out that `DStream`, when its contained type is a `Tuple`, has a convenient method called `updateStateByKey` (provided by `PairDStreamFunctions`).  Its signature looks like this:

``` scala
def updateStateByKey[S](updateFunc: (Seq[V], Option[S]) ⇒ Option[S])(implicit arg0: ClassTag[S]): DStream[(K, S)]
```

Here,

* `S` represents our state type `State`,
* `V` represents the type of the value in our `DStreams`' `Tuple`s, and
* `K` is the type of the key in our `DStreams`' `Tuple`s.

Wait a minute!  Our `DStream` doesn't contain `Tuple`s, it contains `Tx` objects!  Well, that's true, but we're going to pull a dirty little mapping trick here, and transform the `DStream[Tx]` to a `Stream[(K, V)]`.  `V` will certainly be `Tx`, but what will `K` be?  Well, since we're only doing it so that we can easily sum up all of the `Tx`s in each batch, we can just use a constant.  Any constant value will do fine, since we're not really using it:  `1`, `0`, or even `"Bob"` will work.  Let's just use Scala's `None`, since it conveys nicely that we're not really using the key except to group *all* of the transactions together.

Add the code to transform our `DStream[Tx]` to `DStream[(None.type,Tx)]`:

``` scala
val allTxs = rawTxs.map { tx => (None, tx) }
```

Perfect.  We have a new `DStream` of `Tuple`s to work with.  Now, this means that our update function can be of type

``` scala
(Seq[Tx], Option[State]) ⇒ Option[State]
```

This actually makes perfect sense:  we're going to be given a bunch of `Tx`s and an `Option` containing the previous `State` (which will be `None` on the first call), then we use the given `Tx` objects to create a new `State` and return it.  With that, let's define our state update function now.

``` scala
val update = (txs: Seq[Tx], previous: Option[State]) =>
      Some(previous.getOrElse(State()) + (txs.size, txs.map(_.amount).sum))
```

You can see that

* we're given some `Tx`s and an `Option` of our previous `State`,
* we make sure we have a `State` value with `getOrElse`, then
* use our handy-dandy `+` method to return a new `State` that combines the previous `State` with our new count, `txs.size`, and total, `txs.map(_.amount).sum)`.

Now that we have our `DStream` of `Tuple`s and our update function, let's use them to update our `State`:

``` scala
val states = allTxs.updateStateByKey(update)
```

Notice that we're using the plural form here (`states`).  That's because, in general, the keys may be different.  For our simple example, there will only ever be one element in the `RDD` of the `DStream`, which will be of type `State`.

#### Create function to identify suspicious activity

Ok, so far so good.  Now that we are maintaining `State` appropriately, we can move on to the next bite of meat in our application, detecting suspicious activity.

Somehow, we need to combine the state that we're maintaining with the incoming transactions and somehow use the state to filter which transactions are suspicious.  Just so you're not left on the edge of your seat, let me disappoint you now by telling you that we're going to do something pretty simple:  we'll see if any transactions are within a given tolerance of the running average of all transactions seen so far.  While that's our (boring) algorithm, we're going to have to first see how we can combine our state with incoming transactions using our average-comparing fraud detection function.

Not surprisingly, we need to use `states`, a `DStream`, along with the transaction stream.  We could use `allTxs`, but that uses `Tuple`s with a key of `None`, which is just a waste of time, considering that we still have our original stream, `rawTxs`, available.  The `DStream` method that we're interested in here is called `transformWith`.  It takes a different `DStream` and a function, and returns a new `DStream` that represents the result of transforming `this` stream (our `State`), and the other stream (the `Tx`s),  with the given function, fraud detection.

##### Define fraud detection function

With all this in mind, let's look at the signature of `transformWith` to see what our fraud detection signature is required to be:

``` scala
def transformWith[U, V](other: DStream[U], transformFunc: (RDD[T], RDD[U]) ⇒ RDD[V])(implicit arg0: ClassTag[U], arg1: ClassTag[V]): DStream[V]
```

Here,

* `U` represents the type of the "other" `DStream`,
* `T` is the type of "this" `DStream`, and
* `V` is the type returned by the function that combines `RDD`s from the two `DStream`s.

It follows, then, the type of our fraud detection algorithm is

``` scala
(RDD[State], RDD[Tx]) => RDD[V]
```

Let's write the function now:

``` scala
val suspiciousCheck = (s: RDD[State], t: RDD[Tx]) => {
  val states = s.collect
  val stats = if (states.length == 0) State() else states.head
  t.filter(x => abs(x.amount) > fraudFactor * abs(stats.mean))
    .map(x => (s"${(100 * abs(x.amount / stats.mean)).toInt}%", x))
}
```

Breaking it down, we see that we first crack open the `RDD[State]` to get the only one in it (or a default one).  Next, we filter any `Tx`s whose `amount`s exceed our `fraudFactor` times the running average, `stats.mean`.  Then, as a convenience, we map the suspicious `Tx`s so that we return a `Tuple` of the percentage the amount is out of whack & the `Tx` itself; this makes `V` in the function signature above be `(String,Tx)`.  Now that we've defined our pseudoawesome fraud detection function, let's put it to work on our stream.

##### Combine accumulated state with streaming data

Our last step is to put it all together via `transformWith`.  Here's the code:

``` scala
val suspiciousTxs = states
  .map(_._2) // throw away the unused key & get just the State value
  .transformWith(rawTxs, suspiciousCheck)

states.map(_._2.toString).print
suspiciousTxs.foreachRDD(rdd => rdd.foreach(x => println(x.toString)))
```

Why the `map(_._2)`?  Well, remember that `states` is of type `DStream[(None, State)]`, and we're not using the key `None` except to group all of the `Tx`s together.  So, we transform the `DStream[(None, State)]` into just `DStream[State]`, then combine it with the transaction stream via `transformWith` using our fraud detection function.

The `print` & `println` calls on the last two lines just make it so the `DStream`s print themselves out on each batch interval so we can see what's going on.  In a real system, we'd probably write that data to a file in HDFS or ship it off to some other program or database for further investigation.

#### Let 'er rip!

Ok.  Our program is written and ready to go.  Now, we need to build & run it.

##### Build the program

In order to submit programs to Spark, we need to do more than just build a jar containing our application code.  We actually need to build an assembly jar that contains our programs and all of its transitive dependencies, except for the Spark libraries.

> Note:  For Spark dependencies, use the Maven & sbt scope value `provided`.  This lab doesn't because we're running locally.

Fortunately for you, there's good news on the horizon:  we've already taken care of the plumbing required to build the application assembly jar!

Pop over to your terminal now, and ensure that your current working directory is the root of this lab.  Then, issue the command

```
sbt package
```

If all goes well, you should end up with an application assembly jar in `target/scala-2.11` (depending on your Scala version) with the form `<sbt-project-name>_<scala-version>-<project-version>.jar`.  It should be the only jar in the build directory, so it really doesn't matter.  Your console output should look something like this:

```
[info] Loading project definition from /.../lab-01-suspicious-purchase-amounts/project
[info] Set current project to lab-01-suspicious-purchase-amounts (in build file:/.../lab-01-suspicious-purchase-amounts/)
[info] Updating {file:/.../lab-01-suspicious-purchase-amounts/}root...
[info] Resolving jline#jline;2.12.1 ...
[info] Done updating.
[info] Compiling 2 Scala sources to /.../lab-01-suspicious-purchase-amounts/target/scala-2.11/classes...
[warn] there was one deprecation warning; re-run with -deprecation for details
[warn] one warning found
[info] Packaging /.../lab-01-suspicious-purchase-amounts/target/scala-2.11/lab-01-suspicious-purchase-amounts_2.11-0.1-SNAPSHOT.jar ...
[info] Done packaging.
[success] Total time: 14 s, completed Jan 6, 2016 5:53:20 PM
```

If all doesn't go well, you've got a compilation or packaging error somewhere.  Time to dig in and fix it.  Hint:  read the compiler output!  :)

> Note:  Make sure that after each edit you make, you save your work and reissue the `sbt package` command.  Otherwise, you're just going to be spinning your wheels but going nowhere!

##### Submit the program to Spark

Once you've successfully built the project, it's time to run it!  Before we do, though, it's not as simple as `java -jar *.jar`:  Spark requires you to "submit" your application to it, so that it can be deployed into the cluster.  To do this, assuming your Spark installation is found at `$SPARK_HOME`, issue the command

```
$SPARK_HOME/bin/spark-submit --class com.example.transactions.SuspiciousPurchaseAmounts \
  --master 'local[*]' target/scala-2.11/*.jar
```

After some churning and logging output, you should see something like the following:

```

-------------------------------------------
Time: 1452125382000 ms
-------------------------------------------
State(16,-10083.447920107632)

(1586%,Tx(2016-01-01,59db9348-45ad-45b1-a499-4a2173213931,-10000.0))
-------------------------------------------
Time: 1452125383000 ms
-------------------------------------------
State(16,-10083.447920107632)

-------------------------------------------
Time: 1452125384000 ms
-------------------------------------------
State(16,-10083.447920107632)

```

If there's too much logging output, try redirecting `stderr` to `/dev/null` with this variant on the above command:

```
$SPARK_HOME/bin/spark-submit --class com.example.transactions.SuspiciousPurchaseAmounts \
  --master 'local[*]' target/scala-2.11/*.jar 2>/dev/null
```

Be aware, though, that if you do this and there's an error, you won't see it!

Well, you can see that there was a suspicious transaction for $10,000 that was 1586% of the running average, well outside of our default tolerance of 133%.  Now, the Loss Prevention department can track down that criminal!

#### Switch over to network streaming

Ok, now it's time for us to stop pumping fake data and start getting some real data in there.  For this next step, let's say that whatever process is recording transaction data is sending it to our Spark Streaming program as text in comma-separated value format over a network socket to port 9999 by default, one record per line.  Fortunately, to do this, there's only a couple of changes that we need to make.

##### Replace `queueStream` as data source

Replace the call to `queueStream` with the following:

``` scala
val rawTxs = ctx.socketTextStream("localhost", 9999)
  .map(_.split(",")) // split CSV text
  .map(x => Tx(date = x(0), desc = x(1), amount = x(2).toDouble)) // map to Tx object
  .filter( _.amount < 0) // only look for debits
```

As you can see, we're telling Spark Streaming to read from port 9999 on localhost and convert the incoming data into `Tx` objects and only look for debits (which are negative in our example).

##### Replace `TxPump` and lifecycle methods

Next, we're going to remove our artificial `TxPump` — comment or delete the call to `TxPump(ctx, queue, 3)` and `ctx.stop()`, then add the following call:

``` scala
ctx.awaitTermination()
```

This call basically tells Spark to keep running until it's told to terminate (which we'll do via `Ctrl-c` later on).

##### Build

Just as before, build your program with `sbt package` and ensure that there are no errors.  If there are, remember to listen to the compiler (that is, read what the compiler's error output is).

##### Simulate transaction source

Now, we said that in our new version of the program, we'd be receiving transaction data on port 9999.  There's a convenient utility in most Unix-like systems called `nc` ("netcat") that will take data from `stdin` and pipe it to a designated socket.

> Note:  Windows systems should have a similar command called `ncat`.

For our application, this will be `localhost` on port `9999`, of course.

Open a new terminal and, if you're on Linux, issue the command

```
nc localhost 9999
```

If you're on Mac, issue the command

```
nc -lk 9999
```

Check your documentation if you're on Windows (I'll avoid the obligatory snide "Windoze" or other remark here).

If there's an error, diagnose & correct it.  Otherwise, you should see the cursor on the next line, waiting for input on `stdin`.  Leave that process be for the moment; we're going to return to our program now and come back to it when we're ready to stream data.

##### Submit your program

Just as before, submit your program to Spark with the following command:

```
$SPARK_HOME/bin/spark-submit --class com.example.transactions.SuspiciousPurchaseAmounts \
  --master 'local[*]' target/scala-2.11/*.jar
```

> Remember:  You can suppress info & error logging by appending `2>/dev/null` to the command.

Once you've succesfully submitted your program to Spark and it's running we're ready to start streaming some data.

##### Stream transaction data to your application

We've included some real, albeit sanitized, transaction data for you to pipe to your Spark Streaming program.  In any text editor, open file `tx.csv` in the `lesson-5xx-resources` directory of this course.  Next, copy the first five lines of it, making sure that you get *entire lines, including the trailing newline*.

> Remember, newline is our record delimiter.

Now that we have these lines in our clipboard, paste them into your netcat terminal.  This will cause netcat to pipe the given text to the given port (9999).  Then, our application should receive and process it.

If all goes well, you should see output in your program's terminal similar to the following:

```

-------------------------------------------
Time: 1452128238000 ms
-------------------------------------------

-------------------------------------------
Time: 1452128239000 ms
-------------------------------------------
State(count = 5, mean = -40.733999999999995, sum = -203.67)

(162%,Tx(2015-06-16,POS Withdrawal - 75901 CORNER STORE 13        DRIPPING SPRITXUS - Card Ending In 7090,-66.21))
-------------------------------------------
Time: 1452128240000 ms
-------------------------------------------
State(count = 5, mean = -40.733999999999995, sum = -203.67)

-------------------------------------------
Time: 1452128241000 ms
-------------------------------------------
State(count = 5, mean = -40.733999999999995, sum = -203.67)

```

As you can see, we identified a potentially fraudulent transaction of $66.21 because it was 162% of our running average, which is $40.73 based on 5 transactions!  Let's have some fun now and really slam this baby to see what it can do.

Take a huge chunk of lines from the `tx.csv` file, starting at line 6, and copy & paste it into your netcat terminal.  Heck, take the whole rest of the file.  You should see a grip of activity that looks something like this:

```

-------------------------------------------
Time: 1452128363000 ms
-------------------------------------------
State(count = 5, mean = -40.733999999999995, sum = -203.67)

-------------------------------------------
Time: 1452128364000 ms
-------------------------------------------
State(count = 454, mean = -42.235660792951556, sum = -19174.990000000005)

(563%,Tx(2015-06-26,POS Withdrawal - WINN-DIXIE GROCERY #14 401 N CARROLLTON AVE   NEW ORLEANS  L - Card Ending In 7090,-238.05))
(246%,Tx(2015-06-26,POS Withdrawal - HAUNTEDHIST 7700 EASTPORT PARKWAY  5048612727   LAUS - Card Ending In 2000,-104.0))
(828%,Tx(2015-07-06,Withdrawal - Shared Branch 5029 KYLE CENTER DR     KYLE          TX,-350.0))
(287%,Tx(2015-07-22,POS Withdrawal - WM SUPERCENTER # Wal-Mart Super Center  BUDA         TXUS - Card Ending In 7090,-121.35))
...
(289%,Tx(2015-07-20,External Withdrawal - PAYPAL INSTANT TRANSFER - INST XFER,-122.29))
(144%,Tx(2015-07-20,POS Withdrawal - THE VENUE 3991 E Highway 290     DRIPPING SPRITXUS - Card Ending In 7090,-60.91))
-------------------------------------------
Time: 1452128365000 ms
-------------------------------------------
State(count = 1042, mean = -37.67829174664106, sum = -39260.779999999984)

(796%,Tx(2015-11-06,POS Withdrawal - 87607 HEB #014               KYLE         TXUS - Card Ending In 7090,-300.07))
(197%,Tx(2015-11-06,POS Withdrawal - LANDRYS SA DOWNTOWN 517 N PRESA            SAN ANTONIO  TXUS - Card Ending In 2000,-74.43))
(272%,Tx(2015-09-25,ATM Withdrawal - Bancard Systems 916 S 3RD ST.          RENTON       WAUS - Card Ending In 7090,-102.5))
(140%,Tx(2015-11-07,POS Withdrawal - 75901 CORNER STORE 13        DRIPPING SPRITXUS - Card Ending In 7090,-52.82))
...
(323%,Tx(2015-11-02,POS Withdrawal - IPIC AUSTIN F&B 3225 AMY DONOVAN PLZ   AUSTIN       TXUS - Card Ending In 2000,-121.8))
(146%,Tx(2015-11-02,POS Withdrawal - PAPPADEAUX SEAFOOD KIT 1304 COPELAND ROAD     ARLINGTON    T - Card Ending In 7090,-55.17))
(525%,Tx(2015-11-03,Withdrawal - Online Banking Transfer To XXXXXXXXXX CK,-198.0))
(265%,Tx(2015-11-03,Withdrawal - Online Banking Transfer To XXXXXXXXXX CK,-100.0))
-------------------------------------------
Time: 1452128366000 ms
-------------------------------------------
State(count = 1167, mean = -39.402150814053115, sum = -45982.30999999998)

(293%,Tx(2015-11-28,POS Withdrawal - 11871517 BIG LOTS  AUSTINTX    AUSTIN       TXUS - Card Ending In 2000,-115.59))
(186%,Tx(2015-11-30,POS Withdrawal - BJS RESTAURANTS 471 5207 BRODIE LANE #300  SUNSET VALLEYTXUS - Card Ending In 2000,-73.3))
(138%,Tx(2015-11-30,POS Withdrawal - THE HOME DEPOT 4703 1715 SOUTH 352ND ST    FEDERAL WAY  WAUS - Card Ending In 7090,-54.75))
...
(253%,Tx(2015-12-14,External Withdrawal - PAYPAL INSTANT TRANSFER - INST XFER,-100.0))
(425%,Tx(2015-12-15,POS Withdrawal - COSTCO WHSE #0641 4301 W WILLIAM CANNON DAUSTIN       TXUS - Card Ending In 2000,-167.8))
-------------------------------------------
Time: 1452128367000 ms
-------------------------------------------
State(count = 1167, mean = -39.402150814053115, sum = -45982.30999999998)

-------------------------------------------
Time: 1452128368000 ms
-------------------------------------------
State(count = 1167, mean = -39.402150814053115, sum = -45982.30999999998)

```

You can see that on the machine under load (an 8-core MacBook Pro with 16 Gb RAM), the first batch interval of 1 second processed 454 - 5 = 439 new transactions and identified many transactions that were greater than 133% of the new running average of around $42.23, then, in the next 1-second interval, calculated a new running average of around $37.68 from 1042 - 454 = 588 new transactions which produced a bunch more suspicious charges, then the next (and last) interval calculated a new average of around $39.40 on 1167 - 1042 = 125 new transactions, with more suspicious charges.  Over time, we'd expect the average to settle down as it did here, and we could tweak the fraud factor up as we learned more about how highly correlated our fraud factor is with actual fraud.

Your machine's numbers might show different batch interval amounts and running averages, but you should end up with the same final transaction count of 1167, mean of around $39.40, and total dollars processed of about $45,982.31.

## Conclusion

In this lab, you saw how Spark Streaming can be used to process continuously streamed data and handle it not only with ease, but also with nearly the same API and programming concepts as batch data!
