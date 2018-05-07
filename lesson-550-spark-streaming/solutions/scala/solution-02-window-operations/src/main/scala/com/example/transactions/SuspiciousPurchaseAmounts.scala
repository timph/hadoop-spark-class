package com.example.transactions

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import scala.math._
import scala.util.Try

case class Tx(date: String, desc: String, amount: Double)

case class State(count: Long = 0, sum: Double = 0) {
  val mean = if (count == 0) 0.0 else sum / count

  def +(that: State) = State(this.count + that.count, this.sum + that.sum)

  def +(count: Long, sum: Double) = State(this.count + count, this.sum + sum)

  override def toString = s"State(count = $count, mean = $mean, sum = $sum)"
}

object SuspiciousPurchaseAmounts {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SuspiciousPurchaseAmounts")
    val batch = Milliseconds(Try(args(0).toLong).getOrElse(1000L))
    val window = Milliseconds(Try(args(1).toLong).getOrElse(3000L))
    val slide = Try(Milliseconds(args(2).toLong)).getOrElse(batch)
    val ctx = new StreamingContext(conf, batch)
    val fraudFactor = Try(args(3).toDouble).getOrElse(1.33)
    val windowFactor = Try(args(4).toDouble).getOrElse(2.0)
    ctx.checkpoint("./anomolies/" + System.currentTimeMillis)

//    val queue = new scala.collection.mutable.SynchronizedQueue[RDD[Tx]]()
//    val rawTxs = ctx.queueStream(queue)
    
    val rawTxs = ctx.socketTextStream("localhost", 9999)
      .map(_.split(","))
      .map(x => Tx(date = x(0), desc = x(1), amount = x(2).toDouble))
      .filter( _.amount < 0)

    val windowTxs = rawTxs.map(tx => (None, tx)).window(window, slide) // forces all txs to have same key

    val update = (txs: Seq[Tx], previous: Option[State]) =>
      Some(State(txs.size, txs.map(_.amount).sum))

    val states = windowTxs.updateStateByKey(update)

    val suspiciousAmountCheck = (s: RDD[State], t: RDD[Tx]) => {
      val states = s.collect
      val stats = if (states.length == 0) State() else states.head // there's only one
      t.filter(x => abs(x.amount) > fraudFactor * abs(stats.mean))
        .map(x => (s"${(100 * abs(x.amount / stats.mean)).toInt}%", x.date, x.amount, x.desc))
    }
    val suspiciousAmountTxs = states
      .map(_._2) // throw away the unused key & get just the State value
      .transformWith(windowTxs.map(_._2), // ditto
        suspiciousAmountCheck)

    states.map(_._2.toString).print
    suspiciousAmountTxs.foreachRDD(rdd => rdd.foreach(x => println(x.toString)))


    ctx.start()
//    TxPump(ctx, queue, 3)
//    ctx.stop()
    ctx.awaitTermination()
  }
}
