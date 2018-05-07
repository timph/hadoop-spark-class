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
    val interval = Milliseconds(Try(args(0).toLong).getOrElse(1000L))
    val ctx = new StreamingContext(conf, interval)
    ctx.sparkContext.setLogLevel("ERROR")
    val fraudFactor = Try(args(1).toDouble).getOrElse(1.33)
    val checkpointDir = "./anomolies/" + System.currentTimeMillis

    ctx.checkpoint(checkpointDir)

//    val queue = new scala.collection.mutable.SynchronizedQueue[RDD[Tx]]()
//    val rawTxs = ctx.queueStream(queue)
    
    val rawTxs = ctx.socketTextStream("localhost", 9999)
      .map(_.split(","))
      .map(x => Tx(date = x(0), desc = x(1), amount = x(2).toDouble))
      .filter( _.amount < 0)

    val allTxs = rawTxs.map { tx => (None, tx) } // forces all txs to have same key

    val update = (txs: Seq[Tx], previous: Option[State]) =>
      Some(previous.getOrElse(State()) + (txs.size, txs.map(_.amount).sum))

    val states = allTxs.updateStateByKey(update)

    val suspiciousCheck = (s: RDD[State], t: RDD[Tx]) => {
      val states = s.collect
      val stats = if (states.length == 0) State() else states.head // there's only one
      t.filter(x => abs(x.amount) > fraudFactor * abs(stats.mean))
        .map(x => (s"${(100 * abs(x.amount / stats.mean)).toInt}%", x))
    }

    val suspiciousTxs = states
      .map(_._2) // throw away the unused key & get just the State value
      .transformWith(rawTxs, suspiciousCheck)

    states.map(_._2.toString).print
    suspiciousTxs.foreachRDD(rdd => rdd.foreach(x => println(x.toString)))

    ctx.start()
//    TxPump(ctx, queue, 3)
//    ctx.stop()
    ctx.awaitTermination()
  }
}
