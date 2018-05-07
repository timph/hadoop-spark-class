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
    val fraudFactor = Try(args(1).toDouble).getOrElse(1.33)
    val checkpointDir = "./anomolies/" + System.currentTimeMillis

    val ctx = new StreamingContext(conf, interval)

    val queue = new scala.collection.mutable.SynchronizedQueue[RDD[Tx]]()
    val rawTxs = ctx.queueStream(queue)

    // TODO

    ctx.start()
    TxPump(ctx, queue, 3)
    ctx.stop()
  }
}
