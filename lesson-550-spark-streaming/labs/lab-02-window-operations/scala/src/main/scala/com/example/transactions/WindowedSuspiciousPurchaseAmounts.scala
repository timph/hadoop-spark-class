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

object WindowedSuspiciousPurchaseAmounts {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SuspiciousPurchaseAmounts")
    val batch = Milliseconds(Try(args(0).toLong).getOrElse(1000L))
    val window = Milliseconds(Try(args(1).toLong).getOrElse(3000L))
    val slide = Try(Milliseconds(args(2).toLong)).getOrElse(batch)
    val ctx = new StreamingContext(conf, batch)
    val fraudFactor = Try(args(3).toDouble).getOrElse(1.33)
    val windowFactor = Try(args(4).toDouble).getOrElse(2.0)

    val queue = new scala.collection.mutable.SynchronizedQueue[RDD[Tx]]()
    val rawTxs = ctx.queueStream(queue)
    
    // TODO

    ctx.start()
    TxPump(ctx, queue, 3)
    ctx.stop()
  }
}
