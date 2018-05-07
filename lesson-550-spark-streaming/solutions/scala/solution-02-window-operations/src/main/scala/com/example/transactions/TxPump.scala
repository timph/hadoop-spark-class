package com.example.transactions

import java.util.{Calendar, Date}

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext

import scala.collection.mutable
import scala.math._

object TxPump {
  def desc = java.util.UUID.randomUUID.toString
  def rndDbl(max:Double = 1, min:Double = 0) = ((max - min) * random) + min
  def rndInt(max:Int = 1, min:Int = 0) = ((max - min + 1) * random).toInt + min
  def dt = "2016-01-01"
  val maxNorm = 10.00
  val minNorm = .01
  val suspicious = 10000.00
  def apply(sc: StreamingContext, queue: mutable.SynchronizedQueue[RDD[Tx]], nPasses:Int = 1, sleep: Int = 10000) {
    for (i <- 1 to max(nPasses, 1)) {
      val txs = List(
        Tx(dt, desc, -rndDbl(maxNorm, minNorm)),
        Tx(dt, desc, -rndDbl(maxNorm, minNorm)),
        Tx(dt, desc, -rndDbl(maxNorm, minNorm)),
        Tx(dt, desc, -rndDbl(maxNorm, minNorm)),
        Tx(dt, desc, -rndDbl(maxNorm, minNorm)),
        Tx(dt, desc, -rndDbl(maxNorm, minNorm)),
        Tx(dt, desc, -rndDbl(maxNorm, minNorm)),
        Tx(dt, desc, -rndDbl(maxNorm, minNorm)),
        Tx(dt, desc, -rndDbl(maxNorm, minNorm)),
        Tx(dt, desc, -rndDbl(maxNorm, minNorm)),
        Tx(dt, desc, -rndDbl(maxNorm, minNorm)),
        Tx(dt, desc, -rndDbl(maxNorm, minNorm)),
        Tx(dt, desc, -rndDbl(maxNorm, minNorm)),
        Tx(dt, desc, -rndDbl(maxNorm, minNorm)),
        Tx(dt, desc, -rndDbl(maxNorm, minNorm)),
        Tx(dt, desc, -suspicious * i)
      )

      queue += sc.sparkContext.parallelize(txs)
      Thread.sleep(sleep)
    }
  }
}