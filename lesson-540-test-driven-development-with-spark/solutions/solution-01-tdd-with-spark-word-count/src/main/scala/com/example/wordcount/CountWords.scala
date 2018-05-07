package com.example.wordcount

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.util.{Failure, Success, Try}

object CountWords {
  val delimiters = Array(' ','.',',',';',':','-','?','!','@','#','$','%','&','(',')','\"','/','\\')

  def wordsFrom(lines: RDD[String]) = lines.flatMap(_.split(delimiters)).filter(_.trim.length > 0)

  def wordCountsFrom(words: RDD[String]) = words.map(w => (w.toLowerCase, 1)).reduceByKey(_ + _).sortBy(_._2, false)

  def wordCounts = wordsFrom _ andThen wordCountsFrom _

  // default method with required parameters
  def apply(path:String)(implicit sc:SparkContext) = wordCounts(sc.textFile(path))

  // main method
  def main(args: Array[String]) {
    implicit val sc = new SparkContext(new SparkConf().setAppName("CountWords").setMaster("local[*]"))
    val counts = Try(apply(args(0)).collect)
    sc.stop
    counts match {
      case Success(wc) => wc.foreach(println(_))
      case Failure(e) => println("error: " + e.getMessage)
    }
  }
}
