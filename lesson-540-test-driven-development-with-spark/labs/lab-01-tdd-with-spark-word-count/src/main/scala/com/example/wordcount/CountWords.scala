package com.example.wordcount

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.util.{Failure, Success, Try}

object CountWords {
  val delimiters = Array(' ','.',',',';',':','-','?','!','@','#','$','%','&','(',')','\"','/','\\')

  // TODO: define function to convert lines to words
  def wordsFrom(lines: RDD[String]) = ???

  // TODO: define function to count words & sort
  def wordCountsFrom(words: RDD[String]) = ???

  // TODO: combine functions words & wordCounts
  def wordCounts = ???
}
