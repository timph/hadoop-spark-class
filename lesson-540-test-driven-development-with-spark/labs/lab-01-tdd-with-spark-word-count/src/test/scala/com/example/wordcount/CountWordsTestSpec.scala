package com.example.wordcount

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}

class CountWordsTestSpec extends FlatSpec with Matchers with BeforeAndAfterAll {

  val ctx = new SparkContext(new SparkConf().setAppName("CountWordsTestSpec").setMaster("local[*]"))
  val lines = ctx.parallelize(Array("Line,One", "Line:Two", "Line Three"))
  val words = ctx.parallelize(Array("Word1", "Word1", "Word2", "word3"))

  override def afterAll() {
    ctx.stop()
  }

  // TODO: write a test to convert lines to words
  "wordsFrom" should "convert lines into words" in ???

  // TODO: write a test to count each word
  "wordCountsFrom" should "count each word" in ???

  // TODO: write a test to count words in lines
  "fileWordCounts" should "count words in file" in ???
}
