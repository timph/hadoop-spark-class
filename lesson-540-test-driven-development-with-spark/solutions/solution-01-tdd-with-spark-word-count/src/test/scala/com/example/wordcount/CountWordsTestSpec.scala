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

  // write a test to convert lines to words
  "wordsFrom" should "convert lines into words" in {
    CountWords.wordsFrom(lines).count should be (6)
  }

  // write a test to count each word
  "wordCountsFrom" should "count each word" in {
    val counts = CountWords.wordCountsFrom(words).collect
    counts should contain ("word1", 2)
    counts should contain ("word2", 1)
    counts should contain ("word3", 1)
  }

  // write a test to count words in lines
  "fileWordCounts" should "count words in file" in {
    val results = CountWords.wordCounts(lines).collect
    results should contain ("line", 3)
    results should contain ("one", 1)
    results should contain ("two", 1)
    results should contain ("three", 1)
  }
}
