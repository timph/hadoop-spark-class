package com.scispike.spark;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class PairRdd {

	  private static final Pattern separator = Pattern.compile("[ \\],.:;?!\\-@#\\(\\)\\\\\\*\\\"_]+");	  
	  
	  public static void main(String[] args) throws Exception {

		String srcDir = "/Users/kmusial/hadoopupdate/data";
		String file = srcDir + "/sherlock-holmes.txt";

	    SparkSession spark = SparkSession.builder().appName("PairRdd").getOrCreate();
	    spark.sparkContext().setLogLevel("ERROR");
	    
	    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
	    
	    JavaRDD<String> lines = spark.read().textFile(file).javaRDD();

	    JavaRDD<String> words = lines.map(s -> s.toLowerCase()).flatMap(s -> Arrays.asList(separator.split(s)).iterator());

	    JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<String, Integer>(s, 1));

	    JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

	    List<Tuple2<String, Integer>> output = counts.collect();

		// print the distinct word count
	    System.out.println("distinct word count " + output.size());
	    
	    // converting list into map for random access
	    Map<String, Integer> map = output.stream().collect(Collectors.toMap(Tuple2::_1, Tuple2::_2));

	    // print the counts for given words
	    System.out.println("if  count " + map.get("if"));
	    	System.out.println("and count " + map.get("and"));
	    	System.out.println("but count " + map.get("but"));

	    	// get the word with the highest count
	    	Tuple2<String, Integer> maxWord = counts.reduce((i1, i2) -> i1._2() > i2._2() ? i1 : i2);
	    	System.out.println("word with max count " + maxWord._1() + ": " + maxWord._2());
	    
	    spark.stop();
	    jsc.close();
	  }
}
