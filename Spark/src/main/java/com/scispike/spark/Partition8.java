package com.scispike.spark;

import java.util.Arrays;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class Partition8 {

	  private static final Pattern separator = Pattern.compile("[ \\],.:;?!\\-@#\\(\\)\\\\\\*\\\"_]+");	  
	  
	  
	  private void repartition(JavaRDD<String> lines, int partitionCount) {
		  
  		JavaRDD<String> reprt = lines.repartition(partitionCount);
		
  		long ts = System.currentTimeMillis();
  		
  	    JavaRDD<String> words = reprt.map(s -> s.toLowerCase()).flatMap(s -> Arrays.asList(separator.split(s)).iterator());

  	    JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<String, Integer>(s, 1));

  	    JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

  	    	// get the word with the highest count
  	    	Tuple2<String, Integer> maxWord = counts.reduce((i1, i2) -> i1._2() > i2._2() ? i1 : i2);
  	    	
	    long te = System.currentTimeMillis();
  	    	System.out.println("word with max count " + maxWord._1() + ": " + maxWord._2() + " partition count " + partitionCount + " time " + (te-ts));
	  }
	  
	  
	  private void runJob() {
		  
		String srcDir = "/Users/kmusial/hadoopupdate/data";
		String file = srcDir + "/sherlock-holmes.txt";

	    SparkSession spark = SparkSession.builder().appName("Partition").getOrCreate();
	    spark.sparkContext().setLogLevel("ERROR");
	    
	    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
	    
	    JavaRDD<String> lines = spark.read().textFile(file).javaRDD();

	    int minCores = 2;
	    	int numCores = 8;

	    	IntStream.rangeClosed(minCores, numCores).filter(i -> i % 2 == 0).forEach(i -> repartition(lines, i));
	    
	    spark.stop();
	    jsc.close();
	  }
	  
	  
	  public static void main(String[] args) throws Exception {
		  new Partition8().runJob();
	  }
}
