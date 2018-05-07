package com.scispike.spark;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class Clustering implements Serializable {

    private static final long serialVersionUID = 1l;
	
    private Vector getVector(Double val) {
    	
  	  double[] vec = new double[1];
  	  vec[0] = val;
  	  
  	  return Vectors.dense(vec);
    }
    
    
	private void runJob() {

		String srcDir = "/Users/kmusial/hadoopupdate/data";
		String file = srcDir + "/tx.csv";

	    SparkSession spark = SparkSession.builder().appName("Clustering").getOrCreate();
	    spark.sparkContext().setLogLevel("ERROR");
	    
	    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
	    
	    JavaRDD<String> lines = spark.read().textFile(file).javaRDD();
	    
	    JavaRDD<Vector> amounts = lines.map(s -> s.split(",")).map(s -> Double.parseDouble(s[2])).map(s -> getVector(s));

	    amounts.cache();

	    int clusterCount = 10;
	    int iterationCount = 20;

	    KMeansModel clusters = KMeans.train(amounts.rdd(), clusterCount, iterationCount);
	
		System.out.println("Cluster centers:");
		for (Vector center: clusters.clusterCenters()) {
		   System.out.println(" " + center);
		}
	    
		List<Double> list = DoubleStream.iterate(1, i -> i*10).limit(4).boxed().collect(Collectors.toList()); 
		JavaRDD<Double> vals = jsc.parallelize(list).flatMap(s -> Arrays.asList(5*s, -5*s).iterator());

		JavaPairRDD<Double, Integer> clsd = vals.mapToPair(s -> new Tuple2<Double, Integer>(s, clusters.predict(getVector(s))));
	    
	    System.out.println("Predictions");
	    for (Tuple2<Double, Integer> elem : clsd.collect()) {
	    		System.out.println("Amount: " + elem._1 + "\tCluster: " + elem._2);
	    }

		spark.stop();
	    jsc.close();
	}
	  
	  
	public static void main(String[] args) throws Exception {
		new Clustering().runJob();
	}
}
