package com.scispike.spark;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class SuspiciousPurchaseAmountsWindowQueue {

	  private static final Integer DURATION = 2;
	  private static final Integer WINDOW_LENGTH = 5*DURATION;
	  private static final Integer SLIDE_INTERVAL = DURATION;
	  private static final Double  FRAUD_FACTOR = 1.33;

	  private static final Double  MIN_AMOUNT = 0.01;
	  private static final Double  MAX_AMOUNT = 10.0;
	  private static final Double  SUSPICIOUS_AMOUNT = 10000.0;
	  
	  private static void fillQueue(JavaStreamingContext ssc, Queue<JavaRDD<Transaction>> rddQueue) {
		  
		    // Create and push some RDDs into the queue
		  
		    Random rnd = new Random();
		    List<Transaction> list = new ArrayList<Transaction>();
		    for (int j = 1; j < 5; j++) {
			    for (int i = 0; i < 15; i++) {
			    	
			    	  Double amount = rnd.nextDouble()*(MAX_AMOUNT-MIN_AMOUNT)+MIN_AMOUNT;
			    	  Transaction txn = new Transaction("2018-01-01", "description", -amount);
			      list.add(txn);
			    }
		    	    Transaction txn = new Transaction("2018-01-01", "description", -SUSPICIOUS_AMOUNT*j);
		        list.add(txn);
			    rddQueue.add(ssc.sparkContext().parallelize(list));
		    }
	  }
	  
	  public static void main(String[] args) throws Exception {

		    SparkConf sparkConf = new SparkConf().setAppName("SuspiciousPurchaseAmounts");
		    JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(DURATION));
		    ssc.sparkContext().setLogLevel("ERROR");


		    Queue<JavaRDD<Transaction>> rddQueue = new LinkedList<>();
		    fillQueue(ssc, rddQueue);
		    		    
		    JavaDStream<Transaction> debitTxns = ssc.queueStream(rddQueue, true);

		    // we need to add the key to transactions to be able to compare with the amount mean
		    // in the real application it would be more natural to use the account id as the key
		    JavaPairDStream<Integer, Transaction> keyedTxns = debitTxns.mapToPair(s -> new Tuple2<Integer, Transaction>(1, s));
		    
		    // creating a state from a transaction, state representing a set of transactions with the amount mean
		    JavaDStream<TransactionState> amounts = debitTxns.map(s -> new TransactionState(1, s.getAmount()));
		    
		    // adding the key to the amounts to join with transactions
		    JavaPairDStream<Integer, TransactionState> keyedAmounts = amounts.mapToPair(s -> new Tuple2<Integer, TransactionState>(1, s));
		    
		    // reducing function to get the state with the correct amount mean for a window of streamed transactions
		    Function2<TransactionState, TransactionState, TransactionState> reduceFunc = (s1, s2) -> {
		       return TransactionState.add(s1, s2);
		    };
		    
		    // reducing amounts over the window to get the amount mean
		    JavaPairDStream<Integer, TransactionState> meanAmounts = keyedAmounts.reduceByKeyAndWindow(reduceFunc, Durations.seconds(WINDOW_LENGTH), Durations.seconds(SLIDE_INTERVAL));
		    
		    // joining transactions with the states
		    JavaPairDStream<Integer, Tuple2<Transaction, TransactionState>> joinedTxnsStates = keyedTxns.join(meanAmounts);
		    
		    // identifying suspicious transactions and stripping extra data from the join
		    JavaDStream<Transaction> suspiciousTxns = joinedTxnsStates.map(s -> s._2).filter(s -> s._1.getAmount() < s._2.getMean()*FRAUD_FACTOR).map(s -> s._1);

		    suspiciousTxns.print();

		    ssc.start();
		    ssc.awaitTermination();
		  }
}
