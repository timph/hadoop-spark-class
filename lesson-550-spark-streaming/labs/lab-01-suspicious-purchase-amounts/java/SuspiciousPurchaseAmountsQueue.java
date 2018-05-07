package com.scispike.spark;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class SuspiciousPurchaseAmountsQueue {
	
	  private static final Integer DURATION = 1;
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

		    // setting the checkpoint to store the state
		    ssc.checkpoint(".");

		    Queue<JavaRDD<Transaction>> rddQueue = new LinkedList<>();
		    fillQueue(ssc, rddQueue);
		    		    
		    JavaDStream<Transaction> debTxns = ssc.queueStream(rddQueue, true);
		    
		    // all transactions are given the same key as all of them should contribute to the amount's mean
		    JavaPairDStream<Integer, Transaction> keyTxns = debTxns.mapToPair(s -> new Tuple2<>(1, s));
		    
		    // defining mapping function to evaluate the transaction using the current state and to update the state
		    Function3<Integer, Optional<Transaction>, State<TransactionState>, Tuple2<Integer, Transaction>> mappingFunc = (key, txn, state) -> {

		    		TransactionState curState = state.exists() ? state.get() : new TransactionState();
		    		Transaction curTxn = txn.orElse(new Transaction());
		        	
		        	if (curTxn.getAmount() != null) {

		        		Boolean fraudFlag = Math.abs(curTxn.getAmount()) > Math.abs(curState.getMean() * FRAUD_FACTOR);
		        		curTxn.setFraudFlag(fraudFlag);
		        		curState.add(curTxn);
		        	}
		        	
		        Tuple2<Integer, Transaction> output = new Tuple2<>(1, curTxn);
		        state.update(curState);
		        return output;
		    };

		    
		    JavaMapWithStateDStream<Integer, Transaction, TransactionState, Tuple2<Integer, Transaction>> mapWithState =
		    		keyTxns.mapWithState(StateSpec.function(mappingFunc));
		    
		    // dropping the key and filtering fraudulent transactions 
		    JavaDStream<Transaction> suspiciousTxns = mapWithState.map(s -> s._2).filter(s -> s.getFraudFlag());
		    
		    suspiciousTxns.print();
		    
		    ssc.start();
		    
		    ssc.awaitTermination();
		  }
}
