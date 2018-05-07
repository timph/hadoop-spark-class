package com.scispike.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class SuspiciousPurchaseAmounts {
	
	  private static final String  HOST = "localhost";
	  private static final Integer PORT = 9999;
	  private static final Integer DURATION = 1;
	  private static final Double  FRAUD_FACTOR = 1.33;
	  
	  public static void main(String[] args) throws Exception {

		    SparkConf sparkConf = new SparkConf().setAppName("SuspiciousPurchaseAmounts");
		    JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(DURATION));
		    ssc.sparkContext().setLogLevel("ERROR");

		    // setting the checkpoint to store the state
		    ssc.checkpoint(".");

		    JavaReceiverInputDStream<String> lines = ssc.socketTextStream(HOST, PORT, StorageLevels.MEMORY_AND_DISK_SER);
		    
		    JavaDStream<Transaction> rawTxns = lines.map(s -> s.split(",")).map(s -> new Transaction(s[0], s[1], Double.parseDouble(s[2])));
		    JavaDStream<Transaction> debTxns = rawTxns.filter(s -> s.getAmount() < 0);
		    
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
