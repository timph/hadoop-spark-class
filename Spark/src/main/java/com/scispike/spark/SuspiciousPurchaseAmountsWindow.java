package com.scispike.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;


public class SuspiciousPurchaseAmountsWindow {

	  private static final String  HOST = "localhost";
	  private static final Integer PORT = 9999;
	  private static final Integer DURATION = 2;
	  private static final Integer WINDOW_LENGTH = 5*DURATION;
	  private static final Integer SLIDE_INTERVAL = DURATION;
	  private static final Double  FRAUD_FACTOR = 1.33;
	  
	  public static void main(String[] args) throws Exception {

		    SparkConf sparkConf = new SparkConf().setAppName("SuspiciousPurchaseAmounts");
		    JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(DURATION));
		    ssc.sparkContext().setLogLevel("ERROR");

		    JavaReceiverInputDStream<String> lines = ssc.socketTextStream(HOST, PORT, StorageLevels.MEMORY_AND_DISK_SER);
		    
		    JavaDStream<Transaction> rawTxns = lines.map(s -> s.split(",")).filter(s -> s.length == 3).map(s -> new Transaction(s[0], s[1], Double.parseDouble(s[2])));
		    JavaDStream<Transaction> debitTxns = rawTxns.filter(s -> s.getAmount() < 0);

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
