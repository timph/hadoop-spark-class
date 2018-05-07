package com.scispike.spark;

import java.io.Serializable;

public class TransactionState implements Serializable {
	
    private static final long serialVersionUID = 1l;
    
	Integer count = 0;
	Double amount = 0.0;
	Double mean = 0.0;
	
	public TransactionState(Integer count, Double amount) {
		this.count = count;
		this.amount = amount;
		this.mean = amount/count;
	}

	public TransactionState() {
		this.count = 0;
		this.amount = 0.0;
		this.mean = 0.0;
	}
	
	public void add(Transaction txn) {
		
		if (txn.getAmount() != null) {
			this.count++;
			this.amount += txn.getAmount();
			this.mean = this.amount/this.count;
			
//			System.out.println("Transaction:\t" + txn.toString() + "\tTransaction State:\t" + this.toString());
		}
	}
	
	public static TransactionState add(TransactionState s1, TransactionState s2) {
		return new TransactionState(s1.getCount()+s2.getCount(), s1.getAmount()+s2.getAmount());
	}
	
	public void update(TransactionState s) {
		this.count += s.getCount();
		this.amount += s.getAmount();
		this.mean = this.amount/this.count;
	}
	
	public Integer getCount() {
		return count;
	}
	
	public void setCount(Integer count) {
		this.count = count;
	}
	
	public Double getAmount() {
		return amount;
	}
	
	public void setAmount(Double amount) {
		this.amount = amount;
	}
	
	public Double getMean() {
		return mean;
	}
	
	public String toString() {
		return this.count + "\t" + this.amount + "\t" + this.mean;
	}
}
