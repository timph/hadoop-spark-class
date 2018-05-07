package com.scispike.spark;

import java.io.Serializable;

public class Transaction implements Serializable {

    private static final long serialVersionUID = 1l;

	private String date;
	private String desc;
	private Double amount;
	private Boolean fraudFlag;

	public Transaction(String date, String desc, Double amount) {
		
		this(date, desc, amount, false);
	}

	public Transaction(String date, String desc, Double amount, Boolean fraudFlag) {
		
		this.date=date;
		this.desc=desc;
		this.amount=amount;
		this.fraudFlag=fraudFlag;
	}

	public Transaction() {
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getDesc() {
		return desc;
	}

	public void setDesc(String desc) {
		this.desc = desc;
	}

	public Double getAmount() {
		return amount;
	}

	public void setAmount(Double amount) {
		this.amount = amount;
	}
	
	public Boolean getFraudFlag() {
		return fraudFlag;
	}

	public void setFraudFlag(Boolean fraudFlag) {
		this.fraudFlag = fraudFlag;
	}
	
	public String toString() {
		return this.date + "\t" + this.desc + "\t" + this.amount;
	}
}
