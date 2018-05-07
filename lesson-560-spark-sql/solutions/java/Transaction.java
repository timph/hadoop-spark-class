package com.scispike.spark;

import java.io.Serializable;

public class Transaction  implements Serializable {

    private static final long serialVersionUID = 1l;

	private String date;
	private String desc;
	private Double amount;

	public Transaction(String date, String desc, Double amount) {

		this.date=date;
		this.desc=desc;
		this.amount=amount;
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
}
