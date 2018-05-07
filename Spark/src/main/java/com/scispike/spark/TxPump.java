package com.scispike.spark;

import java.util.Random;

public class TxPump {

	public static void main(String[] args) {

		Double min = 0.01;
		Double max = 10.0;
		Double suspect = 1000.0;
		Double chance = 0.1;
		
		Random rnd = new Random();
		while (true) {
			Double amount = rnd.nextDouble() < chance ? -rnd.nextDouble()*suspect+min : -rnd.nextDouble()*(max-min)+min;
			System.out.println("2016-01-01" + "," + "Description" + "," + amount);
			Integer sleep = rnd.nextInt(500);
			try {
			    Thread.sleep(Long.valueOf(sleep));
			} catch (Exception ex) {}
		}
	}
}
