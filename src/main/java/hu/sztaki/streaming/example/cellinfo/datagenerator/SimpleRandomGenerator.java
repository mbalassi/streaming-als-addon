package hu.sztaki.streaming.example.cellinfo.datagenerator;

import java.util.Random;

public class SimpleRandomGenerator implements DataGenerator {
	private Random r_ = new Random();

	@Override
	public Record get() {
		int phone = 1000000 + r_.nextInt(8999999);
		int cell = r_.nextInt(10000);
		long time = System.currentTimeMillis();
		return new Record(time, cell, phone);
	}

}
