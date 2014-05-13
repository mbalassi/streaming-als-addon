package hu.sztaki.streaming.example.cellinfo.rabbitmq;

import java.io.IOException;
import java.util.Random;

public class DataStreamGenerator {
	private int freq_;
	private Random r_;
	private RabbitMQ rmq_;
	private String queueName_;

	public DataStreamGenerator(int freq, String host, String queueName) {
		r_ = new Random();
		rmq_ = new RabbitMQ(host);
		freq_ = freq;
		queueName_ = queueName;
	}

	public void generate() {
		for (;;) {
			try {
				Thread.sleep(r_.nextInt(freq_));
				int phone = 1000000 + r_.nextInt(8999999);
				int cell = r_.nextInt(10000);
				long time = System.currentTimeMillis();
				String message = Long.toString(time) + '\t' + Integer.toString(cell) + '\t'
						+ Integer.toString(phone);
				try {
					rmq_.sendMessage(queueName_, message);
				} catch (IOException io) {
					io.printStackTrace();
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) {
		DataStreamGenerator gen = new DataStreamGenerator(Integer.valueOf(args[0]), args[1],
				args[2]);
		gen.generate();
	}
}
