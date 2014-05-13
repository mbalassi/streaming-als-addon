package hu.sztaki.streaming.example.cellinfo.rabbitmq;

public class DataStreamReader {
	public static void main(String[] args) {
		RabbitMQ rmq = new RabbitMQ(args[0]);
		for (;;) {
			try {
				System.out.println(rmq.receive(args[1]));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
