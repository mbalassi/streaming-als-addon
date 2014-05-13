package hu.sztaki.streaming.example.cellinfo.rabbitmq;

import java.io.IOException;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

public class RabbitMQ {
	private String host_;

	public RabbitMQ(String host) {
		this.host_ = host;
	}

	public void sendMessage(String queueName, String message) throws IOException {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(host_);
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();

		channel.queueDeclare(queueName, false, false, false, null);
		channel.basicPublish("", queueName, null, message.getBytes());

		channel.close();
		connection.close();
	}

	public String receive(String queueName) throws IOException, InterruptedException {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(host_);
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();

		channel.queueDeclare(queueName, false, false, false, null);

		QueueingConsumer consumer = new QueueingConsumer(channel);
		channel.basicConsume(queueName, true, consumer);

		try {
			QueueingConsumer.Delivery delivery = consumer.nextDelivery();
			return new String(delivery.getBody());
		} catch (ShutdownSignalException | ConsumerCancelledException | InterruptedException e) {
			throw new InterruptedException();
		} finally {
			channel.close();
			connection.close();
		}
	}
}
