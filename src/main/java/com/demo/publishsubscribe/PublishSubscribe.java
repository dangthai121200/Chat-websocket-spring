package com.demo.publishsubscribe;

import java.util.Scanner;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class PublishSubscribe {

	private static final String EXCHANGE_NAME = "logs";

	public static class EmitLog {

		public static void main(String[] argv) throws Exception {
			
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost("localhost");
			Connection connection = factory.newConnection();
			
			Channel channel = connection.createChannel();
			channel.exchangeDeclare(EXCHANGE_NAME, "fanout");

			while (true) {
				
				Scanner scanner = new Scanner(System.in);
				System.out.println("Nhập message: ");
				String message = scanner.nextLine();
				
				channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes("UTF-8"));
				System.out.println(" [x] Sent '" + message + "'");
			}

		}
	}

	public static class ReceiveLogs {
		private static final String EXCHANGE_NAME = "logs";

		public static void main(String[] argv) throws Exception {
			
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost("localhost");
			Connection connection = factory.newConnection();
			Channel channel = connection.createChannel();

			channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
			String queueName = channel.queueDeclare().getQueue();
			channel.queueBind(queueName, EXCHANGE_NAME, "");

			System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

			DeliverCallback deliverCallback = (consumerTag, delivery) -> {
				String message = new String(delivery.getBody(), "UTF-8");
				System.out.println(" [x] Received '" + message + "'");
			};
			
			channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
			});
			
		}
	}
}
