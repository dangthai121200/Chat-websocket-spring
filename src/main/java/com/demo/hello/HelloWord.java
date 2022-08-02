package com.demo.hello;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class HelloWord {

	public static class Send {
		private final static String QUEUE_NAME = "hello";

		public static void main(String[] args) throws IOException, TimeoutException {
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost("localhost");
			Connection connection = factory.newConnection();
			Channel channel = connection.createChannel();
			channel.queueDeclare(QUEUE_NAME, false, false, false, null);

			while(true) {
				
				Scanner scanner = new Scanner(System.in);
				System.out.println("Nhập message: ");
				String message = scanner.nextLine();
				channel.basicPublish("", QUEUE_NAME, null, message.getBytes());

				System.out.println(" [x] Sent '" + message + "'");
			}

		}
	}

	public static class Recv {

		private final static String QUEUE_NAME = "hello";

		public static void main(String[] argv) throws Exception {

			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost("localhost");
			Connection connection = factory.newConnection();
			Channel channel = connection.createChannel();

			channel.queueDeclare(QUEUE_NAME, false, false, false, null);

			System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

			DeliverCallback deliverCallback = (consumerTag, delivery) -> {
				String message = new String(delivery.getBody(), "UTF-8");
				System.out.println(" [x] Received '" + message + "'");
			};
			channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> {
			});
		}
	}

}
