package com.demo.routing;

import java.util.Scanner;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class Routing {
	static final String EXCHANGE_NAME = "direct_logs";

	public static class EmitLogDirect {

		public static void main(String[] argv) throws Exception {
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost("localhost");
			try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
				channel.exchangeDeclare(EXCHANGE_NAME, "direct");
				while (true) {
					String severity = getSeverity();
					String message = getMessage();
					channel.basicPublish(EXCHANGE_NAME, severity, null, message.getBytes("UTF-8"));
					System.out.println(" [x] Sent '" + severity + "':'" + message + "'");
				}

			}
		}

		private static String getMessage() {
			Scanner scanner = new Scanner(System.in);
			System.out.println("Nhập message: ");
			String message = scanner.nextLine();
			return message;
		}

		private static String getSeverity() {
			Scanner scanner = new Scanner(System.in);
			System.out.println("Nhập Severity: ");
			String message = scanner.nextLine();
			return message;
		}
	}

	public static class ReceiveLogsDirect {
		
		public static void main(String[] argv) throws Exception {
					
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost("localhost");
			Connection connection = factory.newConnection();
			Channel channel = connection.createChannel();

			channel.exchangeDeclare(EXCHANGE_NAME, "direct");
			String queueName = channel.queueDeclare().getQueue();

			if (argv.length < 1) {
				System.err.println("Usage: ReceiveLogsDirect [info] [warning] [error]");
				System.exit(1);
			}

			for (String severity : argv) {
				channel.queueBind(queueName, EXCHANGE_NAME, severity);
			}
			System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

			DeliverCallback deliverCallback = (consumerTag, delivery) -> {
				String message = new String(delivery.getBody(), "UTF-8");
				System.out.println(" [x] Received '" + delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
			};
			channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
			});
		}
	}
}
