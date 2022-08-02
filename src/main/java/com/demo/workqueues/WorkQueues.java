package com.demo.workqueues;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.MessageProperties;

public class WorkQueues {

	public static class NewTask {
		private static final String TASK_QUEUE_NAME = "task_queue";

		public static void main(String[] args) throws IOException, TimeoutException {
			ConnectionFactory connectionFactory = new ConnectionFactory();
			connectionFactory.setHost("localhost");
			Connection connection = connectionFactory.newConnection();
			Channel channel = connection.createChannel();
			boolean durable = true;
			channel.queueDeclare(TASK_QUEUE_NAME, durable, false, false, null);

			while (true) {
				Scanner scanner = new Scanner(System.in);
				System.out.println("Nhập message: ");
				String message = scanner.nextLine();			
				channel.basicPublish("", TASK_QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
				System.out.println(" [x] Sent '" + message + "'");
			}
		}
	}

	public static class Worker {
		private static final String TASK_QUEUE_NAME = "task_queue";

		public static void main(String[] args) throws IOException, TimeoutException {
			ConnectionFactory connectionFactory = new ConnectionFactory();
			connectionFactory.setHost("localhost");

			Connection connection = connectionFactory.newConnection();
			Channel channel = connection.createChannel();
			int prefetchCount = 1;
			channel.basicQos(prefetchCount);
			boolean durable = true;
			channel.queueDeclare(TASK_QUEUE_NAME, durable, false, false, null);
			channel.basicQos(1);
			System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

			DeliverCallback deliverCallback = (consumerTag, delivery) -> {
				String message = new String(delivery.getBody(), "UTF-8");

				System.out.println(" [x] Received '" + message + "'");
				try {
					doWork(message);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} finally {
					System.out.println(" [x] Done");
				}
			};
			boolean autoAck = true; // acknowledgment is covered below
			channel.basicConsume(TASK_QUEUE_NAME, autoAck, deliverCallback, consumerTag -> {
			});

		}

		private static void doWork(String task) throws InterruptedException {
			for (char ch : task.toCharArray()) {
				if (ch == '.')
					Thread.sleep(1000);
			}

		}
	}
}
