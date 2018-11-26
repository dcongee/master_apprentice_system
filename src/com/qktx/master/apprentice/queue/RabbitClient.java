package com.qktx.master.apprentice.queue;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class RabbitClient {
	private Logger logger = Logger.getLogger(RabbitClient.class);

	private Channel channel;
	private ConnectionFactory factory;

	public RabbitClient(ConnectionFactory factory) {
		this.factory = factory;
		initRabbitClient();
	}

	public void initRabbitClient() {
		logger.info("init rabbitMQ client.");

		Connection connection = null;
		try {
			connection = this.factory.newConnection();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TimeoutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			this.channel = connection.createChannel();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public Channel getChannel() {
		return channel;
	}

	public void setChannel(Channel channel) {
		this.channel = channel;
	}

}
