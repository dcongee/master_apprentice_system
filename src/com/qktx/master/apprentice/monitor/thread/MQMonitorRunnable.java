package com.qktx.master.apprentice.monitor.thread;

import org.apache.log4j.Logger;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

public class MQMonitorRunnable implements Runnable {

	private static Logger logger = Logger.getLogger(MQMonitorRunnable.class);

	private Channel channel;

	public MQMonitorRunnable(Channel channel) {
		this.channel = channel;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub

		if (channel != null) {
			Connection connection = channel.getConnection();

			if (!connection.isOpen()) {
				logger.warn("connection is not open.try to reopen connection");
				logger.warn(connection.getCloseReason());
			}

			if (!channel.isOpen()) {
				logger.warn("channel is not open.try to reopen channel");
			}
		}

	}

}
