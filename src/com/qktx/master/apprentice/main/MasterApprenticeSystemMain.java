package com.qktx.master.apprentice.main;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.qktx.master.apprentice.config.MasterApprenticeConfig;
import com.qktx.master.apprentice.handle.MasterApprenticeSystemHandle;
import com.qktx.master.apprentice.handle.HandleResultType;
import com.qktx.master.apprentice.http.MasterApprenticeSystemHttpServer;
import com.qktx.master.apprentice.mysql.MySQLClient;
import com.qktx.master.apprentice.rabbitmq.RabbitClient;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class MasterApprenticeSystemMain {

	private static Logger logger = Logger.getLogger(MasterApprenticeSystemMain.class);

	void startRabbitConsumer(MasterApprenticeConfig config, Channel consumerChannel,
			MasterApprenticeSystemHandle dbHandler) {
		logger.info("start to consumer user info...");
		logger.info("rabbitmq consumer queue manual ack is " + config.getRabbitConsumerManualAck() + ".");

		// 创建队列消费者
		Consumer consumer = new DefaultConsumer(consumerChannel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
					byte[] body) throws IOException {
				String str = new String(body, "UTF-8");
				if (config.getRabbitConsumerManualAck()) {
					logger.debug("manual ack" + envelope.getDeliveryTag());
					while (dbHandler.handle(str) == HandleResultType.HANLER_FAILED) {
						try {
							Thread.sleep(1 * 1000);
						} catch (InterruptedException e) {
							e.printStackTrace();
							logger.error("Thread sleep InterruptedException,exit.");
							System.exit(1);
						}
					}
					consumerChannel.basicAck(envelope.getDeliveryTag(), false); // 手动ack
				} else {
					dbHandler.handle(str); // 存储到mysql,自动ack
				}
				str = null;
				throw new IOException();
			}
		};

		if (config.getRabbitConsumerManualAck()) {
			// 创建队列消费者
			try {
				consumerChannel.basicConsume(config.getRabbitConsumerQueueName(), false, consumer); // 手动ack
			} catch (IOException e) {
				e.printStackTrace();
			}
		} else {
			try {
				consumerChannel.basicConsume(config.getRabbitConsumerQueueName(), true, consumer); // 自动ack
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	public static void main(String[] args) {
		MasterApprenticeConfig config = new MasterApprenticeConfig();

		ConnectionFactory consumerfactory = new ConnectionFactory();
		consumerfactory.setHost(config.getRabbitConsumerHost());
		consumerfactory.setPort(config.getRabbitConsumerPort());
		consumerfactory.setUsername(config.getRabbitConsumerUser());
		consumerfactory.setPassword(config.getRabbitConsumerPasswd());
		consumerfactory.setAutomaticRecoveryEnabled(config.getRabbitConsumerAutomaticRecoveryEnable());
		consumerfactory.setTopologyRecoveryEnabled(true);
		consumerfactory.setNetworkRecoveryInterval(5);

		MySQLClient mysqlClient = new MySQLClient(config);
		MasterApprenticeSystemHandle dbHandler = new MasterApprenticeSystemHandle(mysqlClient, config);

		RabbitClient consumerRabbitClient = new RabbitClient(consumerfactory, config, dbHandler);
		consumerRabbitClient.startRabbitConsumer();

		MasterApprenticeSystemHttpServer httpServer = new MasterApprenticeSystemHttpServer(config, mysqlClient);
		httpServer.start();

	}

}
