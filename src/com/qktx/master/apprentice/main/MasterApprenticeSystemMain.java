package com.qktx.master.apprentice.main;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.qktx.master.apprentice.config.MasterApprenticeConfig;
import com.qktx.master.apprentice.handle.DBHandler;
import com.qktx.master.apprentice.handle.MasterApprenticeSystemHandle;
import com.qktx.master.apprentice.http.MasterApprenticeSystemHttpServer;
import com.qktx.master.apprentice.jedis.RedisClient;
import com.qktx.master.apprentice.mysql.MySQLClient;
import com.qktx.master.apprentice.queue.RabbitClient;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import redis.clients.jedis.JedisPool;

public class MasterApprenticeSystemMain {

	private static Logger logger = Logger.getLogger(MasterApprenticeSystemMain.class);

	public static void main(String[] args) {
		MasterApprenticeConfig config = new MasterApprenticeConfig();

		ConnectionFactory consumerfactory = new ConnectionFactory();
		consumerfactory.setHost(config.getRabbitConsumerHost());
		consumerfactory.setPort(config.getRabbitConsumerPort());
		consumerfactory.setUsername(config.getRabbitConsumerUser());
		consumerfactory.setPassword(config.getRabbitConsumerPasswd());

		RabbitClient consumerRabbitClient = new RabbitClient(consumerfactory);
		Channel consumerChannel = consumerRabbitClient.getChannel();

		ConnectionFactory producefactory = new ConnectionFactory();
		producefactory.setHost(config.getRabbitProduceHost());
		producefactory.setPort(config.getRabbitProducePort());
		producefactory.setUsername(config.getRabbitProduceUser());
		producefactory.setPassword(config.getRabbitProducePasswd());

		RabbitClient produceRabbitClient = new RabbitClient(consumerfactory);
		Channel produceChannel = produceRabbitClient.getChannel();

		// channel.exchangeDeclare("aa", "direct", true);
		// 声明一个随机队列
		// String queueName = channel.queueDeclare().getQueue();

		// 所有日志严重性级别
		// String[] severities = { "error", "info", "warning" };
		// for (String severity : severities) {
		// // 关注所有级别的日志（多重绑定）
		// channel.queueBind(queueName, "exchange_test", severity);
		// }

		// channel.queueBind("MYSQL_QUEUE", "MYSQL_EXCHANGE", "MYSQL_ROUTING_KEY");

		// System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

		RedisClient redisClient = new RedisClient(config);
		JedisPool jedisPool = redisClient.getJedisPool();

		MasterApprenticeSystemHandle masterApprenticeSystemhandle = new MasterApprenticeSystemHandle(produceChannel,
				jedisPool, config);

		MySQLClient mysqlClient = new MySQLClient(config);
		DBHandler dbHandler = new DBHandler(produceChannel, mysqlClient, config);

		logger.info("start to consumer user info...");

		// 创建队列消费者
		Consumer consumer = new DefaultConsumer(consumerChannel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
					byte[] body) throws IOException {
				// System.out.println(" [x] Received '" + envelope.getRoutingKey() + "':'" +
				// message + "'");

				// masterApprenticeSystemhandle.handle(new String(body, "UTF-8")); // 存储到redis
				dbHandler.handle(new String(body, "UTF-8")); // 存储到mysql

			}
		};
		try {
			consumerChannel.basicConsume(config.getRabbitConsumerQueueName(), true, consumer);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		MasterApprenticeSystemHttpServer httpServer = new MasterApprenticeSystemHttpServer(config, jedisPool,
				mysqlClient);
		httpServer.start();

		// while (true) {
		// System.out.println(jedisPool.getNumActive());
		// try {
		// Thread.sleep(1*1000);
		// } catch (InterruptedException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		// }
	}

}
