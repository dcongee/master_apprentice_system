package com.qktx.master.apprentice.rabbitmq;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

import com.qktx.master.apprentice.config.MasterApprenticeConfig;
import com.qktx.master.apprentice.handle.HandleResultType;
import com.qktx.master.apprentice.handle.MasterApprenticeSystemHandle;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.Recoverable;

public class RabbitClient {
	private Logger logger = Logger.getLogger(RabbitClient.class);

	private Channel channel;
	private ConnectionFactory factory;
	private Connection connection;

	private MasterApprenticeConfig config;
	private MasterApprenticeSystemHandle masterApprenticeSystemHandle;

	public RabbitClient(ConnectionFactory factory) {
		this.factory = factory;
		initRabbitClient();
	}

	public RabbitClient(ConnectionFactory factory, MasterApprenticeConfig config,
			MasterApprenticeSystemHandle masterApprenticeSystemHandle) {
		this.config = config;
		this.masterApprenticeSystemHandle = masterApprenticeSystemHandle;
		this.factory = factory;
		initRabbitClient();
	}

	public void initRabbitClient() {
		logger.info("init rabbitMQ client.");
		try {
			this.connection = this.factory.newConnection();
			this.connection.addShutdownListener(new RabbitMQShutdownListener());
			((Recoverable) connection).addRecoveryListener(new RabbitMQRecoveryListener());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TimeoutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.createChannel();
	}

	public void createChannel() {
		if (this.connection != null && this.connection.isOpen() && this.channel == null) {
			try {
				this.channel = this.connection.createChannel();
				this.channel.addShutdownListener(new RabbitMQShutdownListener());
				((Recoverable) this.channel).addRecoveryListener(new RabbitMQRecoveryListener());
				this.channel.basicQos(1);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();

			}
		}
	}

	public void reCreateChannel() {
		if (this.connection != null && this.connection.isOpen() && !this.channel.isOpen()) {
			try {
				this.channel = this.connection.createChannel();
				this.channel.addShutdownListener(new RabbitMQShutdownListener());
				((Recoverable) this.channel).addRecoveryListener(new RabbitMQRecoveryListener());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void reStartRabbitConsumer() {
		reCreateChannel();
		startRabbitConsumer();
	}

	public void startRabbitConsumer() {
		logger.info("start to consumer user info...");
		logger.info("rabbitmq consumer queue manual ack is " + config.getRabbitConsumerManualAck() + ".");

		// 创建队列消费者
		Consumer consumer = new DefaultConsumer(this.channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
					byte[] body) {
				String str = "";
				try {
					str = new String(body, "UTF-8");
				} catch (UnsupportedEncodingException e1) {
					e1.printStackTrace();
					logger.error("unsupported Encoding utf-8." + body);
					basicReject(envelope, str);
					str = null;
					return;
				}

				try {
					while (masterApprenticeSystemHandle.handle(str) == HandleResultType.HANLER_FAILED) {
						logger.error("handle return failed.sleep 1 second,wait to retry...");
						Thread.sleep(1 * 1000);
					}
				} catch (Exception e) {
					e.printStackTrace();
					logger.error("handle unknown error." + str);
					basicReject(envelope, str);
					str = null;
					return;
				}

				if (config.getRabbitConsumerManualAck()) {
					logger.debug("manual ack" + envelope.getDeliveryTag());
					try {
						// 手动ack
						channel.basicAck(envelope.getDeliveryTag(), false);
					} catch (IOException e) {
						e.printStackTrace();
						logger.error("manual ack failed." + str);
						str = null;
						return;
					}
				}
				str = null;
			}
		};

		if (this.config.getRabbitConsumerManualAck()) {
			// 创建队列消费者
			try {
				this.channel.basicConsume(this.config.getRabbitConsumerQueueName(), false, consumer); // 手动ack
			} catch (IOException e) {
				e.printStackTrace();
			}
		} else {
			try {
				this.channel.basicConsume(this.config.getRabbitConsumerQueueName(), true, consumer); // 自动ack
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	public void basicReject(Envelope envelope, String str) {
		try {
			// 手动ack
			channel.basicReject(envelope.getDeliveryTag(), false);
		} catch (IOException e) {
			e.printStackTrace();
			logger.error("manual reject failed." + str);
		}
	}

	public void startOldRabbitConsumer() {
		logger.info("start to consumer user info...");
		logger.info("rabbitmq consumer queue manual ack is " + config.getRabbitConsumerManualAck() + ".");

		// 创建队列消费者
		Consumer consumer = new DefaultConsumer(this.channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
					byte[] body) throws IOException {
				String str = new String(body, "UTF-8");

				while (masterApprenticeSystemHandle.handle(str) == HandleResultType.HANLER_FAILED) {
					try {
						Thread.sleep(1 * 1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
						logger.error("Thread sleep InterruptedException,exit.");
						// System.exit(1);
					}
				}

				if (config.getRabbitConsumerManualAck()) {
					logger.debug("manual ack" + envelope.getDeliveryTag());
					// 手动ack
					channel.basicAck(envelope.getDeliveryTag(), false);
				}
				str = null;
			}
		};

		if (this.config.getRabbitConsumerManualAck()) {
			// 创建队列消费者
			try {
				this.channel.basicConsume(this.config.getRabbitConsumerQueueName(), false, consumer); // 手动ack
			} catch (IOException e) {
				e.printStackTrace();
			}
		} else {
			try {
				this.channel.basicConsume(this.config.getRabbitConsumerQueueName(), true, consumer); // 自动ack
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	public void close(Connection connection) {
		if (connection != null) {
			try {
				connection.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public void close(Channel channel) {
		if (channel != null) {
			try {
				channel.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (TimeoutException e) {
				e.printStackTrace();
			}
		}
	}

	public Channel getChannel() {
		return channel;
	}

	public void setChannel(Channel channel) {
		this.channel = channel;
	}

	public Connection getConnection() {
		return connection;
	}

	public void setConnection(Connection connection) {
		this.connection = connection;
	}

}
