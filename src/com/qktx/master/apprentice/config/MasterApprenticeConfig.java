package com.qktx.master.apprentice.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

public class MasterApprenticeConfig {

	private String rabbitConsumerHost;
	private int rabbitConsumerPort;
	private String rabbitConsumerUser;
	private String rabbitConsumerPasswd;
	private String rabbitConsumerExchangeName;
	private String rabbitConsumerExchangeType;
	private String rabbitConsumerRoutingKey;
	private String rabbitConsumerQueueName;
	private boolean rabbitConsumerAutomaticRecoveryEnable = true;
	private boolean rabbitConsumerManualAck = true;

	private String rabbitProduceHost;
	private int rabbitProducePort;
	private String rabbitProduceUser;
	private String rabbitProducePasswd;
	private String rabbitProduceExchangeName;
	private String rabbitProduceExchangeType;
	private String rabbitProduceRoutingKey;
	private String rabbitProduceQueueName;
	private boolean rabbitProduceExchangeDurable;
	private boolean rabbitProduceQueueDurable;

	private String redisHost;
	private int redisPort;
	private String redisPasswd;
	private boolean testWhileIdle;
	private int minIdle;
	private int maxIdle;
	private int maxTotal;
	private long maxWaitMillis;

	private String masterUserIDSKeyPrefix = "MASTER_";
	private String apprenticeUserIDSKeyPrefix = "APPRENTICE_";
	private String topMasterUserIDSKeyPrefix = "TOP_MASTER_";

	private String metaTableName = "TABLE_NAME";
	private String metaDatabaseName = "DATABASE_NAME";
	private String metaBinLogFileName = "BINLOG_FILE";
	private String metaBinlogPositionName = "BINLOG_POS";
	private String metaSqltypeName = "SQL_TYPE";
	private String updateBeforName = "BEFOR_VALUE";
	private String updateAfterName = "AFTER_VALUE";

	private String tableName = "HHZ_USER";
	private String schemaName = "QKTX_DB";

	private int httpPort = 8080;
	private String httpHost = "127.0.0.1";

	private String mysqlDriverClassName = "com.mysql.jdbc.Driver";
	private String mysqlURL;
	private String mysqlUser;
	private String mysqlPasswd;
	private int mysqlPoolInitialSize = 5;
	private int mysqlPoolMinIdle = 5;
	private int mysqlPoolMaxActive = 100;
	private boolean mysqlPoolRemoveAbandoned = true;
	private int mysqlPoolRemoveAbandonedTimeout = 180;
	private boolean mysqlPoolTestWhileIdle = true;
	private long mysqlPoolMinEvictableIdleTimeMillis = 30000;
	private String mysqlPoolValidationQuery = "select 1";
	private long mysqlPoolTimeBetweenEvictionRunsMillis = 30000;

	private static Logger logger = Logger.getLogger(MasterApprenticeConfig.class);

	public MasterApprenticeConfig() {
		this.initConfig();
	}

	public void initConfig() {
		logger.info("read config qktx.conf");
		Properties properties = new Properties();
		InputStream in = MasterApprenticeConfig.class.getClassLoader().getResourceAsStream("qktx.conf");
		try {
			properties.load(in);
			this.rabbitConsumerHost = properties.getProperty("qktx.rabbitmq.consumer.host").trim();
			this.rabbitConsumerPort = Integer.valueOf(properties.getProperty("qktx.rabbitmq.consumer.port").trim());
			this.rabbitConsumerUser = properties.getProperty("qktx.rabbitmq.consumer.user").trim();
			this.rabbitConsumerPasswd = properties.getProperty("qktx.rabbitmq.consumer.passwd").trim();
			this.rabbitConsumerExchangeName = properties.getProperty("qktx.rabbitmq.consumer.exchange.name").trim();
			this.rabbitConsumerExchangeType = properties.getProperty("qktx.rabbitmq.consumer.exchange.type").trim();
			this.rabbitConsumerRoutingKey = properties.getProperty("qktx.rabbitmq.consumer.routing.key").trim();
			this.rabbitConsumerQueueName = properties.getProperty("qktx.rabbitmq.consumer.queue.name").trim();
			this.rabbitConsumerAutomaticRecoveryEnable = Boolean
					.valueOf(properties.getProperty("qktx.rabbitmq.consumer.automaticRecoveryEnable").trim());
			this.rabbitConsumerManualAck = Boolean
					.valueOf(properties.getProperty("qktx.rabbitmq.consumer.manualAck").trim());

			// this.rabbitProduceHost =
			// properties.getProperty("qktx.rabbitmq.produce.host").trim();
			// this.rabbitProducePort =
			// Integer.valueOf(properties.getProperty("qktx.rabbitmq.produce.port").trim());
			// this.rabbitProduceUser =
			// properties.getProperty("qktx.rabbitmq.produce.user").trim();
			// this.rabbitProducePasswd =
			// properties.getProperty("qktx.rabbitmq.produce.passwd").trim();
			// this.rabbitProduceExchangeName =
			// properties.getProperty("qktx.rabbitmq.produce.exchange.name").trim();
			// this.rabbitProduceExchangeType =
			// properties.getProperty("qktx.rabbitmq.produce.exchange.type").trim();
			// this.rabbitProduceRoutingKey =
			// properties.getProperty("qktx.rabbitmq.produce.routing.key").trim();
			// this.rabbitProduceQueueName =
			// properties.getProperty("qktx.rabbitmq.produce.queue.name").trim();
			// this.rabbitProduceExchangeDurable = Boolean
			// .valueOf(properties.getProperty("qktx.rabbitmq.produce.exchange.durable").trim());
			// this.rabbitProduceQueueDurable = Boolean
			// .valueOf(properties.getProperty("qktx.rabbitmq.produce.queue.durable").trim());

			// this.redisHost = properties.getProperty("qktx.rabbitmq.produce.host").trim();
			// this.redisPort =
			// Integer.valueOf(properties.getProperty("qktx.redis.port").trim());
			// this.redisPasswd = properties.getProperty("qktx.redis.passwd").trim();
			// this.testWhileIdle =
			// Boolean.valueOf(properties.getProperty("qktx.redis.TestWhileIdle").trim());
			// this.minIdle =
			// Integer.valueOf(properties.getProperty("qktx.redis.MinIdle").trim());
			// this.maxIdle =
			// Integer.valueOf(properties.getProperty("qktx.redis.MaxIdle").trim());
			// this.maxTotal =
			// Integer.valueOf(properties.getProperty("qktx.redis.MaxTotal").trim());
			// this.maxWaitMillis =
			// Long.valueOf(properties.getProperty("qktx.redis.MaxWaitMillis").trim());
			//
			// this.masterUserIDSKeyPrefix =
			// properties.getProperty("qktx.master.userids.key.prefix").trim().toUpperCase();
			// this.apprenticeUserIDSKeyPrefix =
			// properties.getProperty("qktx.apprentice.userids.key.prefix").trim()
			// .toUpperCase();
			// this.topMasterUserIDSKeyPrefix =
			// properties.getProperty("qktx.top.master.userid.key.prefix").trim()
			// .toUpperCase();

			this.metaTableName = properties.getProperty("fd.meta.data.tablename").trim().toUpperCase();
			this.metaDatabaseName = properties.getProperty("fd.meta.data.databasename").trim().toUpperCase();
			this.metaBinLogFileName = properties.getProperty("fd.meta.data.binlogfilename").trim().toUpperCase();
			this.metaBinlogPositionName = properties.getProperty("fd.meta.data.binlogposition").trim().toUpperCase();
			this.metaSqltypeName = properties.getProperty("fd.meta.data.sqltype").trim().toUpperCase();
			this.updateBeforName = properties.getProperty("fd.result.data.update.beforname").trim().toUpperCase();
			this.updateAfterName = properties.getProperty("fd.result.data.update.aftername").trim().toUpperCase();

			this.tableName = properties.getProperty("qktx.table.name").trim().toUpperCase();
			this.schemaName = properties.getProperty("qktx.schema.name").trim().toUpperCase();
			this.httpPort = Integer.valueOf(properties.getProperty("qktx.http.server.bind.port").trim());
			this.httpHost = properties.getProperty("qktx.http.server.bind.host").trim();

			this.mysqlDriverClassName = properties.getProperty("qktx.mysql.driver.class.name").trim();
			this.mysqlURL = properties.getProperty("qktx.mysql.url").trim();
			this.mysqlUser = properties.getProperty("qktx.mysql.user").trim();
			this.mysqlPasswd = properties.getProperty("qktx.mysql.passwd").trim();
			this.mysqlPoolInitialSize = Integer.valueOf(properties.getProperty("qktx.mysql.pool.initSize").trim());
			this.mysqlPoolMinIdle = Integer.valueOf(properties.getProperty("qktx.mysql.pool.minIdle").trim());
			this.mysqlPoolMaxActive = Integer.valueOf(properties.getProperty("qktx.mysql.pool.maxActive").trim());
			this.mysqlPoolRemoveAbandoned = Boolean
					.valueOf(properties.getProperty("qktx.mysql.pool.removeAbandoned").trim());
			this.mysqlPoolRemoveAbandonedTimeout = Integer
					.valueOf(properties.getProperty("qktx.mysql.pool.removeAbandonedTimeout").trim());
			this.mysqlPoolTestWhileIdle = Boolean
					.valueOf(properties.getProperty("qktx.mysql.pool.testWhileIdle").trim());
			this.mysqlPoolMinEvictableIdleTimeMillis = Long
					.valueOf(properties.getProperty("qktx.mysql.pool.minEvictableIdleTimeMillis").trim());
			this.mysqlPoolValidationQuery = properties.getProperty("qktx.mysql.pool.validationQuery").trim()
					.toUpperCase();
			this.mysqlPoolTimeBetweenEvictionRunsMillis = Long
					.valueOf(properties.getProperty("qktx.mysql.pool.timeBetweenEvictionRunsMillis").trim());

			logger.info("read config finished.");
		} catch (IOException e) {
			logger.error("config file load failed" + e.getMessage());
			e.printStackTrace();
		} finally {
			try {
				in.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	public String getRabbitConsumerHost() {
		return rabbitConsumerHost;
	}

	public void setRabbitConsumerHost(String rabbitConsumerHost) {
		this.rabbitConsumerHost = rabbitConsumerHost;
	}

	public int getRabbitConsumerPort() {
		return rabbitConsumerPort;
	}

	public void setRabbitConsumerPort(int rabbitConsumerPort) {
		this.rabbitConsumerPort = rabbitConsumerPort;
	}

	public String getRabbitConsumerUser() {
		return rabbitConsumerUser;
	}

	public void setRabbitConsumerUser(String rabbitConsumerUser) {
		this.rabbitConsumerUser = rabbitConsumerUser;
	}

	public String getRabbitConsumerPasswd() {
		return rabbitConsumerPasswd;
	}

	public void setRabbitConsumerPasswd(String rabbitConsumerPasswd) {
		this.rabbitConsumerPasswd = rabbitConsumerPasswd;
	}

	public String getRabbitConsumerExchangeName() {
		return rabbitConsumerExchangeName;
	}

	public void setRabbitConsumerExchangeName(String rabbitConsumerExchangeName) {
		this.rabbitConsumerExchangeName = rabbitConsumerExchangeName;
	}

	public String getRabbitConsumerExchangeType() {
		return rabbitConsumerExchangeType;
	}

	public void setRabbitConsumerExchangeType(String rabbitConsumerExchangeType) {
		this.rabbitConsumerExchangeType = rabbitConsumerExchangeType;
	}

	public String getRabbitConsumerRoutingKey() {
		return rabbitConsumerRoutingKey;
	}

	public void setRabbitConsumerRoutingKey(String rabbitConsumerRoutingKey) {
		this.rabbitConsumerRoutingKey = rabbitConsumerRoutingKey;
	}

	public String getRabbitConsumerQueueName() {
		return rabbitConsumerQueueName;
	}

	public void setRabbitConsumerQueueName(String rabbitConsumerQueueName) {
		this.rabbitConsumerQueueName = rabbitConsumerQueueName;
	}

	public boolean getRabbitConsumerAutomaticRecoveryEnable() {
		return rabbitConsumerAutomaticRecoveryEnable;
	}

	public void setRabbitConsumerAutomaticRecoveryEnable(boolean rabbitConsumerAutomaticRecoveryEnable) {
		this.rabbitConsumerAutomaticRecoveryEnable = rabbitConsumerAutomaticRecoveryEnable;
	}

	public boolean getRabbitConsumerManualAck() {
		return rabbitConsumerManualAck;
	}

	public void setRabbitConsumerManualAck(boolean rabbitConsumerManualAck) {
		this.rabbitConsumerManualAck = rabbitConsumerManualAck;
	}

	public String getRabbitProduceHost() {
		return rabbitProduceHost;
	}

	public void setRabbitProduceHost(String rabbitProduceHost) {
		this.rabbitProduceHost = rabbitProduceHost;
	}

	public int getRabbitProducePort() {
		return rabbitProducePort;
	}

	public void setRabbitProducePort(int rabbitProducePort) {
		this.rabbitProducePort = rabbitProducePort;
	}

	public String getRabbitProduceUser() {
		return rabbitProduceUser;
	}

	public void setRabbitProduceUser(String rabbitProduceUser) {
		this.rabbitProduceUser = rabbitProduceUser;
	}

	public String getRabbitProducePasswd() {
		return rabbitProducePasswd;
	}

	public void setRabbitProducePasswd(String rabbitProducePasswd) {
		this.rabbitProducePasswd = rabbitProducePasswd;
	}

	public String getRabbitProduceExchangeName() {
		return rabbitProduceExchangeName;
	}

	public void setRabbitProduceExchangeName(String rabbitProduceExchangeName) {
		this.rabbitProduceExchangeName = rabbitProduceExchangeName;
	}

	public String getRabbitProduceExchangeType() {
		return rabbitProduceExchangeType;
	}

	public void setRabbitProduceExchangeType(String rabbitProduceExchangeType) {
		this.rabbitProduceExchangeType = rabbitProduceExchangeType;
	}

	public String getRabbitProduceRoutingKey() {
		return rabbitProduceRoutingKey;
	}

	public void setRabbitProduceRoutingKey(String rabbitProduceRoutingKey) {
		this.rabbitProduceRoutingKey = rabbitProduceRoutingKey;
	}

	public String getRabbitProduceQueueName() {
		return rabbitProduceQueueName;
	}

	public void setRabbitProduceQueueName(String rabbitProduceQueueName) {
		this.rabbitProduceQueueName = rabbitProduceQueueName;
	}

	public boolean isRabbitProduceExchangeDurable() {
		return rabbitProduceExchangeDurable;
	}

	public void setRabbitProduceExchangeDurable(boolean rabbitProduceExchangeDurable) {
		this.rabbitProduceExchangeDurable = rabbitProduceExchangeDurable;
	}

	public boolean isRabbitProduceQueueDurable() {
		return rabbitProduceQueueDurable;
	}

	public void setRabbitProduceQueueDurable(boolean rabbitProduceQueueDurable) {
		this.rabbitProduceQueueDurable = rabbitProduceQueueDurable;
	}

	public String getRedisHost() {
		return redisHost;
	}

	public void setRedisHost(String redisHost) {
		this.redisHost = redisHost;
	}

	public int getRedisPort() {
		return redisPort;
	}

	public void setRedisPort(int redisPort) {
		this.redisPort = redisPort;
	}

	public String getRedisPasswd() {
		return redisPasswd;
	}

	public void setRedisPasswd(String redisPasswd) {
		this.redisPasswd = redisPasswd;
	}

	public boolean getTestWhileIdle() {
		return testWhileIdle;
	}

	public void setTestWhileIdle(boolean testWhileIdle) {
		this.testWhileIdle = testWhileIdle;
	}

	public int getMinIdle() {
		return minIdle;
	}

	public void setMinIdle(int minIdle) {
		this.minIdle = minIdle;
	}

	public int getMaxIdle() {
		return maxIdle;
	}

	public void setMaxIdle(int maxIdle) {
		this.maxIdle = maxIdle;
	}

	public int getMaxTotal() {
		return maxTotal;
	}

	public void setMaxTotal(int maxTotal) {
		this.maxTotal = maxTotal;
	}

	public long getMaxWaitMillis() {
		return maxWaitMillis;
	}

	public void setMaxWaitMillis(long maxWaitMillis) {
		this.maxWaitMillis = maxWaitMillis;
	}

	public String getMasterUserIDSKeyPrefix() {
		return masterUserIDSKeyPrefix;
	}

	public void setMasterUserIDSKeyPrefix(String masterUserIDSKeyPrefix) {
		this.masterUserIDSKeyPrefix = masterUserIDSKeyPrefix;
	}

	public String getApprenticeUserIDSKeyPrefix() {
		return apprenticeUserIDSKeyPrefix;
	}

	public void setApprenticeUserIDSKeyPrefix(String apprenticeUserIDSKeyPrefix) {
		this.apprenticeUserIDSKeyPrefix = apprenticeUserIDSKeyPrefix;
	}

	public String getMetaTableName() {
		return metaTableName;
	}

	public void setMetaTableName(String metaTableName) {
		this.metaTableName = metaTableName;
	}

	public String getMetaDatabaseName() {
		return metaDatabaseName;
	}

	public void setMetaDatabaseName(String metaDatabaseName) {
		this.metaDatabaseName = metaDatabaseName;
	}

	public String getMetaBinLogFileName() {
		return metaBinLogFileName;
	}

	public void setMetaBinLogFileName(String metaBinLogFileName) {
		this.metaBinLogFileName = metaBinLogFileName;
	}

	public String getMetaBinlogPositionName() {
		return metaBinlogPositionName;
	}

	public void setMetaBinlogPositionName(String metaBinlogPositionName) {
		this.metaBinlogPositionName = metaBinlogPositionName;
	}

	public String getMetaSqltypeName() {
		return metaSqltypeName;
	}

	public void setMetaSqltypeName(String metaSqltypeName) {
		this.metaSqltypeName = metaSqltypeName;
	}

	public String getUpdateBeforName() {
		return updateBeforName;
	}

	public void setUpdateBeforName(String updateBeforName) {
		this.updateBeforName = updateBeforName;
	}

	public String getUpdateAfterName() {
		return updateAfterName;
	}

	public void setUpdateAfterName(String updateAfterName) {
		this.updateAfterName = updateAfterName;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getSchemaName() {
		return schemaName;
	}

	public void setSchemaName(String schemaName) {
		this.schemaName = schemaName;
	}

	public int getHttpPort() {
		return httpPort;
	}

	public void setHttpPort(int httpPort) {
		this.httpPort = httpPort;
	}

	public String getHttpHost() {
		return httpHost;
	}

	public void setHttpHost(String httpHost) {
		this.httpHost = httpHost;
	}

	public String getTopMasterUserIDSKeyPrefix() {
		return topMasterUserIDSKeyPrefix;
	}

	public void setTopMasterUserIDSKeyPrefix(String topMasterUserIDSKeyPrefix) {
		this.topMasterUserIDSKeyPrefix = topMasterUserIDSKeyPrefix;
	}

	public String getMysqlUser() {
		return mysqlUser;
	}

	public void setMysqlUser(String mysqlUser) {
		this.mysqlUser = mysqlUser;
	}

	public String getMysqlPasswd() {
		return mysqlPasswd;
	}

	public void setMysqlPasswd(String mysqlPasswd) {
		this.mysqlPasswd = mysqlPasswd;
	}

	public String getMysqlURL() {
		return mysqlURL;
	}

	public void setMysqlURL(String mysqlURL) {
		this.mysqlURL = mysqlURL;
	}

	public String getMysqlDriverClassName() {
		return mysqlDriverClassName;
	}

	public void setMysqlDriverClassName(String mysqlDriverClassName) {
		this.mysqlDriverClassName = mysqlDriverClassName;
	}

	public int getMysqlPoolInitialSize() {
		return mysqlPoolInitialSize;
	}

	public void setMysqlPoolInitialSize(int mysqlPoolInitialSize) {
		this.mysqlPoolInitialSize = mysqlPoolInitialSize;
	}

	public int getMysqlPoolMinIdle() {
		return mysqlPoolMinIdle;
	}

	public void setMysqlPoolMinIdle(int mysqlPoolMinIdle) {
		this.mysqlPoolMinIdle = mysqlPoolMinIdle;
	}

	public int getMysqlPoolMaxActive() {
		return mysqlPoolMaxActive;
	}

	public void setMysqlPoolMaxActive(int mysqlPoolMaxActive) {
		this.mysqlPoolMaxActive = mysqlPoolMaxActive;
	}

	public boolean getMysqlPoolRemoveAbandoned() {
		return mysqlPoolRemoveAbandoned;
	}

	public void setMysqlPoolRemoveAbandoned(boolean mysqlPoolRemoveAbandoned) {
		this.mysqlPoolRemoveAbandoned = mysqlPoolRemoveAbandoned;
	}

	public int getMysqlPoolRemoveAbandonedTimeout() {
		return mysqlPoolRemoveAbandonedTimeout;
	}

	public void setMysqlPoolRemoveAbandonedTimeout(int mysqlPoolRemoveAbandonedTimeout) {
		this.mysqlPoolRemoveAbandonedTimeout = mysqlPoolRemoveAbandonedTimeout;
	}

	public boolean getMysqlPoolTestWhileIdle() {
		return mysqlPoolTestWhileIdle;
	}

	public void setMysqlPoolTestWhileIdle(boolean mysqlPoolTestWhileIdle) {
		this.mysqlPoolTestWhileIdle = mysqlPoolTestWhileIdle;
	}

	public long getMysqlPoolMinEvictableIdleTimeMillis() {
		return mysqlPoolMinEvictableIdleTimeMillis;
	}

	public void setMysqlPoolMinEvictableIdleTimeMillis(long mysqlPoolMinEvictableIdleTimeMillis) {
		this.mysqlPoolMinEvictableIdleTimeMillis = mysqlPoolMinEvictableIdleTimeMillis;
	}

	public String getMysqlPoolValidationQuery() {
		return mysqlPoolValidationQuery;
	}

	public void setMysqlPoolValidationQuery(String mysqlPoolValidationQuery) {
		this.mysqlPoolValidationQuery = mysqlPoolValidationQuery;
	}

	public long getMysqlPoolTimeBetweenEvictionRunsMillis() {
		return mysqlPoolTimeBetweenEvictionRunsMillis;
	}

	public void setMysqlPoolTimeBetweenEvictionRunsMillis(long mysqlPoolTimeBetweenEvictionRunsMillis) {
		this.mysqlPoolTimeBetweenEvictionRunsMillis = mysqlPoolTimeBetweenEvictionRunsMillis;
	}

}
