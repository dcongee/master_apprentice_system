package com.qktx.master.apprentice.jedis;

import org.apache.log4j.Logger;

import com.qktx.master.apprentice.config.MasterApprenticeConfig;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisClient {
	private Logger logger = Logger.getLogger(RedisClient.class);

	private JedisPool jedisPool;
	private MasterApprenticeConfig config;

	public RedisClient(MasterApprenticeConfig config) {
		this.config = config;
		this.initRedis();
	}

	public void initRedis() {
		logger.info("init redis client info.");
		JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
		jedisPoolConfig.setMinIdle(config.getMinIdle());
		jedisPoolConfig.setMaxIdle(config.getMaxIdle());
		jedisPoolConfig.setMaxTotal(config.getMaxTotal());
		jedisPoolConfig.setTestWhileIdle(config.getTestWhileIdle());
		jedisPoolConfig.setMaxWaitMillis(config.getMaxWaitMillis());
		if (config.getRedisPasswd().length() == 0 || config.getRedisPasswd() == null) {
			this.jedisPool = new JedisPool(jedisPoolConfig, config.getRedisHost(), config.getRedisPort(), 10000);
		} else {
			this.jedisPool = new JedisPool(jedisPoolConfig, config.getRedisHost(), config.getRedisPort(), 10000,
					config.getRedisPasswd());
		}
		try {
			Jedis jedis = this.jedisPool.getResource();
			jedis.set("forrest_test", "1", "nx", "ex", 1);
			this.jedisPool.returnResource(jedis);
		} catch (Exception e) {
			logger.error("redis client init failed." + e.getMessage());
			e.printStackTrace();
			System.exit(1);
		}

	}

	public JedisPool getJedisPool() {
		return jedisPool;
	}

	public void setJedisPool(JedisPool jedisPool) {
		this.jedisPool = jedisPool;
	}

}
