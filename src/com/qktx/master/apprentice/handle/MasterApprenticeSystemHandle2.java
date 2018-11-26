package com.qktx.master.apprentice.handle;

import java.util.Map;

import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSONObject;
import com.qktx.master.apprentice.config.MasterApprenticeConfig;
import com.qktx.master.apprentice.jedis.RedisClient;
import com.qktx.master.apprentice.queue.RabbitClient;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

public class MasterApprenticeSystemHandle2 {
	private static Logger logger = Logger.getLogger(MasterApprenticeSystemHandle2.class);

	private JedisPool jedisPool;
	private Channel produceChannel;
	MasterApprenticeConfig config;

	public MasterApprenticeSystemHandle2(Channel produceChannel, JedisPool jedisPool, MasterApprenticeConfig config) {
		this.jedisPool = jedisPool;
		this.produceChannel = produceChannel;
		this.config = config;

	}

	public void handle(String msg) {
		Map<String, Object> map = (Map<String, Object>) JSONObject.parse(msg);
		String sqlType = (String) map.get(config.getMetaSqltypeName());
		filter(map);
		switch (sqlType) {
		case "INSERT":
			handlerInsert(map);
			break;
		case "UPDATE":
			handlerUpdate(map);
			break;
		case "DELETE":
			handlerDelete(map);
			break;
		default:
			logger.debug("unknown SQL_TYPE " + sqlType);
			break;
		}
	}

	public void filter(Map<String, Object> map) {
		String tableName = (String) map.get(config.getMetaTableName());
		String schemaName = (String) map.get(config.getMetaDatabaseName());
		if (!tableName.equals(config.getTableName()) || !schemaName.equals(config.getSchemaName())) {
			logger.debug("unknown table " + schemaName + "." + tableName);
			return;
		}
	}

	private void handlerInsert(Map<String, Object> map) {
		// TODO Auto-generated method stub
		String parentUserID = (String) map.get("PARENT_USER_ID");
		String userID = (String) map.get("USER_ID");
		// String userStatus = (String)map.get("STATUS");

		if (parentUserID.equals("-99")) {
			return;
		} else {
			this.handleMasterAndApprentice(userID, parentUserID);
		}
	}

	private void handlerUpdate(Map<String, Object> map) {
		// TODO Auto-generated method stub
		Map<String, String> beforMap = (Map<String, String>) map.get(config.getUpdateBeforName());
		Map<String, String> afterMap = (Map<String, String>) map.get(config.getUpdateAfterName());

		String beforParentUserID = beforMap.get("PARENT_USER_ID");
		String afterParentUserID = afterMap.get("PARENT_USER_ID");

		// String afterUserStatus = afterMap.get("STATUS");

		String beforUserID = beforMap.get("USER_ID");
		String afterUserID = afterMap.get("USER_ID");
		// 上下级关系无变化,或者beforParentUserID与afterParentUserID都为-99
		if (beforParentUserID.equals(afterParentUserID)) {
			return;
		}
		// 用户的userid发生变化
		else if (!beforUserID.equals(afterUserID)) {
			logger.error("userID has changed. befor update userID is:" + beforUserID + ". after update userID is: "
					+ afterUserID);
			return;
		} else {
			// 用户父级ID不为-99顶级ID，解绑了原有的师徒关系。
			if (!beforParentUserID.equals("-99") || afterParentUserID.equals("-99")) {
				logger.warn("parent_user_id has changed and befor update parent_user_Id is not -99. user_id is :"
						+ beforUserID + ", befor update parent_user_id is: " + beforParentUserID
						+ ", after update parent_user_id is: " + afterParentUserID);
				releaseMasterApprentice(afterUserID, beforParentUserID, afterParentUserID);
			}

			// 增加新的上下级关系，且新的上级不为-99
			if (!afterParentUserID.equals("-99")) {
				handleMasterAndApprentice(afterUserID, afterParentUserID);
			}
		}
	}

	private void handlerDelete(Map<String, Object> map) {
		// TODO Auto-generated method stub
		String parentUserID = (String) map.get("PARENT_USER_ID");
		String userID = (String) map.get("USER_ID");
		logger.error("userID has deleted, userID: " + userID + ". parentUserID: " + parentUserID);
		return;
	}

	/**
	 * 增加新的上下级关系
	 * 
	 * @param userID
	 *            用户ID
	 * @param parentUserID
	 *            父级ID
	 */
	public void handleMasterAndApprentice(String userID, String parentUserID) {
		Jedis jedis = this.jedisPool.getResource();
		try {

			Map<String, String> masterUserIDMap = jedis.hgetAll(config.getMasterUserIDSKeyPrefix() + parentUserID); // 取得所有父级ID
			Map<String, String> apprenticeUserIDMap = jedis.hgetAll(config.getApprenticeUserIDSKeyPrefix() + userID); // 取得用户所有下级ID。

			// 设置用户的顶级用户ID。
			String topMasterUserID = null;
			if (parentUserID.equals("-99")) {
				topMasterUserID = userID;
			} else {
				topMasterUserID = jedis.get(config.getTopMasterUserIDSKeyPrefix() + parentUserID);
				if (topMasterUserID == null) {
					topMasterUserID = parentUserID;
				}
				jedis.set(config.getTopMasterUserIDSKeyPrefix() + userID, topMasterUserID);
			}

			Pipeline pipeline = jedis.pipelined();

			pipeline.hset(config.getApprenticeUserIDSKeyPrefix() + parentUserID, userID, "1"); // 将用户设置为父级的下级
			pipeline.hset(config.getMasterUserIDSKeyPrefix() + userID, parentUserID, "1"); // 添加用户的父级
			if (apprenticeUserIDMap.size() > 0) {
				pipeline.hmset(config.getApprenticeUserIDSKeyPrefix() + parentUserID, apprenticeUserIDMap); // 将用户的所有下级，设置为用户父级的下级
			}

			/**
			 * 将用户自己与所有的下级，设置为用户所有上级的下级
			 */
			for (Map.Entry<String, String> masterEntry : masterUserIDMap.entrySet()) {
				pipeline.hset(config.getApprenticeUserIDSKeyPrefix() + masterEntry.getKey(), userID, "1"); // 将用户设置为上层父级的下级
				pipeline.hset(config.getMasterUserIDSKeyPrefix() + userID, masterEntry.getKey(), "1"); // 添加上层父级为用户的父级。
				if (apprenticeUserIDMap.size() > 0) {
					pipeline.hmset(config.getApprenticeUserIDSKeyPrefix() + masterEntry.getKey(), apprenticeUserIDMap); // 添加用户的所有下级到用户的上层父级
				}
			}

			/**
			 * 将用户所有的上级，设置为用户所有下级的上级。 新增用戶忽略這一步
			 */
			for (Map.Entry<String, String> apprenticeEntry : apprenticeUserIDMap.entrySet()) {
				pipeline.hset(config.getMasterUserIDSKeyPrefix() + apprenticeEntry.getKey(), parentUserID, "1");
				if (masterUserIDMap.size() > 0) {
					pipeline.hmset(config.getMasterUserIDSKeyPrefix() + apprenticeEntry.getKey(), masterUserIDMap);
				}
				pipeline.set(config.getTopMasterUserIDSKeyPrefix() + apprenticeEntry.getKey(), topMasterUserID); // 设置用户下级的顶级用户ID
			}
			pipeline.sync();

		} catch (Exception e) {
			logger.error("save userid to parent failed, userID: " + userID + ". parentUserID: " + parentUserID);
			e.printStackTrace();
		} finally {
			this.jedisPool.returnResourceObject(jedis);
		}
	}

	/**
	 * 当父级ID发生变更时，解除上下级的关系
	 * 
	 * @param userID
	 * @param oldParentUserID
	 *            变更前的上级ID，非-99
	 * @param newParentUserID
	 *            变更后的上级ID，非-99
	 */
	public void releaseMasterApprentice(String userID, String oldParentUserID, String newParentUserID) {
		Jedis jedis = this.jedisPool.getResource();
		try {
			Map<String, String> masterUserIDMap = jedis.hgetAll(config.getMasterUserIDSKeyPrefix() + userID); // 用户的所有父级
			Map<String, String> apprenticeUserIDMap = jedis.hgetAll(config.getApprenticeUserIDSKeyPrefix() + userID); // 用户所有下级。
			Pipeline pipeline = jedis.pipelined();

			pipeline.del(config.getTopMasterUserIDSKeyPrefix() + userID); // 删除用户的顶级父类ID

			// 把用户与徒弟从所有的父级中删除
			for (Map.Entry<String, String> masterEntry : masterUserIDMap.entrySet()) {
				pipeline.hdel(config.getApprenticeUserIDSKeyPrefix() + masterEntry.getKey(), userID);
				for (Map.Entry<String, String> apprenticeEntry : apprenticeUserIDMap.entrySet()) {
					pipeline.hdel(config.getApprenticeUserIDSKeyPrefix() + masterEntry.getKey(),
							apprenticeEntry.getKey());
				}
			}

			// 把用户的父级从所有的徒弟中删除
			for (Map.Entry<String, String> apprenticeEntry : apprenticeUserIDMap.entrySet()) {
				for (Map.Entry<String, String> masterEntry : masterUserIDMap.entrySet()) {
					pipeline.hdel(config.getMasterUserIDSKeyPrefix() + apprenticeEntry.getKey(), masterEntry.getKey());
				}
				pipeline.set(config.getTopMasterUserIDSKeyPrefix() + apprenticeEntry.getKey(), userID); // 将用户的徒弟顶级父类ID设置为自己。
			}
			pipeline.sync();

		} catch (Exception e) {
			logger.error("release user master and apprentice failed, userID: " + userID + ". oldParentUserID: "
					+ oldParentUserID + ". newParentUserID: " + newParentUserID);
			e.printStackTrace();
		} finally {
			this.jedisPool.returnResourceObject(jedis);
		}
	}

	public static void main(String args[]) {

		MasterApprenticeConfig config = new MasterApprenticeConfig();

		RedisClient redisClient = new RedisClient(config);
		JedisPool jedisPool = redisClient.getJedisPool();

		Jedis jedis = jedisPool.getResource();

		System.out.println(jedis.get("TOP_MASTER_803653"));
		Pipeline p = jedis.pipelined();
		p.set("abc", "123");
		System.out.println(jedis.get("abc") + "%%%%%%%%%%%%");
		p.set("cba", "321");
		p.sync();
		jedisPool.returnResource(jedis);

	}

}
