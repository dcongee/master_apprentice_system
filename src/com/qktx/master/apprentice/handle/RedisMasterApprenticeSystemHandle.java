package com.qktx.master.apprentice.handle;

import java.util.Map;

import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSONObject;
import com.qktx.master.apprentice.config.MasterApprenticeConfig;
import com.qktx.master.apprentice.jedis.RedisClient;
import com.rabbitmq.client.Channel;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

public class RedisMasterApprenticeSystemHandle {
	private static Logger logger = Logger.getLogger(RedisMasterApprenticeSystemHandle.class);

	private JedisPool jedisPool;
	private Channel produceChannel;
	MasterApprenticeConfig config;

	public RedisMasterApprenticeSystemHandle(Channel produceChannel, JedisPool jedisPool, MasterApprenticeConfig config) {
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
			this.handInsertMasterAndApprentice(userID, parentUserID);
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
		if (beforParentUserID.equals(afterParentUserID)) {
			// parentUserID更新前后没变化，表示上下级关系无变化,或者beforParentUserID与afterParentUserID都为-99
			return;
		} else if (!beforUserID.equals(afterUserID)) {
			// 用户的userid发生变化
			logger.error("userID has changed. befor update userID is:" + beforUserID + ". after update userID is: "
					+ afterUserID);
			return;
		} else if (!beforParentUserID.equals("-99") && afterParentUserID.equals("-99")) {
			// parentUserID由正常的用户ID更新为-99，表示用户解绑了原有的上下级关系。
			logger.warn(
					"parent user id has changed, befor update parent_user_Id is not -99, after update parent_user_id is -99,user_id is :"
							+ beforUserID + ", befor update parent_user_id is: " + beforParentUserID
							+ ", after update parent_user_id is: " + afterParentUserID
							+ ". release user master and apprentice");
			releaseMasterApprentice(afterUserID, beforParentUserID);
		} else if (beforParentUserID.equals("-99") && !afterParentUserID.equals("-99")) {
			// parentUserID由-99变为非-99，绑定新的上级。
			handUpdateMasterAndApprentice(afterUserID, afterParentUserID);
		} else {
			// 更新上下级关系，且新的上级不为-99
			handleUpdateMasterAndApprentice(afterUserID, beforParentUserID, afterParentUserID);
		}
	}

	public void handlerDelete(Map<String, Object> map) {
		// TODO Auto-generated method stub
		String parentUserID = (String) map.get("PARENT_USER_ID");
		String userID = (String) map.get("USER_ID");
		logger.error("userID has deleted, userID: " + userID + ". parentUserID: " + parentUserID);
		return;
	}

	/**
	 * 新注册的用户绑定上下级关系，parentUserID不为-99
	 * 
	 * @param userID
	 * @param parentUserID
	 */
	public void handInsertMasterAndApprentice(String userID, String parentUserID) {
		Jedis jedis = this.jedisPool.getResource();
		try {
			Map<String, String> masterUserIDMap = jedis.hgetAll(config.getMasterUserIDSKeyPrefix() + parentUserID); // 取得所有父级ID
			String topMasterUserID = jedis.get(config.getTopMasterUserIDSKeyPrefix() + parentUserID); // 取得用户的顶级上级ID
			if (topMasterUserID == null) {
				topMasterUserID = parentUserID;
			}

			Pipeline pipeline = jedis.pipelined();
			pipeline.set(config.getTopMasterUserIDSKeyPrefix() + userID, topMasterUserID);

			pipeline.hset(config.getApprenticeUserIDSKeyPrefix() + parentUserID, userID, "1"); // 将用户设置为父级的下级
			pipeline.hset(config.getMasterUserIDSKeyPrefix() + userID, parentUserID, "1"); // 添加用户的父级

			for (Map.Entry<String, String> masterEntry : masterUserIDMap.entrySet()) {
				pipeline.hset(config.getApprenticeUserIDSKeyPrefix() + masterEntry.getKey(), userID, "1");
				pipeline.hset(config.getMasterUserIDSKeyPrefix() + userID, masterEntry.getKey(), "1");
			}
			pipeline.sync();
		} catch (Exception e) {
			logger.error("new register user's master and apprentice save failed, userID: " + userID + ". parentUserID: "
					+ parentUserID);
			e.printStackTrace();
		} finally {
			this.jedisPool.returnResource(jedis);
		}

	}

	/**
	 * parentUserID由非-99变为-99，解决用户上下级关系
	 * 
	 * @param userID
	 * @param beforParentUserID
	 *            变更前的上级ID，非-99
	 * @param afterParentUserID
	 *            变更后的上级ID，非-99
	 */
	public void releaseMasterApprentice(String userID, String beforParentUserID) {
		Jedis jedis = this.jedisPool.getResource();
		try {
			Map<String, String> masterUserIDMap = jedis.hgetAll(config.getMasterUserIDSKeyPrefix() + userID); // 用户的所有父级
			Map<String, String> apprenticeUserIDMap = jedis.hgetAll(config.getApprenticeUserIDSKeyPrefix() + userID); // 用户所有下级。
			Pipeline pipeline = jedis.pipelined();
			this.release(pipeline, userID, masterUserIDMap, apprenticeUserIDMap);
			pipeline.sync();

		} catch (Exception e) {
			logger.error("release user master and apprentice failed, userID: " + userID + ". oldParentUserID: "
					+ beforParentUserID);
			e.printStackTrace();
		} finally {
			this.jedisPool.returnResource(jedis);
		}
	}

	/**
	 * parentUserID由-99 变为非-99,绑定新的上级
	 * 
	 * @param userID
	 * @param parentUserID
	 */
	public void handUpdateMasterAndApprentice(String userID, String parentUserID) {
		Jedis jedis = this.jedisPool.getResource();
		try {
			Map<String, String> masterUserIDMap = jedis.hgetAll(config.getMasterUserIDSKeyPrefix() + parentUserID); // 用户所有父级ID
			Map<String, String> apprenticeUserIDMap = jedis.hgetAll(config.getApprenticeUserIDSKeyPrefix() + userID); // 用户所有下级ID。

			String topMasterUserID = jedis.get(config.getTopMasterUserIDSKeyPrefix() + parentUserID);
			if (topMasterUserID == null) {
				topMasterUserID = parentUserID;
			}
			Pipeline pipeline = jedis.pipelined();

			this.add(pipeline, userID, parentUserID, topMasterUserID, masterUserIDMap, apprenticeUserIDMap);
			pipeline.sync();
		} catch (Exception e) {
			logger.error(
					"update user master and apprentice failed, userID: " + userID + ". parentUserID: " + parentUserID);
			e.printStackTrace();
		} finally {
			this.jedisPool.returnResource(jedis);
		}

	}

	/**
	 * parentUserID发生变成，且变更前后都不为-99
	 * 
	 * @param userID
	 * @param beforParentUserID
	 * @param afterParentUserID
	 */
	public void handleUpdateMasterAndApprentice(String userID, String beforParentUserID, String afterParentUserID) {
		Jedis jedis = this.jedisPool.getResource();
		try {
			Map<String, String> afterMasterUserIDMap = jedis
					.hgetAll(config.getMasterUserIDSKeyPrefix() + afterParentUserID); // 新的上级
			Map<String, String> beforMasterUserIDMap = jedis.hgetAll(config.getMasterUserIDSKeyPrefix() + userID); // 老的上级
			Map<String, String> apprenticeUserIDMap = jedis.hgetAll(config.getApprenticeUserIDSKeyPrefix() + userID); // 用户所有下级ID。

			String topMasterUserID = jedis.get(config.getTopMasterUserIDSKeyPrefix() + afterParentUserID);
			if (topMasterUserID == null) {
				topMasterUserID = afterParentUserID;
			}

			Pipeline pipeline = jedis.pipelined();
			this.release(pipeline, userID, beforMasterUserIDMap, apprenticeUserIDMap);
			this.add(pipeline, userID, afterParentUserID, topMasterUserID, afterMasterUserIDMap, apprenticeUserIDMap);
			pipeline.sync();

		} catch (Exception e) {
			logger.error("update user master and apprentice failed, userID: " + userID + ". befor update parentUserID: "
					+ beforParentUserID + ". after update parentUserID: " + afterParentUserID);
			e.printStackTrace();
		} finally {
			this.jedisPool.returnResource(jedis);
		}

	}

	/**
	 * 
	 * @param pipeline
	 * @param userID
	 *            用户ID
	 * @param masterUserIDMap
	 *            需要解除关系的上级ID集合
	 * @param apprenticeUserIDMap
	 *            需要解除关系的下级ID集合
	 * @throws Exception
	 */

	public void release(Pipeline pipeline, String userID, Map<String, String> masterUserIDMap,
			Map<String, String> apprenticeUserIDMap) throws Exception {

		pipeline.del(config.getTopMasterUserIDSKeyPrefix() + userID); // 删除用户的顶级父类ID

		// 把用户与徒弟从所有的父级中删除
		for (Map.Entry<String, String> masterEntry : masterUserIDMap.entrySet()) {
			pipeline.hdel(config.getApprenticeUserIDSKeyPrefix() + masterEntry.getKey(), userID);
			pipeline.hdel(config.getMasterUserIDSKeyPrefix() + userID, masterEntry.getKey());
			for (Map.Entry<String, String> apprenticeEntry : apprenticeUserIDMap.entrySet()) {
				pipeline.hdel(config.getApprenticeUserIDSKeyPrefix() + masterEntry.getKey(), apprenticeEntry.getKey());
				pipeline.hdel(config.getMasterUserIDSKeyPrefix() + apprenticeEntry.getKey(), masterEntry.getKey());

			}
		}

		// 把徒弟的顶级父级ID设置为userID
		for (Map.Entry<String, String> apprenticeEntry : apprenticeUserIDMap.entrySet()) {
			pipeline.set(config.getTopMasterUserIDSKeyPrefix() + apprenticeEntry.getKey(), userID); // 将用户的徒弟顶级父类ID设置为自己。
		}

	}

	/**
	 * 
	 * @param pipeline
	 * @param userID
	 *            用户ID
	 * @param parentUserID
	 *            新的上级ID
	 * @param topMasterUserID
	 *            顶级用户ID
	 * @param masterUserIDMap
	 *            上线用户ID
	 * @param apprenticeUserIDMap
	 *            下级用户ID
	 * @throws Exception
	 */
	public void add(Pipeline pipeline, String userID, String parentUserID, String topMasterUserID,
			Map<String, String> masterUserIDMap, Map<String, String> apprenticeUserIDMap) throws Exception {

		pipeline.set(config.getTopMasterUserIDSKeyPrefix() + userID, topMasterUserID);

		pipeline.hset(config.getApprenticeUserIDSKeyPrefix() + parentUserID, userID, "1"); // 将用户设置为父级的下级
		pipeline.hset(config.getMasterUserIDSKeyPrefix() + userID, parentUserID, "1"); // 添加用户的父级
		if (apprenticeUserIDMap.size() > 0) {
			pipeline.hmset(config.getApprenticeUserIDSKeyPrefix() + parentUserID, apprenticeUserIDMap); // 将用户的所有下级，设置为用户父级的下级
		}

		for (Map.Entry<String, String> masterEntry : masterUserIDMap.entrySet()) {
			pipeline.hset(config.getApprenticeUserIDSKeyPrefix() + masterEntry.getKey(), userID, "1"); // 将用户设置为上层父级的下级
			pipeline.hset(config.getMasterUserIDSKeyPrefix() + userID, masterEntry.getKey(), "1"); // 添加上层父级为用户的父级。
			if (apprenticeUserIDMap.size() > 0) {
				pipeline.hmset(config.getApprenticeUserIDSKeyPrefix() + masterEntry.getKey(), apprenticeUserIDMap);
			}
		}

		for (Map.Entry<String, String> apprenticeEntry : apprenticeUserIDMap.entrySet()) {
			pipeline.set(config.getTopMasterUserIDSKeyPrefix() + apprenticeEntry.getKey(), topMasterUserID); // 设置用户下级的顶级用户ID
			pipeline.hset(config.getMasterUserIDSKeyPrefix() + apprenticeEntry.getKey(), parentUserID, "1");
			if (masterUserIDMap.size() > 0) {
				pipeline.hmset(config.getMasterUserIDSKeyPrefix() + apprenticeEntry.getKey(), masterUserIDMap);
			}
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
