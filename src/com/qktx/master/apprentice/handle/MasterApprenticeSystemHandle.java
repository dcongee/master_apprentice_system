package com.qktx.master.apprentice.handle;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.qktx.master.apprentice.config.MasterApprenticeConfig;
import com.qktx.master.apprentice.mysql.MySQLClient;
import com.qktx.master.apprentice.pojo.UserInfo;
import com.rabbitmq.client.Channel;

public class MasterApprenticeSystemHandle {
	private static Logger logger = Logger.getLogger(MasterApprenticeSystemHandle.class);
	private MasterApprenticeConfig config;
	private MySQLClient mysqlClient;
	private Channel produceChannel;

	public MasterApprenticeSystemHandle(MasterApprenticeConfig config) {
		this.config = config;
	}

	public MasterApprenticeSystemHandle(Channel produceChannel, MySQLClient mysqlClient,
			MasterApprenticeConfig config) {
		this.config = config;
		this.mysqlClient = mysqlClient;
		this.produceChannel = produceChannel;
	}

	public MasterApprenticeSystemHandle(MySQLClient mysqlClient, MasterApprenticeConfig config) {
		this.config = config;
		this.mysqlClient = mysqlClient;
	}

	public HandleResultType handle(String msg) {
		Map<String, Object> map = (Map<String, Object>) JSONObject.parse(msg);
		String sqlType = (String) map.get(config.getMetaSqltypeName());
		HandleResultType resultType = filter(map);
		if (resultType == HandleResultType.FILTER_ERROR) {
			return resultType;
		}
		switch (sqlType) {
		case "INSERT":
			resultType = handlerInsert(map);
			break;
		case "UPDATE":
			resultType = handlerUpdate(map);
			break;
		case "DELETE":
			resultType = handlerDelete(map);
			break;
		case "REPAIR":
			resultType = handlerRepair(map);
			break;
		default:
			logger.debug("unknown SQL_TYPE " + sqlType + "; data map: " + map);
			resultType = HandleResultType.OTHER_ERROR;
			break;
		}
		return resultType;
	}

	public HandleResultType filter(Map<String, Object> map) {
		String tableName = (String) map.get(config.getMetaTableName());
		String schemaName = (String) map.get(config.getMetaDatabaseName());
		if (!tableName.equals(config.getTableName()) || !schemaName.equals(config.getSchemaName())) {
			logger.error("unknown table " + schemaName + "." + tableName + "; data map: " + map);
			return HandleResultType.FILTER_ERROR;
		}
		return HandleResultType.FILTER_OK;
	}

	private HandleResultType handlerInsert(Map<String, Object> map) {
		Long parentUserID = Long.valueOf((String) map.get("PARENT_USER_ID"));
		Long userID = Long.valueOf((String) map.get("USER_ID"));

		// 如果parentUserID与UserID相同，否则parentUserID设置为-99
		if (userID.equals(parentUserID) && userID != -99) {
			logger.warn("parentUserID is userID. userID is: " + userID + "; parentUserID is: " + parentUserID
					+ ". set parentUserID=-99");
			parentUserID = -99l;
		}

		DruidPooledConnection con = null;
		try {
			con = this.mysqlClient.getDataSource().getConnection();
			con.setAutoCommit(false);
			if (!this.insertParentAndApprentice(con, userID, parentUserID)) {
				logger.error("new register user's master and apprentice save failed, userID: " + userID
						+ ". parentUserID: " + parentUserID);
				con.rollback();
				return HandleResultType.HANLER_FAILED;
			}
			con.commit();
		} catch (SQLException e) {
			e.printStackTrace();
			return HandleResultType.HANLER_FAILED;
		} finally {
			this.mysqlClient.close(con);
		}
		return HandleResultType.HANDLER_OK;
	}

	private HandleResultType handlerUpdate(Map<String, Object> map) {
		// TODO Auto-generated method stub
		Map<String, Object> beforMap = (Map<String, Object>) map.get(config.getUpdateBeforName());
		Map<String, Object> afterMap = (Map<String, Object>) map.get(config.getUpdateAfterName());

		Long beforParentUserID = Long.valueOf((String) beforMap.get("PARENT_USER_ID"));
		Long afterParentUserID = Long.valueOf((String) afterMap.get("PARENT_USER_ID"));

		// String afterUserStatus = afterMap.get("STATUS");

		Long beforUserID = Long.valueOf((String) beforMap.get("USER_ID"));
		Long afterUserID = Long.valueOf((String) afterMap.get("USER_ID"));

		if (!beforUserID.equals(afterUserID)) {
			// 用户的userid发生变化
			logger.error("userID has changed. befor update userID is:" + beforUserID + ". after update userID is: "
					+ afterUserID);
			return HandleResultType.HANDLER_OK;
		}

		if (beforParentUserID.equals(afterParentUserID)) {
			// parentUserID更新前后没变化，表示上下级关系无变化,或者beforParentUserID与afterParentUserID都为-99
			return HandleResultType.HANDLER_OK;
		}

		// beforParentUserID为-99的用户，绑定自己作为自己的上级
		if (afterUserID.equals(afterParentUserID) && afterUserID != -99 && beforParentUserID == -99) {
			logger.warn("userID's parent change to self,not support. after userID is: " + afterUserID
					+ ". after parentUserID is: " + afterParentUserID);
			return HandleResultType.HANDLER_OK;
		}

		// 绑定自己作为自己的上级
		if (afterUserID.equals(afterParentUserID) && afterUserID != -99) {
			logger.warn("userID's parent change to self,not support. after userID is: " + afterUserID
					+ ". afterParentUserID is: " + afterParentUserID + ". set parentUserID=-99");
			afterParentUserID = -99l;
		}

		DruidPooledConnection con = null;
		try {
			con = this.mysqlClient.getDataSource().getConnection();
			con.setAutoCommit(false);

			if (beforParentUserID != -99 && afterParentUserID == -99) {
				// parentUserID由正常的用户ID更新为-99，表示用户解绑了原有的上下级关系。
				logger.warn(
						"parent user id has changed, befor update parent_user_Id is not -99, after update parent_user_id is -99,user_id is :"
								+ beforUserID + ", befor update parent_user_id is: " + beforParentUserID
								+ ", after update parent_user_id is: " + afterParentUserID
								+ ". release user master and apprentice");

				if (!this.releaseMasterApprentice(con, afterUserID)) {
					logger.error("user remove parent failed, user_id is :" + afterUserID
							+ ", befor update parent_user_id is: " + beforParentUserID
							+ ", after update parent_user_id is: " + afterParentUserID);
					con.rollback();
					return HandleResultType.HANLER_FAILED;
				}
				con.commit();
				return HandleResultType.HANDLER_OK;
			}

			if (beforParentUserID == -99 && afterParentUserID != -99) {
				// parentUserID由-99变为非-99，绑定新的上级。
				if (!updateMasterAndApprentice(con, afterUserID, afterParentUserID)) {
					logger.error("user add new parent failed. user_id is :" + afterUserID
							+ ", befor update parent_user_id is: " + beforParentUserID
							+ ", after update parent_user_id is: " + afterParentUserID);
					con.rollback();
					return HandleResultType.HANLER_FAILED;
				}
				con.commit();
				return HandleResultType.HANDLER_OK;
			}

			if (!beforParentUserID.equals(afterParentUserID) && beforParentUserID != -99 && afterParentUserID != -99) {
				// 更新上下级关系，且上级更新前后都不为-99
				logger.warn(
						"parent user id has changed,user_id is :" + beforUserID + ", befor update parent_user_id is: "
								+ beforParentUserID + ", after update parent_user_id is: " + afterParentUserID + ";");
				if (!handleUpdateMasterAndApprentice(con, afterUserID, beforParentUserID, afterParentUserID)) {
					logger.error("update update parent failed. user_id is :" + afterUserID
							+ ", befor update parent_user_id is: " + beforParentUserID
							+ ", after update parent_user_id is: " + afterParentUserID);
					con.rollback();
					return HandleResultType.HANLER_FAILED;
				}
				con.commit();
				return HandleResultType.HANDLER_OK;
			}

		} catch (SQLException e2) {
			e2.printStackTrace();
			return HandleResultType.HANLER_FAILED;
		} finally {
			this.mysqlClient.close(con);
		}
		return HandleResultType.HANDLER_OK;
	}

	public HandleResultType handlerDelete(Map<String, Object> map) {
		// TODO Auto-generated method stub
		Long parentUserID = Long.valueOf((String) map.get("PARENT_USER_ID"));
		Long userID = Long.valueOf((String) map.get("USER_ID"));
		logger.warn("userID has deleted, userID: " + userID + ". parentUserID: " + parentUserID);

		DruidPooledConnection con = null;
		try {
			con = this.mysqlClient.getDataSource().getConnection();
			con.setAutoCommit(false);
			List<UserInfo> apprenticeInfo = this.queryALLApprenticeByUser(con, userID);
			if (apprenticeInfo == null) {
				logger.error("userID has delete failed,query user apprentice is failed, userID: " + userID
						+ ". parentUserID: " + parentUserID);
				con.rollback();
				return HandleResultType.HANLER_FAILED;
			}

			List<UserInfo> parentInfoList = this.queryALLParentByUserID(con, userID);
			if (parentInfoList == null) {
				logger.error("userID has delete failed,query user parent is failed, userID: " + userID
						+ ". parentUserID: " + parentUserID);
				con.rollback();
				return HandleResultType.HANLER_FAILED;
			}

			// 用户的一级的徒弟变为顶级用户
			if (!this.updateUserLevel(con, userID)) {
				logger.error("userID has delete failed,update user level failed, userID: " + userID + ". parentUserID: "
						+ parentUserID);
				con.rollback();
				return HandleResultType.HANLER_FAILED;
			}

			// 删除用户的所有上级与所有下级
			if (!this.delete(con, userID)) {
				logger.error("userID has delete failed, userID: " + userID + ". parentUserID: " + parentUserID);
				con.rollback();
				return HandleResultType.HANLER_FAILED;
			}

			// 删除用户下级的上级
			if (!this.deleteParentByUser(con, apprenticeInfo, parentInfoList)) {
				logger.error("userID has delete failed,delete user apprentice's parent failed userID: " + userID
						+ ". parentUserID: " + parentUserID);
				con.rollback();
				return HandleResultType.HANLER_FAILED;
			}
			con.commit();
		} catch (SQLException e) {
			logger.error("userID has delete failed, userID: " + userID + ". parentUserID: " + parentUserID);
			e.printStackTrace();
			return HandleResultType.HANLER_FAILED;
		} finally {
			this.mysqlClient.close(con);
		}
		return HandleResultType.HANDLER_OK;
	}

	private HandleResultType handlerRepair(Map<String, Object> map) {
		// TODO Auto-generated method stub
		Long userID = Long.valueOf((String) map.get("USER_ID"));

		DruidPooledConnection con = null;
		try {
			con = this.mysqlClient.getDataSource().getConnection();
			con.setAutoCommit(false);

			// List<UserInfo> apprenticeInfoList = this.queryALLApprenticeByUser(con,
			// userID);
			List<UserInfo> parentInfoList = this.queryALLParentByUserID(con, userID);
			if (parentInfoList == null) {
				logger.error("user repair failed,query user all parents failed, userID: " + userID);
				con.rollback();
				return HandleResultType.HANLER_FAILED;
			}

			if (!this.delete(con, userID)) {
				logger.error("user repair failed,delete user all parents and apprentices failed, userID: " + userID);
				con.rollback();
				return HandleResultType.HANLER_FAILED;
			}
			// this.delete(con, apprenticeInfoList);

			if (!this.delete(con, parentInfoList)) {
				logger.error("user repair failed,delete parent's parents and apprentices failed, userID: " + userID);
				con.rollback();
				return HandleResultType.HANLER_FAILED;
			}

			Long topParentUserID = this.queryTopParentUserIDFromDataSource(con, userID);

			if (topParentUserID == null) {
				logger.error("user repair failed,query user's top parent userid failed, userID: " + userID);
				con.rollback();
				return HandleResultType.HANLER_FAILED;
			}

			if (topParentUserID == 0) {
				logger.warn("user repair failed,because top parent user id is " + topParentUserID
						+ ";userid not found in source table, userID: " + userID);
				con.rollback();
				return HandleResultType.HANDLER_OK;
			}

			List<UserInfo> userInfoList = new ArrayList<UserInfo>();
			if (!this.queryALLApprenticeFromDataSourceByUser(con, topParentUserID, userInfoList)) {
				logger.error("user repair failed,query top user's apprentices failed, userID: " + userID
						+ "; top parent userID is: " + topParentUserID);
				con.rollback();
				return HandleResultType.HANLER_FAILED;
			}

			// 保存顶级用户ID
			userInfoList.add(new UserInfo(-99l, topParentUserID));

			for (UserInfo userInfo : userInfoList) {
				Long resaveUserID = userInfo.getUserID();
				Long resaveParentUserID = userInfo.getParentUserID();
				if (!this.insertParentAndApprentice(con, resaveUserID, resaveParentUserID)) {
					logger.error("userID has repair failed,resave user info failed, userID: " + userID
							+ "; resave userID: " + resaveUserID + " resave parent userID is:" + resaveParentUserID);
					con.rollback();
					return HandleResultType.HANLER_FAILED;
				}
			}

			userInfoList = null;
			con.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return HandleResultType.HANLER_FAILED;
		}
		return HandleResultType.HANDLER_OK;
	}

	public boolean insertParentAndApprentice(DruidPooledConnection con, Long userID, Long parentUserID) {
		if (parentUserID == -99 || userID == -99) {
			// parent_user_Id为-99的直接写入数据。
			UserInfo userInfo = new UserInfo();
			userInfo.setUserID(userID);
			userInfo.setParentUserID(parentUserID);
			userInfo.setLevel(0);

			if (userID == -99 && parentUserID != -99) {
				logger.warn("userid is -99,but parent userid is not -99,parent userid is: " + parentUserID);
			}

			if (!this.insert(con, userInfo)) {
				return false;
			}
		} else {
			// 先查询出parent_user_id的所有上级
			// 将parent_user_id的所有上级设置为自己的上级
			List<UserInfo> parentUserInfoList = this.queryALLParentByUserID(con, parentUserID);
			if (parentUserInfoList == null) {
				return false;
			}

			// 处理全量数据时用到。
			// 查出userID的所有下级。
			List<UserInfo> apprenticeUserInfoList = this.queryALLApprenticeByUser(con, userID);
			if (apprenticeUserInfoList == null) {
				return false;
			}

			// userID下级新增上级信息
			if (apprenticeUserInfoList.size() > 0) {
				UserInfo userInfo = new UserInfo();
				userInfo.setParentUserID(parentUserID);
				userInfo.setLevel(1);
				if (!this.insert(con, userInfo, apprenticeUserInfoList)) {
					return false;
				}
			}

			// 将parentUserInfoList设置为userID下级的上级
			if (apprenticeUserInfoList.size() > 0 && parentUserInfoList.size() > 0) {
				if (!this.insert(con, apprenticeUserInfoList, parentUserInfoList)) {
					return false;
				}
			}

			// 将parentUserInfoList与parentUserID设置为userid的上级
			if (!this.insert(con, userID, parentUserID, parentUserInfoList)) {
				return false;
			}
		}
		return true;
	}

	/**
	 * parentUserID由正常的用户ID更新为-99，表示用户解绑了原有的上下级关系。 1.查询出userID所有的上级。
	 * 2.查询出userID所有的下级。 3.删除userID的所有上级信息,同时，userID的下级，也删除所有的上级信息（但不包括userID）
	 * 4.新增parent_user_id为-99的信息
	 * 
	 * @param con
	 * @param afterUserID
	 * @return
	 */
	public boolean releaseMasterApprentice(DruidPooledConnection con, Long afterUserID) {

		// 查询出userID所有的上级。
		List<UserInfo> parentUserInfoList = this.queryALLParentByUserID(con, afterUserID);
		if (parentUserInfoList == null) {
			return false;
		}

		// 查询出userID所有的下级。
		List<UserInfo> apprenticeUserInfoList = this.queryALLApprenticeByUser(con, afterUserID);
		if (apprenticeUserInfoList == null) {
			return false;
		}

		//// 删除userID的所有上级信息
		if (!this.deleteParentByUser(con, afterUserID)) {
			return false;
		}

		// 新增parent_user_id为-99的信息
		UserInfo userInfo = new UserInfo();
		userInfo.setUserID(afterUserID);
		userInfo.setParentUserID(-99L);
		userInfo.setLevel(0);
		if (!this.insert(con, userInfo))
			return false;

		// userID的下级，也删除所有的上级信息（但不包括userID）
		if (apprenticeUserInfoList.size() > 0 && parentUserInfoList.size() > 0) {
			if (!this.deleteParentByUser(con, apprenticeUserInfoList, parentUserInfoList)) {
				return false;
			}
		}
		return true;
	}

	/**
	 * parentUserID由-99变为afterParentUserID，绑定新的上级。 1.查询出userID所有的下级
	 * 2.查询出parentUserID所有的上级 3.更新userID的上级信息 ,并新增上级4。userID下级新增上级信息
	 * 
	 * @param afterUserID
	 *            用户ID
	 * @param afterParentUserID
	 *            新的parentUserID
	 * @return
	 */
	boolean updateMasterAndApprentice(DruidPooledConnection con, Long afterUserID, Long afterParentUserID) {
		// 1.查询出userID所有的下级。
		List<UserInfo> apprenticeUserInfoList = this.queryALLApprenticeByUser(con, afterUserID);
		if (apprenticeUserInfoList == null) {
			return false;
		}

		// 2.查询出parentUserID所有的上级.
		List<UserInfo> parentUserInfoList = this.queryALLParentByUserID(con, afterParentUserID);
		if (parentUserInfoList == null) {
			return false;
		}

		// 3.更新userID的上级信息为afterParentUserID
		this.updateParentUserID(con, afterUserID, afterParentUserID);

		// 新增上级信息
		if (parentUserInfoList.size() > 0) {
			if (!this.insert(con, afterUserID, parentUserInfoList)) {
				return false;
			}
		}

		// 4。userID下级新增上级信息
		if (apprenticeUserInfoList.size() > 0) {
			UserInfo userInfo = new UserInfo();
			userInfo.setParentUserID(afterParentUserID);
			userInfo.setLevel(1);
			if (!this.insert(con, userInfo, apprenticeUserInfoList)) {
				return false;
			}
		}

		if (apprenticeUserInfoList.size() > 0 && parentUserInfoList.size() > 0) {
			if (!this.insert(con, apprenticeUserInfoList, parentUserInfoList)) {
				return false;
			}
		}
		return true;
	}

	/**
	 * 更新上下级关系，且上级更新前后都不为-99
	 * 
	 * @param con
	 * @param afterUserID
	 *            用户ID
	 * @param beforParentUserID
	 *            变更前的parentUserID,非-99
	 * @param afterParentUserID
	 *            变更后的parentUserID,非-99
	 * @return
	 */
	boolean handleUpdateMasterAndApprentice(DruidPooledConnection con, Long afterUserID, Long beforParentUserID,
			Long afterParentUserID) {
		if (!this.releaseMasterApprentice(con, afterUserID)) {
			return false;
		}
		if (!this.updateMasterAndApprentice(con, afterUserID, afterParentUserID)) {
			return false;
		}

		return true;
	}

	/**
	 * 查询userID的所有上级
	 * 
	 * @param con
	 * @param apprenticeUserID
	 * @param parentUserID
	 * @return
	 */
	public List<UserInfo> queryALLParentByUserID(DruidPooledConnection con, Long userID) {
		PreparedStatement ps = null;
		ResultSet rs = null;
		String sql = "select user_id,parent_user_id,level from qz_user_parent where user_id=? and parent_user_id<>-99";
		List<UserInfo> list = new ArrayList<UserInfo>();
		try {
			ps = con.prepareStatement(sql);
			ps.setLong(1, userID);
			rs = ps.executeQuery();
			while (rs.next()) {
				Long parentUserID = rs.getLong("parent_user_id");
				int level = rs.getInt("level");
				UserInfo userInfo = new UserInfo();
				userInfo.setUserID(rs.getLong("user_id"));
				userInfo.setParentUserID(parentUserID);
				userInfo.setLevel(level);
				list.add(userInfo);
			}
			// int newLevel = list.size() + 1;
			// list.add(new UserInfo(parentUserID, userID, newLevel));
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		} finally {
			this.mysqlClient.close(ps, rs);
		}
		return list;
	}

	/**
	 * parentUserID给你list设置为用户userID的上级
	 * 
	 * @param con
	 * @param list
	 * @return
	 */
	public boolean insert(DruidPooledConnection con, Long userID, Long parentUserID, List<UserInfo> list) {
		PreparedStatement ps = null;
		String sql = "insert ignore into qz_user_parent set user_id=?,parent_user_id=?,level=?";
		try {
			ps = con.prepareStatement(sql);

			if (!userID.equals(parentUserID)) {
				ps.setLong(1, userID);
				ps.setLong(2, parentUserID);
				ps.setInt(3, 1);
				ps.addBatch();
			}

			for (UserInfo userInfo : list) {
				if (!userID.equals(userInfo.getParentUserID())) {
					ps.setLong(1, userID);
					ps.setLong(2, userInfo.getParentUserID());
					ps.setInt(3, userInfo.getLevel() + 1);
					ps.addBatch();
				}
			}

			ps.executeBatch();
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		} finally {
			this.mysqlClient.close(ps);
		}
		return true;
	}

	/**
	 * 查询出用户所有的下级
	 * 
	 * @param con
	 * @param afterUserID
	 * @return
	 */
	public List<UserInfo> queryALLApprenticeByUser(DruidPooledConnection con, Long afterUserID) {
		PreparedStatement ps = null;
		ResultSet rs = null;
		String sql = "select user_id,parent_user_id,level from qz_user_parent where parent_user_id=? ";
		List<UserInfo> list = new ArrayList<UserInfo>();
		try {
			ps = con.prepareStatement(sql);
			ps.setLong(1, afterUserID);
			rs = ps.executeQuery();
			// int newLevel = 0;
			while (rs.next()) {
				UserInfo userInfo = new UserInfo();
				userInfo.setParentUserID(rs.getLong("parent_user_id"));
				userInfo.setUserID(rs.getLong("user_id"));
				userInfo.setLevel(rs.getInt("level"));
				list.add(userInfo);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		} finally {
			this.mysqlClient.close(ps, rs);
		}
		return list;
	}

	/**
	 * 删除用户的所有上级
	 * 
	 * @param con
	 * @param afterUserID
	 * @return
	 */
	public boolean deleteParentByUser(DruidPooledConnection con, Long afterUserID) {
		PreparedStatement ps = null;
		String sql = "delete from qz_user_parent where user_id=?";
		try {
			ps = con.prepareStatement(sql);
			ps.setLong(1, afterUserID);
			ps.execute();
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		} finally {
			this.mysqlClient.close(ps);
		}
		return true;
	}

	/**
	 * 删除用户的所有上级
	 * 
	 * @param con
	 * @param afterUserID
	 * @return
	 */
	public boolean deleteParentByUser(DruidPooledConnection con, List<UserInfo> apprenticeUserInfoList,
			List<UserInfo> parentInfoList) {
		PreparedStatement ps = null;
		String sql = "delete from qz_user_parent where user_id=? and parent_user_id=?";
		try {
			ps = con.prepareStatement(sql);
			for (UserInfo userInfo : apprenticeUserInfoList) {
				for (UserInfo parentInfo : parentInfoList) {
					ps.setLong(1, userInfo.getUserID());
					ps.setLong(2, parentInfo.getParentUserID());
					ps.addBatch();
				}
			}
			ps.executeBatch();
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		} finally {
			this.mysqlClient.close(ps);
		}
		return true;
	}

	/**
	 * 保存单个用户的信息
	 * 
	 * @param con
	 * @param userInfo
	 * @return
	 */
	public boolean insert(DruidPooledConnection con, UserInfo userInfo) {
		PreparedStatement ps = null;
		String sql = "insert ignore into qz_user_parent set user_id=?,parent_user_id=?,level=?";
		try {
			if (!userInfo.getUserID().equals(userInfo.getParentUserID()) || userInfo.getUserID() == -99) {
				ps = con.prepareStatement(sql);
				ps.setLong(1, userInfo.getUserID());
				ps.setLong(2, userInfo.getParentUserID());
				ps.setInt(3, userInfo.getLevel());
				ps.execute();
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		} finally {
			this.mysqlClient.close(ps);
		}
		return true;
	}

	/**
	 * 把用户的parent_user_id由-99更新为parentUserID
	 * 
	 * @param con
	 * @param userID
	 * @param parentUserID
	 * @return
	 */
	public boolean updateParentUserID(DruidPooledConnection con, Long userID, Long parentUserID) {
		PreparedStatement ps = null;
		String sql = "update qz_user_parent set parent_user_id=?,level=? where user_ID=? and parent_user_id=-99";
		try {
			ps = con.prepareStatement(sql);
			ps.setLong(1, parentUserID);
			ps.setInt(2, 1);
			ps.setLong(3, userID);
			ps.execute();
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		} finally {
			this.mysqlClient.close(ps);
		}
		return true;
	}

	/**
	 * 把parentUserInfoList设置为userInfoList的上级
	 * 
	 * @param con
	 * @param userInfo
	 * @return
	 */
	public boolean insert(DruidPooledConnection con, List<UserInfo> userInfoList, List<UserInfo> parentUserInfoList) {
		PreparedStatement ps = null;
		String sql = "insert ignore into qz_user_parent set user_id=?,parent_user_id=?,level=?";
		try {
			ps = con.prepareStatement(sql);
			for (UserInfo userInfo : userInfoList) {
				for (UserInfo parentUserInfo : parentUserInfoList) {
					if (!userInfo.getUserID().equals(parentUserInfo.getParentUserID())) {
						ps.setLong(1, userInfo.getUserID());
						ps.setLong(2, parentUserInfo.getParentUserID());
						ps.setInt(3, userInfo.getLevel() + parentUserInfo.getLevel() + 1);
						ps.addBatch();
					}
				}
			}
			ps.executeBatch();
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		} finally {
			this.mysqlClient.close(ps);
		}
		return true;
	}

	/**
	 * 将userInfo设置为apprenticeUserInfoList的上级
	 * 
	 * @param con
	 * @param userInfo
	 *            用户的上级信息
	 * @param apprenticeUserInfoList
	 *            用户信息
	 * @return
	 */
	private boolean insert(DruidPooledConnection con, UserInfo userInfo, List<UserInfo> apprenticeUserInfoList) {
		// TODO Auto-generated method stub
		PreparedStatement ps = null;
		String sql = "insert ignore into qz_user_parent set user_id=?,parent_user_id=?,level=?";

		try {
			ps = con.prepareStatement(sql);
			for (UserInfo apprenticeUserInfo : apprenticeUserInfoList) {
				if (!apprenticeUserInfo.getUserID().equals(userInfo.getParentUserID())) {
					ps.setLong(1, apprenticeUserInfo.getUserID());
					ps.setLong(2, userInfo.getParentUserID());
					ps.setInt(3, apprenticeUserInfo.getLevel() + userInfo.getLevel());
					ps.addBatch();
				}
			}
			ps.executeBatch();
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		} finally {
			this.mysqlClient.close(ps);
		}

		return true;
	}

	/**
	 * parentUserInfoList设置为afterUserID的上级
	 * 
	 * @param con
	 * @param afterUserID
	 *            用户ID
	 * @param parentUserInfoList
	 *            afterUserID的上级
	 * @return
	 */
	private boolean insert(DruidPooledConnection con, Long afterUserID, List<UserInfo> parentUserInfoList) {
		// TODO Auto-generated method stub
		PreparedStatement ps = null;
		String sql = "insert ignore into qz_user_parent set user_id=?,parent_user_id=?,level=?";
		try {
			ps = con.prepareStatement(sql);
			for (UserInfo parentUserInfo : parentUserInfoList) {
				if (!afterUserID.equals(parentUserInfo.getParentUserID())) {
					ps.setLong(1, afterUserID);
					ps.setLong(2, parentUserInfo.getParentUserID());
					ps.setInt(3, parentUserInfo.getLevel() + 1);
					ps.addBatch();
				}
			}
			ps.executeBatch();
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		} finally {
			this.mysqlClient.close(ps);
		}

		return true;

	}

	/**
	 * 删除用户的所有上下级
	 * 
	 * @param con
	 * @param userID
	 * @return
	 */
	public boolean delete(DruidPooledConnection con, Long userID) {
		PreparedStatement ps = null;
		String delUserSql = "delete from qz_user_parent where user_id=?"; // 删除用户所有的上级
		String delApprenticeSql = "delete from qz_user_parent where parent_user_id=?"; // 删除用户所有的下级
		try {
			ps = con.prepareStatement(delUserSql);
			ps.setLong(1, userID);
			ps.execute();
			this.mysqlClient.close(ps);

			ps = con.prepareStatement(delApprenticeSql);
			ps.setLong(1, userID);
			ps.execute();

		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		} finally {
			this.mysqlClient.close(ps);
		}
		return true;
	}

	public boolean delete(DruidPooledConnection con, List<UserInfo> userInfoList) {
		PreparedStatement userPS = null;
		PreparedStatement parentPS = null;
		String delUserSql = "delete from qz_user_parent where user_id=?"; // 删除用户所有的上级
		String delApprenticeSql = "delete from qz_user_parent where parent_user_id=?"; // 删除用户所有的下级

		try {
			userPS = con.prepareStatement(delUserSql);
			parentPS = con.prepareStatement(delApprenticeSql);
			for (UserInfo userInfo : userInfoList) {
				userPS.setLong(1, userInfo.getUserID());
				parentPS.setLong(1, userInfo.getUserID());
				userPS.addBatch();
				parentPS.addBatch();
			}
			userPS.executeBatch();
			parentPS.executeBatch();
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		} finally {
			this.mysqlClient.close(userPS);
			this.mysqlClient.close(parentPS);
		}
		return true;
	}

	public boolean delete(DruidPooledConnection con, List<UserInfo> parentInfoList, List<UserInfo> apprenticeInfoList) {
		PreparedStatement ps = null;
		try {
			ps = con.prepareStatement("delete from qz_user_parent where user_id=? and parent_user_id=?");
			for (UserInfo apprenticeInfo : apprenticeInfoList) {
				for (UserInfo parentInfo : parentInfoList) {
					ps.setLong(1, apprenticeInfo.getUserID());
					ps.setLong(2, parentInfo.getParentUserID());
					ps.addBatch();
				}
			}
			ps.executeBatch();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		} finally {
			this.mysqlClient.close(ps);
		}

		return true;
	}

	/**
	 * 一级徒弟变为顶级师傅
	 * 
	 * @param con
	 * @param parentUserID
	 * @return
	 */
	public boolean updateUserLevel(DruidPooledConnection con, Long parentUserID) {
		PreparedStatement ps = null;
		try {
			/*
			 * 一级徒弟变为顶级师傅
			 */
			ps = con.prepareStatement(
					"update qz_user_parent set level=0,parent_user_id=-99 where parent_user_id=? and level=1");
			ps.setLong(1, parentUserID);
			ps.execute();
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		} finally {
			this.mysqlClient.close(ps);
		}
		return true;
	}

	/**
	 * 通过原表递归查询出userid的顶级userid
	 * 
	 * @param con
	 * @param userID
	 * @return
	 */
	public Long queryTopParentUserIDFromDataSource(DruidPooledConnection con, Long userID) {
		PreparedStatement ps = null;
		ResultSet rs = null;
		String sql = "select user_id,parent_user_id from hhz_user where user_id=?";

		Long parentUserID = 0L;
		try {
			ps = con.prepareStatement(sql);
			ps.setLong(1, userID);
			rs = ps.executeQuery();
			while (rs.next()) {
				parentUserID = rs.getLong("parent_user_id");
				if (parentUserID == -99) {
					logger.debug("userID=>" + userID + " parentUserID=>" + parentUserID);
					return rs.getLong("user_id");
				}
				logger.debug("userID=>" + userID + " parentUserID=>" + parentUserID);
			}

			// 如果用户不存在
			if (parentUserID == 0) {
				logger.warn("not found this user: " + userID);
				return parentUserID;
			}

		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		} finally {
			this.mysqlClient.close(ps, rs);
		}
		return queryTopParentUserIDFromDataSource(con, parentUserID);

	}

	/**
	 * 通过原表递归查询出userID的所有徒弟
	 * 
	 * @param con
	 * @param userID
	 * @param map
	 * @return
	 */
	public boolean queryALLApprenticeFromDataSourceByUser(DruidPooledConnection con, Long userID,
			List<UserInfo> userInfoList) {
		PreparedStatement ps = null;
		ResultSet rs = null;
		String sql = "select user_id,parent_user_id from hhz_user where parent_user_id=? ";
		try {
			ps = con.prepareStatement(sql);
			ps.setLong(1, userID);
			rs = ps.executeQuery();
			while (rs.next()) {
				Long newUserID = rs.getLong("user_id");
				Long newParentUserID = rs.getLong("parent_user_id");
				// map.put(rs.getLong("user_id"), rs.getLong("parent_user_id"));
				UserInfo userInfo = new UserInfo();
				userInfo.setUserID(newUserID);
				userInfo.setParentUserID(newParentUserID);
				userInfoList.add(userInfo);

				logger.debug("userID=>" + newUserID + " parentUserID=>" + newParentUserID);

				boolean result = queryALLApprenticeFromDataSourceByUser(con, newUserID, userInfoList);
				if (!result) {
					return false;
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		} finally {
			this.mysqlClient.close(ps, rs);
		}
		return true;
	}

	public static void main(String args[]) {
		Long a = 123L;
		Long b = 123L;
		System.out.println(a == b);
		System.out.println(a.equals(b));

		MasterApprenticeConfig config = new MasterApprenticeConfig();
		MySQLClient m = new MySQLClient(config);
		MasterApprenticeSystemHandle db = new MasterApprenticeSystemHandle(m, config);
		DruidPooledConnection con = null;
		try {
			con = m.getDataSource().getConnection();
			Long topParentUserID = db.queryTopParentUserIDFromDataSource(con, 379559l);
			System.out.println(topParentUserID);
			System.out.println();
			System.out.println();

			List<UserInfo> userInfoList = new ArrayList<UserInfo>();
			userInfoList.add(new UserInfo(-99l, topParentUserID));

			db.queryALLApprenticeFromDataSourceByUser(con, topParentUserID, userInfoList);

			System.out.println();
			System.out.println();
			for (UserInfo u : userInfoList) {
				logger.debug("userID=>" + u.getUserID() + " parentUserID=>" + u.getParentUserID());
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			m.close(con);
		}

	}
}
