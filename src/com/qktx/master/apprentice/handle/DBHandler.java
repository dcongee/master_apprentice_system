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
import com.qktx.master.appentice.pojo.UserInfo;
import com.qktx.master.apprentice.config.MasterApprenticeConfig;
import com.qktx.master.apprentice.mysql.MySQLClient;
import com.rabbitmq.client.Channel;

public class DBHandler {
	private static Logger logger = Logger.getLogger(DBHandler.class);
	private MasterApprenticeConfig config;
	private MySQLClient mysqlClient;
	private Channel produceChannel;

	public DBHandler(Channel produceChannel, MySQLClient mysqlClient, MasterApprenticeConfig config) {
		this.config = config;
		this.mysqlClient = mysqlClient;
		this.produceChannel = produceChannel;
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
		Long parentUserID = Long.valueOf((String) map.get("PARENT_USER_ID"));
		Long userID = Long.valueOf((String) map.get("USER_ID"));
		// String userStatus = (String)map.get("STATUS");
		if (!this.insertParentAndApprentice(userID, parentUserID))
			logger.error("new register user's master and apprentice save failed, userID: " + userID + ". parentUserID: "
					+ parentUserID);
	}

	private void handlerUpdate(Map<String, Object> map) {
		// TODO Auto-generated method stub
		Map<String, Object> beforMap = (Map<String, Object>) map.get(config.getUpdateBeforName());
		Map<String, Object> afterMap = (Map<String, Object>) map.get(config.getUpdateAfterName());

		Long beforParentUserID = Long.valueOf((String) beforMap.get("PARENT_USER_ID"));
		Long afterParentUserID = Long.valueOf((String) afterMap.get("PARENT_USER_ID"));

		// String afterUserStatus = afterMap.get("STATUS");

		Long beforUserID = Long.valueOf((String) beforMap.get("USER_ID"));
		Long afterUserID = Long.valueOf((String) afterMap.get("USER_ID"));
		DruidPooledConnection con = null;

		boolean OKflag = true;

		try {
			con = this.mysqlClient.getDataSource().getConnection();
			con.setAutoCommit(false);
			if (beforParentUserID.equals(afterParentUserID)) {
				// parentUserID更新前后没变化，表示上下级关系无变化,或者beforParentUserID与afterParentUserID都为-99
				return;
			} else if (!beforUserID.equals(afterUserID)) {
				// 用户的userid发生变化
				logger.error("userID has changed. befor update userID is:" + beforUserID + ". after update userID is: "
						+ afterUserID);
				return;
			} else if (beforParentUserID != -99 && afterParentUserID == -99) {
				// parentUserID由正常的用户ID更新为-99，表示用户解绑了原有的上下级关系。
				logger.warn(
						"parent user id has changed, befor update parent_user_Id is not -99, after update parent_user_id is -99,user_id is :"
								+ beforUserID + ", befor update parent_user_id is: " + beforParentUserID
								+ ", after update parent_user_id is: " + afterParentUserID
								+ ". release user master and apprentice");

				if (!this.releaseMasterApprentice(con, afterUserID)) {
					OKflag = false;
					logger.error("user remove parent failed, user_id is :" + afterUserID
							+ ", befor update parent_user_id is: " + beforParentUserID
							+ ", after update parent_user_id is: " + afterParentUserID);
				}
			} else if (beforParentUserID == -99 && afterParentUserID != -99) {
				// parentUserID由-99变为非-99，绑定新的上级。
				if (!updateMasterAndApprentice(con, afterUserID, afterParentUserID)) {
					OKflag = false;
					logger.error("user add new parent failed. user_id is :" + afterUserID
							+ ", befor update parent_user_id is: " + beforParentUserID
							+ ", after update parent_user_id is: " + afterParentUserID);
				}

			} else {
				// 更新上下级关系，且上级更新前后都不为-99
				if (!handleUpdateMasterAndApprentice(con, afterUserID, beforParentUserID, afterParentUserID)) {
					OKflag = false;
					logger.error("update update parent failed. user_id is :" + afterUserID
							+ ", befor update parent_user_id is: " + beforParentUserID
							+ ", after update parent_user_id is: " + afterParentUserID);
				}

			}

			if (!OKflag) {
				con.rollback();
			} else {
				con.commit();
			}
		} catch (SQLException e2) {
			e2.printStackTrace();
		} finally {
			this.mysqlClient.close(con);
		}
	}

	public void handlerDelete(Map<String, Object> map) {
		// TODO Auto-generated method stub
		Long parentUserID = Long.valueOf((String) map.get("PARENT_USER_ID"));
		Long userID = Long.valueOf((String) map.get("USER_ID"));
		logger.error("userID has deleted, userID: " + userID + ". parentUserID: " + parentUserID);
		return;
	}

	public boolean insertParentAndApprentice(Long userID, Long parentUserID) {
		DruidPooledConnection con = null;
		try {
			con = this.mysqlClient.getDataSource().getConnection();
			con.setAutoCommit(false);
			if (parentUserID == -99) {
				// parent_user_Id为-99的直接写入数据。
				UserInfo userInfo = new UserInfo();
				userInfo.setUserID(userID);
				userInfo.setParentUserID(parentUserID);
				userInfo.setLevel(0);
				if (this.insert(con, userInfo)) {
					con.commit();
				} else {
					con.rollback();
					return false;
				}
			} else {
				// 先查询出parent_user_id的所有上级
				// 将parent_user_id的所有上级设置为自己的上级
				List<UserInfo> parentUserInfoList = this.queryALLParentByUserID(con, parentUserID);
				if (parentUserInfoList == null) {
					return false;
				}

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
				if (this.insert(con, userID, parentUserID, parentUserInfoList)) {
					con.commit();
					return true;
				} else {
					con.rollback();
					return false;
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
			try {
				con.rollback();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			return false;
		} finally {
			this.mysqlClient.close(con);
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
		String sql = "insert into qz_user_parent set user_id=?,parent_user_id=?,level=?";
		try {
			ps = con.prepareStatement(sql);
			ps.setLong(1, userID);
			ps.setLong(2, parentUserID);
			ps.setInt(3, 1);
			ps.addBatch();

			for (UserInfo userInfo : list) {
				ps.setLong(1, userID);
				ps.setLong(2, userInfo.getParentUserID());
				ps.setInt(3, userInfo.getLevel() + 1);
				ps.addBatch();
			}

			ps.executeBatch();
		} catch (SQLException e) {
			logger.error("");
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
	public boolean deleteParentByUser(DruidPooledConnection con, List<UserInfo> userInfoList,
			List<UserInfo> parentInfoList) {
		PreparedStatement ps = null;
		String sql = "delete from qz_user_parent where user_id=? and parent_user_id=?";
		try {
			ps = con.prepareStatement(sql);
			for (UserInfo userInfo : userInfoList) {
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
		String sql = "insert into qz_user_parent set user_id=?,parent_user_id=?,level=?";
		try {
			ps = con.prepareStatement(sql);
			ps.setLong(1, userInfo.getUserID());
			ps.setLong(2, userInfo.getParentUserID());
			ps.setInt(3, userInfo.getLevel());
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
		String sql = "insert into qz_user_parent set user_id=?,parent_user_id=?,level=?";
		try {
			ps = con.prepareStatement(sql);
			for (UserInfo userInfo : userInfoList) {
				for (UserInfo parentUserInfo : parentUserInfoList) {
					ps.setLong(1, userInfo.getUserID());
					ps.setLong(2, parentUserInfo.getParentUserID());
					ps.setInt(3, userInfo.getLevel() + parentUserInfo.getLevel() + 1);
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
		String sql = "insert into qz_user_parent set user_id=?,parent_user_id=?,level=?";

		try {
			ps = con.prepareStatement(sql);
			for (UserInfo apprenticeUserInfo : apprenticeUserInfoList) {
				ps.setLong(1, apprenticeUserInfo.getUserID());
				ps.setLong(2, userInfo.getParentUserID());
				ps.setInt(3, apprenticeUserInfo.getLevel() + userInfo.getLevel());
				ps.addBatch();
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
		String sql = "insert into qz_user_parent set user_id=?,parent_user_id=?,level=?";
		try {
			ps = con.prepareStatement(sql);
			for (UserInfo parentUserInfo : parentUserInfoList) {
				ps.setLong(1, afterUserID);
				ps.setLong(2, parentUserInfo.getParentUserID());
				ps.setInt(3, parentUserInfo.getLevel() + 1);
				ps.addBatch();
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

	public static void main(String args[]) {
		Long a = 123L;
		Long b = 123L;
		System.out.println(a == b);
		System.out.println(a.equals(b));

	}
}
