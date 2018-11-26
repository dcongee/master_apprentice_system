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

public class DBHandler2 {
	private static Logger logger = Logger.getLogger(DBHandler2.class);
	private MasterApprenticeConfig config;
	private MySQLClient mysqlClient;
	private Channel produceChannel;

	public DBHandler2(Channel produceChannel, MySQLClient mysqlClient, MasterApprenticeConfig config) {
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
		Long parentUserID = (Long) map.get("PARENT_USER_ID");
		Long userID = (Long) map.get("USER_ID");
		// String userStatus = (String)map.get("STATUS");
		if (!this.insertParentAndApprentice(userID, parentUserID))
			logger.error("new register user's master and apprentice save failed, userID: " + userID + ". parentUserID: "
					+ parentUserID);
		;
	}

	private void handlerUpdate(Map<String, Object> map) {
		// TODO Auto-generated method stub
		Map<String, Object> beforMap = (Map<String, Object>) map.get(config.getUpdateBeforName());
		Map<String, Object> afterMap = (Map<String, Object>) map.get(config.getUpdateAfterName());

		Long beforParentUserID = (Long) beforMap.get("PARENT_USER_ID");
		Long afterParentUserID = (Long) afterMap.get("PARENT_USER_ID");

		// String afterUserStatus = afterMap.get("STATUS");

		Long beforUserID = (Long) beforMap.get("USER_ID");
		Long afterUserID = (Long) afterMap.get("USER_ID");
		DruidPooledConnection con = null;
		if (beforParentUserID == afterParentUserID) {
			// parentUserID更新前后没变化，表示上下级关系无变化,或者beforParentUserID与afterParentUserID都为-99
			return;
		} else if (beforUserID != afterUserID) {
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

			try {
				con = this.mysqlClient.getDataSource().getConnection();
				if (this.releaseMasterApprentice(con, afterUserID)) {
					con.commit();
				} else {
					con.rollback();
				}
			} catch (SQLException e) {
				e.printStackTrace();
				logger.error("release user failed.");
				try {
					con.rollback();
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
			} finally {
				this.mysqlClient.close(con);
			}

		} else if (beforParentUserID == -99 && afterParentUserID != -99) {
			// parentUserID由-99变为非-99，绑定新的上级。
			// updateMasterAndApprentice(afterUserID, afterParentUserID);

		} else {
			// 更新上下级关系，且新的上级不为-99
			// handleUpdateMasterAndApprentice(afterUserID, beforParentUserID,
			// afterParentUserID);
		}
	}

	boolean updateMasterAndApprentice(Long afterUserID, Long afterParentUserID) {
		// 查询用户所有上级的信息
		// 更新用户的上级信息

		// 查询出用户所有的下级
		// 更新下级的上级信息
		DruidPooledConnection con = null;
		try {
			con = this.mysqlClient.getDataSource().getConnection();
			con.setAutoCommit(false);
			List<UserInfo> parentList = this.queryALLParentByApprentice(con, afterUserID, afterParentUserID);
			if (parentList.size() > 0 && parentList != null && this.insert(con, parentList)) {
				con.commit();
			} else {
				con.rollback();
				return false;
			}

			List<UserInfo> apprenticeList = this.queryALLApprenticeByUser(con, afterUserID);
			if (apprenticeList == null) {
				return false;
			}
			if (apprenticeList.size() > 0) {

			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return true;
	}

	public void handlerDelete(Map<String, Object> map) {
		// TODO Auto-generated method stub
		Long parentUserID = (Long) map.get("PARENT_USER_ID");
		Long userID = (Long) map.get("USER_ID");
		logger.error("userID has deleted, userID: " + userID + ". parentUserID: " + parentUserID);
		return;
	}

	public boolean insertParentAndApprentice(Long userID, Long parentUserID) {
		DruidPooledConnection con = null;
		try {
			con = this.mysqlClient.getDataSource().getConnection();
			con.setAutoCommit(false);
			if (parentUserID == -99) {
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
				List<UserInfo> listUserInfo = this.queryALLParentByApprentice(con, userID, parentUserID);
				if (listUserInfo.size() > 0 && listUserInfo != null && this.insert(con, listUserInfo)) {
					con.commit();
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
	 * 查询用户所有的上级信息。
	 * 
	 * @param con
	 * @param apprenticeUserID
	 * @param parentUserID
	 * @return
	 */
	public List<UserInfo> queryALLParentByApprentice(DruidPooledConnection con, Long userID, Long parentUserID) {
		PreparedStatement ps = null;
		ResultSet rs = null;
		String sql = "select parent_user_id,level from qz_user_parent where user_id=? ";
		List<UserInfo> list = new ArrayList<UserInfo>();
		try {
			ps = con.prepareStatement(sql);
			ps.setLong(1, parentUserID);
			rs = ps.executeQuery();
			while (rs.next()) {
				Long newParentUserID = rs.getLong("parent_user_id");
				int level = rs.getInt("level") + 1;
				UserInfo userInfo = new UserInfo(newParentUserID, userID, level);
				list.add(userInfo);
			}
			int newLevel = list.size() + 1;
			list.add(new UserInfo(parentUserID, userID, newLevel));
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		} finally {
			this.mysqlClient.close(ps, rs);
		}
		return list;
	}

	public boolean releaseMasterApprentice(DruidPooledConnection con, Long afterUserID) {
		// 删除用户的所有上级信息.
		// 新增parent_user_id为-99的信息
		// 删除用户下级的上级
		// 更新下级的LEVEL.
		

		if (this.deleteParentByUser(con, afterUserID)) {
			return false;
		}

		UserInfo userInfo = new UserInfo();
		userInfo.setUserID(afterUserID);
		userInfo.setParentUserID(-99L);
		userInfo.setLevel(0);

		if (!this.insert(con, userInfo))
			return false;

		List<UserInfo> userInfoList = this.queryALLApprenticeByUser(con, afterUserID);
		if (userInfoList == null) {
			return false;
		}

		if (userInfoList.size() > 0) {
			if (!this.deleteParentByUser(con, userInfoList)) {
				return false;
			}
			if (!this.insert(con, userInfoList)) {
				return false;
			}
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
		String sql = "select user_id,parent_user_id,level from qz_user_parent where parent_user_id=? order by level asc";
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
	public boolean deleteParentByUser(DruidPooledConnection con, List<UserInfo> userInfoList) {
		PreparedStatement ps = null;
		String sql = "delete from qz_user_parent where user_id=?";
		try {
			ps = con.prepareStatement(sql);
			for (UserInfo userInfo : userInfoList) {
				ps.setLong(1, userInfo.getUserID());
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
					ps.setLong(2, parentInfo.getUserID());
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

	public UserInfo queryParentByUser(DruidPooledConnection con, Long afterUserID) {
		PreparedStatement ps = null;
		ResultSet rs = null;
		UserInfo userInfo = null;
		String sql = "select parent_user_id,apprentice_user_id,level from qz_parent_apprentice where apprentice_user_id=? and level=1";
		try {
			ps = con.prepareStatement(sql);
			ps.setLong(1, afterUserID);
			rs = ps.executeQuery();
			userInfo = new UserInfo(rs.getLong("parent_user_id"), rs.getLong("apprentice_user_id"), rs.getInt("level"));
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		} finally {
			this.mysqlClient.close(ps);
		}
		return userInfo;
	}

	/**
	 * 查询出用户所有的下级
	 * 
	 * @param con
	 * @param userID
	 * @return
	 */
	public List<UserInfo> queryALLApprenticeByParent(DruidPooledConnection con, Long userID) {
		PreparedStatement ps = null;
		ResultSet rs = null;
		String sql = "select apprentice_user_id,level from qz_user_parent where parent_user_id=? order by level asc";
		List<UserInfo> list = new ArrayList<UserInfo>();
		try {
			ps = con.prepareStatement(sql);
			ps.setLong(1, userID);
			rs = ps.executeQuery();
			while (rs.next()) {
				Long apprenticeUserID = rs.getLong("apprentice_user_id");
				UserInfo userInfo = new UserInfo();
				userInfo.setUserID(apprenticeUserID);
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

	public void deleteALLPrentByApprentice(DruidPooledConnection con, String parentUserID) {

	}

	public void deleteALLApprenticeByParent(DruidPooledConnection con, String apprenticeUserID) {

	}

	/**
	 * 保存一批用户的信息
	 * 
	 * @param con
	 * @param list
	 * @return
	 */
	public boolean insert(DruidPooledConnection con, List<UserInfo> list) {
		PreparedStatement ps = null;
		String sql = "insert into qz_user_parent set user_id=?,parent_user_id=?,level=?";
		try {
			ps = con.prepareStatement(sql);
			for (UserInfo userInfo : list) {
				ps.setLong(1, userInfo.getUserID());
				ps.setLong(2, userInfo.getParentUserID());
				ps.setInt(3, userInfo.getLevel());
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

	public boolean insert(DruidPooledConnection con, List<UserInfo> parentList, List<UserInfo> apprenticeList) {
		PreparedStatement ps = null;
		String sql = "insert into qz_user_parent set user_id=?,parent_user_id=?,level=?";
		try {
			ps = con.prepareStatement(sql);
			for (UserInfo apprenticeInfo : apprenticeList) {

			}
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
