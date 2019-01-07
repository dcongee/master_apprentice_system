package com.qktx.master.apprentice.mysql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.qktx.master.apprentice.config.MasterApprenticeConfig;

public class MySQLClient {
	private static Logger logger = Logger.getLogger(MySQLClient.class);

	private MasterApprenticeConfig config;
	private DruidDataSource dataSource;

	public MySQLClient(MasterApprenticeConfig config) {
		this.config = config;
		initMySQLConnectionPool();
	}

	void initMySQLConnectionPool() {
		logger.info("init mysql jdbc connection pool.");
		dataSource = new DruidDataSource();
		dataSource.setDriverClassName(config.getMysqlDriverClassName());
		dataSource.setUsername(config.getMysqlUser());
		dataSource.setPassword(config.getMysqlPasswd());
		dataSource.setUrl(config.getMysqlURL());
		dataSource.setInitialSize(config.getMysqlPoolInitialSize());
		dataSource.setMinIdle(config.getMysqlPoolMinIdle());
		dataSource.setMaxActive(config.getMysqlPoolMaxActive()); // 启用监控统计功能 dataSource.setFilters("stat");// for mysql
		// dataSource.setPoolPreparedStatements(false);
		dataSource.setRemoveAbandoned(config.getMysqlPoolRemoveAbandoned());
		dataSource.setRemoveAbandonedTimeout(config.getMysqlPoolRemoveAbandonedTimeout());
		// dataSource.setRemoveAbandonedTimeoutMillis(50 * 1000);
		// dataSource.setLogAbandoned(true);
		dataSource.setTestWhileIdle(config.getMysqlPoolTestWhileIdle());
		dataSource.setMinEvictableIdleTimeMillis(config.getMysqlPoolMinEvictableIdleTimeMillis());

		// dataSource.setTransactionQueryTimeout(130);
		dataSource.setValidationQuery(config.getMysqlPoolValidationQuery());
		dataSource.setTimeBetweenEvictionRunsMillis(config.getMysqlPoolTimeBetweenEvictionRunsMillis());
		DruidPooledConnection con = null;
		PreparedStatement ps = null;
		try {
			con = this.dataSource.getConnection();
			ps = con.prepareStatement("select now()");
			ps.executeQuery();
			con.rollback();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			this.close(con, ps);
		}
	}

	public void close(Connection con, PreparedStatement ps, ResultSet rs) {
		if (con != null) {
			try {
				con.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if (ps != null) {
			try {
				ps.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if (rs != null) {
			try {
				rs.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void close(PreparedStatement ps, ResultSet rs) {

		if (ps != null) {
			try {
				ps.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if (rs != null) {
			try {
				rs.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void close(Connection con, PreparedStatement ps) {
		if (con != null) {
			try {
				con.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if (ps != null) {
			try {
				ps.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void close(ResultSet rs) {

		if (rs != null) {
			try {
				rs.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void close(PreparedStatement ps) {

		if (ps != null) {
			try {
				ps.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	public void close(Connection con) {
		if (con != null) {
			try {
				con.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	public MasterApprenticeConfig getConfig() {
		return config;
	}

	public DruidDataSource getDataSource() {
		return dataSource;
	}

	public static void main(String args[]) {
		// System.out.println(new Timestamp(new Date().getTime()).toString());
		MasterApprenticeConfig config = new MasterApprenticeConfig();
		MySQLClient mysqlClient = new MySQLClient(config);
		DruidPooledConnection con = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			con = mysqlClient.getDataSource().getConnection();
			con.setAutoCommit(false);
			ps = con.prepareStatement("select connection_id()");
			rs = ps.executeQuery();
			while (rs.next()) {
				System.out.println("current mysql connection id: " + rs.getInt(1));
			}
			rs.close();
			ps.close();
			ps = con.prepareStatement("select id,sleep(235) from wuhp.w limit 1");
			ps.executeQuery();
			Thread.sleep(240 * 1000);
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			mysqlClient.close(con, ps);
		}
	}
}
