/*
 * Copyright 2013 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.qktx.master.apprentice.http;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSON;
import com.qktx.master.apprentice.config.MasterApprenticeConfig;
import com.qktx.master.apprentice.mysql.MySQLClient;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.util.AsciiString;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class MasterApprenticeSystemHttpServerHandler extends SimpleChannelInboundHandler<HttpObject> {
	// private static final byte[] CONTENT = { 'H', 'e', 'l', 'l', 'o', ' ', 'W',
	// 'o', 'r', 'l', 'd' };

	private static final AsciiString CONTENT_TYPE = AsciiString.cached("Content-Type");
	private static final AsciiString CONTENT_LENGTH = AsciiString.cached("Content-Length");
	private static final AsciiString CONNECTION = AsciiString.cached("Connection");
	private static final AsciiString KEEP_ALIVE = AsciiString.cached("keep-alive");

	private static Logger logger = Logger.getLogger(MasterApprenticeSystemHttpServerHandler.class);

	private JedisPool jedisPool;
	private MySQLClient mysqlClient;
	private MasterApprenticeConfig config;

	public MasterApprenticeSystemHttpServerHandler() {

	}

	public MasterApprenticeSystemHttpServerHandler(MasterApprenticeConfig config, JedisPool jedisPool,
			MySQLClient mysqlClient) {
		this.config = config;
		this.jedisPool = jedisPool;
		this.mysqlClient = mysqlClient;

	}

	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) {
		ctx.flush();
	}

	@Override
	public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
		if (msg instanceof HttpRequest) {
			HttpRequest req = (HttpRequest) msg;

			boolean keepAlive = HttpUtil.isKeepAlive(req);
			// FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK,
			// Unpooled.wrappedBuffer(CONTENT));
			Long userID = null;
			QueryStringDecoder decoder = new QueryStringDecoder(req.getUri());
			// System.out.println(decoder.path() + "%%%%%%%%");
			Map<String, List<String>> parametersMap = decoder.parameters();
			FullHttpResponse response = null;
			if (decoder.path().equals("/userinfo") && parametersMap.containsKey("userid")) {
				userID = Long.valueOf(parametersMap.get("userid").get(0));
				// Map<String, Object> userInfoMap = new HashMap<String, Object>();
				// userInfoMap.put("user_id", userID);
				// userInfoMap.put("apprentice_ids", this.getAllApprenticeID(userID)); //
				// 用户的所有徒弟
				// userInfoMap.put("master_ids", this.getAllMasterID(userID)); // 用户的所有上级
				// String topMasterID = this.getTopMasterID(userID);
				// if (topMasterID == null) {
				// topMasterID = "";
				// } else {
				// userInfoMap.put("top_master_all_apprentice_ids",
				// this.getAllApprenticeID(topMasterID)); // 顶级用户的所有徒弟
				// }
				// userInfoMap.put("top_master_id", topMasterID); // 顶级用户ID

				Map<String, Object> map = new HashMap<String, Object>();
				DruidPooledConnection con = null;
				try {
					con = this.mysqlClient.getDataSource().getConnection();
					con.setAutoCommit(false);
					Map<String, Long> parentUserMap = this.queryUserParent(con, userID);
					Map<String, List<Long>> apprenticeMap = this.queryUserApprentice(con, userID);

					long topParentID = -1;
					Map<String, List<Long>> topParentApprenticeMap = null;
					if (parentUserMap != null && parentUserMap.size() > 0) {
						topParentID = parentUserMap.get("top_parent_id");
						parentUserMap.remove("top_parent_id");
						if (topParentID != -99) {
							topParentApprenticeMap = this.queryUserApprentice(con, topParentID);
						}
					}

					con.commit();
					map.put("user_id", userID);
					map.put("top_parent_id", topParentID);
					map.put("parent_ids", parentUserMap != null ? parentUserMap : "");
					map.put("apprentice_ids", apprenticeMap != null ? apprenticeMap : "");
					map.put("top_parent_all_apprentice_ids",
							topParentApprenticeMap != null ? topParentApprenticeMap : new HashMap<String, Object>());
				} catch (SQLException e) {
					e.printStackTrace();
				} finally {
					this.mysqlClient.close(con);
				}

				response = new DefaultFullHttpResponse(HTTP_1_1, OK,
						Unpooled.wrappedBuffer(JSON.toJSONString(map).getBytes()));
			} else {
				response = new DefaultFullHttpResponse(HTTP_1_1, OK,
						Unpooled.wrappedBuffer("unkhown url or parameters".getBytes()));
			}

			response.headers().set(CONTENT_TYPE, "text/plain");
			response.headers().setInt(CONTENT_LENGTH, response.content().readableBytes());

			if (!keepAlive) {
				ctx.write(response).addListener(ChannelFutureListener.CLOSE);
			} else {
				response.headers().set(CONNECTION, KEEP_ALIVE);
				ctx.write(response);
			}
		}
	}

	public Map<String, Long> queryUserParent(DruidPooledConnection con, Long userID) {
		PreparedStatement ps = null;
		ResultSet rs = null;
		String sql = "select user_id,parent_user_id,level from qz_user_parent force index(idx_user_level) where  user_Id=? order by level asc";
		Map<String, Long> parentUserMap = new HashMap<String, Long>();
		Long parent_user_id = null;
		try {
			ps = con.prepareStatement(sql);
			ps.setLong(1, userID);
			rs = ps.executeQuery();
			while (rs.next()) {
				int level = rs.getInt("level");
				// Long user_id = rs.getLong("user_id");
				parent_user_id = rs.getLong("parent_user_id");
				parentUserMap.put("level_" + level, parent_user_id);
			}
			if (parent_user_id != null) {
				parentUserMap.put("top_parent_id", parent_user_id);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		} finally {
			this.mysqlClient.close(ps, rs);
		}
		return parentUserMap;
	}

	public Long queryUserTopParentUserID(DruidPooledConnection con, Long userID) {
		PreparedStatement ps = null;
		ResultSet rs = null;
		String sql = "select user_id,parent_user_id,level from qz_user_parent force index(idx_user_level) where  user_Id=? order by level desc limit 1";
		Long parent_user_id = null;
		try {
			ps = con.prepareStatement(sql);
			ps.setLong(1, userID);
			rs = ps.executeQuery();
			while (rs.next()) {
				parent_user_id = rs.getLong("parent_user_id");
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		} finally {
			this.mysqlClient.close(ps, rs);
		}
		return parent_user_id;
	}

	public Map<String, List<Long>> queryUserApprentice(DruidPooledConnection con, Long userID) {
		PreparedStatement ps = null;
		ResultSet rs = null;
		String sql = " select parent_user_id,level,user_id from qz_user_parent where parent_user_id=? order by  level asc";
		int intLevel = -1;
		Map<String, List<Long>> map = new HashMap<String, List<Long>>();
		List<Long> apprenticeList = null;

		try {
			ps = con.prepareStatement(sql);
			ps.setLong(1, userID);
			rs = ps.executeQuery();
			while (rs.next()) {
				int level = rs.getInt("level");
				if (intLevel == -1 || intLevel != level) {
					intLevel = level;
					apprenticeList = new ArrayList<Long>();
					map.put("level_" + level, apprenticeList);
				}
				Long user_id = rs.getLong("user_id");
				apprenticeList.add(user_id);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		} finally {
			this.mysqlClient.close(ps, rs);
		}
		return map;
	}

	public String getTopMasterID(String userID) {
		Jedis jedis = this.jedisPool.getResource();
		String topMasterID = null;
		try {
			topMasterID = jedis.get(config.getTopMasterUserIDSKeyPrefix() + userID);
		} catch (Exception e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		} finally {
			this.jedisPool.returnResource(jedis);
		}
		return topMasterID;
		// return topMasterID == null ? "" : topMasterID;
	}

	public List<String> getAllApprenticeID(String userID) {
		Jedis jedis = this.jedisPool.getResource();
		List<String> list = new ArrayList<String>();
		try {
			Map<String, String> map = jedis.hgetAll(this.config.getApprenticeUserIDSKeyPrefix() + userID);
			for (Map.Entry<String, String> entry : map.entrySet()) {
				list.add(entry.getKey());
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		} finally {
			this.jedisPool.returnResource(jedis);
		}
		return list;
	}

	public List<String> getAllMasterID(String userID) {
		Jedis jedis = this.jedisPool.getResource();
		List<String> list = new ArrayList<String>();
		try {
			Map<String, String> map = jedis.hgetAll(this.config.getMasterUserIDSKeyPrefix() + userID);
			for (Map.Entry<String, String> entry : map.entrySet()) {
				list.add(entry.getKey());
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		} finally {
			this.jedisPool.returnResource(jedis);
		}
		return list;
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		cause.printStackTrace();
		ctx.close();
	}

	public static void main(String[] args) {
		Map<String, List<String>> userInfoMap = new HashMap<String, List<String>>();
		List<String> master = new ArrayList<String>();
		List<String> apprentice = new ArrayList<String>();
		userInfoMap.put("master_ids", master);
		userInfoMap.put("apprentice_ids", apprentice);
		master.add("1");
		master.add("2");
		apprentice.add("3");
		apprentice.add("4");
		System.out.println(JSON.toJSONString(userInfoMap));

	}
}
