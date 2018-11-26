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

import com.qktx.master.apprentice.config.MasterApprenticeConfig;
import com.qktx.master.apprentice.mysql.MySQLClient;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerExpectContinueHandler;
import redis.clients.jedis.JedisPool;

public class MasterApprenticeSystemHttpServerInitializer extends ChannelInitializer<SocketChannel> {

	private JedisPool jedisPool;
	private MasterApprenticeConfig config;
	private MySQLClient mysqlClient;

	public MasterApprenticeSystemHttpServerInitializer(MasterApprenticeConfig config, JedisPool jedisPool,MySQLClient mysqlClient) {
		this.config = config;
		this.jedisPool = jedisPool;
		this.mysqlClient=mysqlClient;

	}

	@Override
	public void initChannel(SocketChannel ch) {
		ChannelPipeline p = ch.pipeline();
		p.addLast(new HttpServerCodec());
		p.addLast(new HttpServerExpectContinueHandler());
		p.addLast(new MasterApprenticeSystemHttpServerHandler(config, jedisPool,mysqlClient));
	}
}
