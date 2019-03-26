package com.jd.journalkeeper.rpc.protocol;

import com.jd.journalkeeper.rpc.transport.config.ServerConfig;

/**
 * 协议服务
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2018/9/25
 */
public interface ProtocolServer extends Protocol {

    ServerConfig createServerConfig(ServerConfig serverConfig);
}