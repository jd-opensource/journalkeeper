package com.jd.journalkeeper.rpc.remoting.transport;

import com.jd.journalkeeper.rpc.remoting.transport.config.ClientConfig;

/**
 * 客户端通信工厂
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2018/8/24
 */
public interface TransportClientFactory {

    TransportClient create(ClientConfig config);
}