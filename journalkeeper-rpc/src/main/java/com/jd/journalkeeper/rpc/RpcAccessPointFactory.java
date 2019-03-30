package com.jd.journalkeeper.rpc;

import com.jd.journalkeeper.rpc.client.ClientServerRpcAccessPoint;
import com.jd.journalkeeper.rpc.server.ServerRpcAccessPoint;

import java.net.URI;
import java.util.List;
import java.util.Properties;

/**
 * RPC实现的入口工厂类
 * @author liyue25
 * Date: 2019-03-26
 */
public interface RpcAccessPointFactory {
    /**
     * 创建一个ServerRpc接入点
     */
    ServerRpcAccessPoint createServerRpcAccessPoint(Properties properties);

    /**
     * 创建一个ClientServerRpc接入点
     * @param servers 可连接的服务器列表
     */
    ClientServerRpcAccessPoint createClientServerRpcAccessPoint(List<URI> servers, Properties properties);
}
