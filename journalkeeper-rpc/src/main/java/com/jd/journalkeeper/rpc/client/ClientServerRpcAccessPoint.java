package com.jd.journalkeeper.rpc.client;

import com.jd.journalkeeper.utils.state.StateServer;

import java.net.URI;
import java.util.List;

/**
 * ClientServerRpc接入点，管理Client和Server的rpc远程连接。
 * @author liyue25
 * Date: 2019-03-14
 */
public interface ClientServerRpcAccessPoint  {
    /**
     * 客户端使用
     * 更新可供连接的server列表
     */
    void updateServers(List<URI> servers);

    /**
     * 客户端使用
     * 获取一个ClientServerRpc实例，自动选择server。
     */
    ClientServerRpc getClintServerRpc();

    /**
     * 客户端使用
     * 指定URI获取一个ClientServerRpc实例，一般用于访问Leader
     */
    ClientServerRpc getClintServerRpc(URI uri);

    void stop();

}
