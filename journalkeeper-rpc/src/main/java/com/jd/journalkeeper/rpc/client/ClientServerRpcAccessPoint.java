package com.jd.journalkeeper.rpc.client;

import java.net.URI;

/**
 * ClientServerRpc接入点，管理Client和Server的rpc远程连接。
 * @author liyue25
 * Date: 2019-03-14
 */
public interface ClientServerRpcAccessPoint<E, Q, R> {
   ClientServerRpc<E, Q, R> getClintServerRpc();
   ClientServerRpc<E, Q, R> getClintServerRpc(URI uri);
}
