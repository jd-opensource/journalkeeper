package com.jd.journalkeeper.rpc.client;

/**
 * RPC方法
 * {@link ClientServerRpc#queryServerState(com.jd.journalkeeper.rpc.client.QueryStateRequest) queryServerState}
 * 请求参数。
 * @author liyue25
 * Date: 2019-03-14
 */
public class UpdateClusterStateRequest {
    private final byte [] entry;

    public UpdateClusterStateRequest(byte [] entry) {
        this.entry = entry;
    }

    /**
     * 序列化后待执行的操作。entry将被：
     * 1. 写入Journal；
     * 2. 被状态机执行，变更系统状态；
     * @return 序列化后的entry。
     */
    public byte [] getEntry() {
        return entry;
    }
}
