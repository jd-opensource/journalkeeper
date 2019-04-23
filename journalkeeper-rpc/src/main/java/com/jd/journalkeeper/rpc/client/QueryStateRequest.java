package com.jd.journalkeeper.rpc.client;

/**
 * RPC 方法
 * {@link ClientServerRpc#queryServerState(QueryStateRequest) queryServerState}
 * {@link ClientServerRpc#queryClusterState(QueryStateRequest) queryClusterState}
 * {@link ClientServerRpc#querySnapshot(QueryStateRequest) querySnapshot}
 * 请求参数。
 *
 * @author liyue25
 * Date: 2019-03-14
 */
public class QueryStateRequest {
    private final byte [] query;
    private final long index;

    public QueryStateRequest(byte [] query, long index) {
        this.query = query;
        this.index = index;
    }

    public QueryStateRequest(byte [] query) {
        this(query, -1L);
    }

    /**
     * 序列化后的查询条件。
     * @return 序列化后的查询条件。
     */
    public byte [] getQuery() {
        return query;
    }

    /**
     * Snapshot对应Journal的索引序号。
     * @return 索引序号。
     */
    public long getIndex() {
        return index;
    }
}
