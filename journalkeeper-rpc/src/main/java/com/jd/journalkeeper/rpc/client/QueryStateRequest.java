package com.jd.journalkeeper.rpc.client;

/**
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

    public byte [] getQuery() {
        return query;
    }

    public long getIndex() {
        return index;
    }
}
