package com.jd.journalkeeper.rpc.client;

/**
 * @author liyue25
 * Date: 2019-03-14
 */
public class QueryStateRequest<Q> {
    private final Q query;
    private final long index;

    public QueryStateRequest(Q query, long index) {
        this.query = query;
        this.index = index;
    }

    public QueryStateRequest(Q query) {
        this(query, -1L);
    }

    public Q getQuery() {
        return query;
    }

    public long getIndex() {
        return index;
    }
}
