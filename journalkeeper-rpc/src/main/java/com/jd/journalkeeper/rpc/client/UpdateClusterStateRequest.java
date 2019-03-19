package com.jd.journalkeeper.rpc.client;

/**
 * @author liyue25
 * Date: 2019-03-14
 */
public class UpdateClusterStateRequest<E> {
    private final E entry;

    public UpdateClusterStateRequest(E entry) {
        this.entry = entry;
    }

    public E getEntry() {
        return entry;
    }
}
