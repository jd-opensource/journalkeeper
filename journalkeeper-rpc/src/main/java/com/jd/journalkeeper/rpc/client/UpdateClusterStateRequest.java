package com.jd.journalkeeper.rpc.client;

/**
 * @author liyue25
 * Date: 2019-03-14
 */
public class UpdateClusterStateRequest {
    private final byte [] entry;

    public UpdateClusterStateRequest(byte [] entry) {
        this.entry = entry;
    }

    public byte [] getEntry() {
        return entry;
    }
}
