package com.jd.journalkeeper.rpc.client;

import com.jd.journalkeeper.rpc.remoting.transport.command.Payload;

/**
 * @author liyue25
 * Date: 2019-03-28
 */
public class StringPayload implements Payload {
    private String payload;

    public StringPayload(String payload) {
        this.payload = payload;
    }
    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }
}
