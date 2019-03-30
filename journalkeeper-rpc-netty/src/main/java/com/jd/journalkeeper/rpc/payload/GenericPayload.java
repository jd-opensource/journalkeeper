package com.jd.journalkeeper.rpc.payload;

import com.jd.journalkeeper.rpc.remoting.transport.command.Payload;

/**
 * @author liyue25
 * Date: 2019-03-29
 */
public class GenericPayload<T> implements Payload {
    private T payload;
    public GenericPayload(T payload) {
        this.payload = payload;
    }

    public T getPayload() {
        return payload;
    }

    public void setPayload(T payload) {
        this.payload = payload;
    }

    public static <P> P get(Object payload) {
        return ((GenericPayload<P>) payload).getPayload();
    }
}
