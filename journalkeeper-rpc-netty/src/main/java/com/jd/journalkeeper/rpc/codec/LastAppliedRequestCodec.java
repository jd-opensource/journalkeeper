package com.jd.journalkeeper.rpc.codec;

import com.jd.journalkeeper.rpc.remoting.transport.command.Type;

import static com.jd.journalkeeper.rpc.codec.RpcTypes.LAST_APPLIED_REQUEST;

/**
 * @author liyue25
 * Date: 2019-04-02
 */
public class LastAppliedRequestCodec extends VoidPayloadCodec implements Type {
    @Override
    public int type() {
        return LAST_APPLIED_REQUEST;
    }
}
