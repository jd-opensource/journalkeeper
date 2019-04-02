package com.jd.journalkeeper.rpc.codec;

import com.jd.journalkeeper.rpc.remoting.transport.command.Type;

/**
 * @author liyue25
 * Date: 2019-04-02
 */
public class GetServersRequestCodec extends VoidPayloadCodec implements Type {
    @Override
    public int type() {
        return RpcTypes.GET_SERVERS_REQUEST;
    }
}
