package com.jd.journalkeeper.rpc.codec;

import com.jd.journalkeeper.rpc.remoting.transport.command.Type;

/**
 * @author liyue25
 * Date: 2019-04-22
 */
public class AddPullWatchRequestCodec extends VoidPayloadCodec implements Type {
    @Override
    public int type() {
        return RpcTypes.ADD_PULL_WATCH_REQUEST;
    }
}
