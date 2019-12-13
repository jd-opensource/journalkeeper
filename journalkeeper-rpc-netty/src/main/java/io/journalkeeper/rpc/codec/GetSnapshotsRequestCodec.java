package io.journalkeeper.rpc.codec;

import io.journalkeeper.rpc.remoting.transport.command.Type;

/**
 * GetSnapshostsRequestCodec
 * author: gaohaoxiang
 * date: 2019/12/13
 */
public class GetSnapshotsRequestCodec extends VoidPayloadCodec implements Type {

    @Override
    public int type() {
        return RpcTypes.GET_SNAPSHOTS_REQUEST;
    }
}
