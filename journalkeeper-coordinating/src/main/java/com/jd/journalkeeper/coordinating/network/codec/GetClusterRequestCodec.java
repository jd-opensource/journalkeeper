package com.jd.journalkeeper.coordinating.network.codec;

import com.jd.journalkeeper.coordinating.network.CoordinatingCommands;
import com.jd.journalkeeper.coordinating.network.command.GetClusterRequest;
import io.netty.buffer.ByteBuf;

/**
 * GetClusterRequestCodec
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/4
 */
public class GetClusterRequestCodec implements CoordinatingPayloadCodec<GetClusterRequest> {

    @Override
    public GetClusterRequest decode(CoordinatingHeader header, ByteBuf buffer) throws Exception {
        return new GetClusterRequest();
    }

    @Override
    public void encode(GetClusterRequest payload, ByteBuf buffer) throws Exception {

    }

    @Override
    public int type() {
        return CoordinatingCommands.GET_CLUSTER_REQUEST.getType();
    }
}