package com.jd.journalkeeper.coordinating.network.codec;

import com.jd.journalkeeper.coordinating.network.CoordinatingCommands;
import com.jd.journalkeeper.coordinating.network.command.GetClusterResponse;
import com.jd.journalkeeper.rpc.remoting.serialize.CodecSupport;
import com.jd.journalkeeper.rpc.remoting.transport.command.Type;
import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.StringUtils;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * GetClusterResponseCodec
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/4
 */
public class GetClusterResponseCodec implements CoordinatingPayloadCodec<GetClusterResponse>, Type {

    @Override
    public GetClusterResponse decode(CoordinatingHeader header, ByteBuf buffer) throws Exception {
        URI leader = readNode(buffer);

        int followerSize = CodecSupport.decodeInt(buffer);
        List<URI> followers = new ArrayList<>(followerSize);
        for (int i = 0; i < followerSize; i++) {
            followers.add(readNode(buffer));
        }

        GetClusterResponse response = new GetClusterResponse();
        response.setLeader(leader);
        response.setFollowers(followers);
        return response;
    }

    @Override
    public void encode(GetClusterResponse payload, ByteBuf buffer) throws Exception {
        writeNode(payload.getLeader(), buffer);

        if (payload.getFollowers() == null) {
            CodecSupport.encodeInt(buffer, 0);
        } else {
            CodecSupport.encodeInt(buffer, payload.getFollowers().size());
            for (URI uri : payload.getFollowers()) {
                writeNode(uri, buffer);
            }
        }
    }

    protected URI readNode(ByteBuf buffer) {
        String schema = CodecSupport.decodeString(buffer);
        if (StringUtils.isBlank(schema)) {
            return null;
        }

        String host = CodecSupport.decodeString(buffer);
        int port = CodecSupport.decodeInt(buffer);
        return URI.create(String.format("%s://%s:%s", schema, host, port));
    }

    protected void writeNode(URI node, ByteBuf buffer) {
        if (node == null) {
            CodecSupport.encodeString(buffer, null);
            return;
        }

        CodecSupport.encodeString(buffer, node.getScheme());
        CodecSupport.encodeString(buffer, node.getHost());
        CodecSupport.encodeInt(buffer, node.getPort());
    }

    @Override
    public int type() {
        return CoordinatingCommands.GET_CLUSTER_RESPONSE.getType();
    }
}