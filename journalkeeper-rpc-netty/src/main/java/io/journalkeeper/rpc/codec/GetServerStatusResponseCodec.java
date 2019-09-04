package io.journalkeeper.rpc.codec;

import io.journalkeeper.core.api.RaftServer;
import io.journalkeeper.core.api.ServerStatus;
import io.journalkeeper.core.api.VoterState;
import io.journalkeeper.rpc.client.GetServerStatusResponse;
import io.journalkeeper.rpc.header.JournalKeeperHeader;
import io.journalkeeper.rpc.remoting.serialize.CodecSupport;
import io.journalkeeper.rpc.remoting.transport.command.Type;
import io.netty.buffer.ByteBuf;

/**
 * @author LiYue
 * Date: 2019-09-04
 */
public class GetServerStatusResponseCodec extends ResponseCodec<GetServerStatusResponse> implements Type {
    @Override
    protected void encodeResponse(GetServerStatusResponse response, ByteBuf buffer) throws Exception {
        ServerStatus serverStatus = response.getServerStatus();
        if(null == serverStatus) serverStatus = new ServerStatus();
        CodecSupport.encodeString(buffer, serverStatus.getRoll() == null ? "": serverStatus.getRoll().name());
        CodecSupport.encodeString(buffer,  serverStatus.getVoterState() == null ? "": serverStatus.getVoterState().name());

        CodecSupport.encodeLong(buffer, serverStatus.getMinIndex());
        CodecSupport.encodeLong(buffer, serverStatus.getMaxIndex());
        CodecSupport.encodeLong(buffer, serverStatus.getCommitIndex());
        CodecSupport.encodeLong(buffer, serverStatus.getLastApplied());
    }

    @Override
    protected GetServerStatusResponse decodeResponse(JournalKeeperHeader header, ByteBuf buffer) throws Exception {
        String str = CodecSupport.decodeString(buffer);
        RaftServer.Roll roll = str.isEmpty() ? null : RaftServer.Roll.valueOf(str);
        str = CodecSupport.decodeString(buffer);
        VoterState voterState = str.isEmpty() ? null : VoterState.valueOf(str);

        return new GetServerStatusResponse(new ServerStatus(
                roll,
                CodecSupport.decodeLong(buffer),
                CodecSupport.decodeLong(buffer),
                CodecSupport.decodeLong(buffer),
                CodecSupport.decodeLong(buffer),
                voterState
        ));
    }

    @Override
    public int type() {
        return RpcTypes.GET_SERVER_STATUS_RESPONSE;
    }
}
