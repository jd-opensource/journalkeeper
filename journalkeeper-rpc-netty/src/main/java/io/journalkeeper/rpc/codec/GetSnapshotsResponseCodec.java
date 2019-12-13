package io.journalkeeper.rpc.codec;

import io.journalkeeper.core.api.SnapshotEntry;
import io.journalkeeper.core.api.SnapshotsEntry;
import io.journalkeeper.rpc.client.GetSnapshotsResponse;
import io.journalkeeper.rpc.header.JournalKeeperHeader;
import io.journalkeeper.rpc.remoting.serialize.CodecSupport;
import io.journalkeeper.rpc.remoting.transport.command.Type;
import io.netty.buffer.ByteBuf;

import java.util.List;
import java.util.stream.Collectors;

/**
 * GetSnapshostsResponseCodec
 * author: gaohaoxiang
 * date: 2019/12/13
 */
public class GetSnapshotsResponseCodec extends LeaderResponseCodec<GetSnapshotsResponse> implements Type {

    @Override
    protected void encodeLeaderResponse(GetSnapshotsResponse leaderResponse, ByteBuf buffer) throws Exception {
        CodecSupport.encodeCollection(buffer, leaderResponse.getSnapshots().getSnapshots(),
                (obj, entryBuffer) -> {
                    SnapshotEntry entry = (SnapshotEntry) obj;
                    CodecSupport.encodeString(entryBuffer, entry.getPath());
                    CodecSupport.encodeLong(entryBuffer, entry.getLastIncludedIndex());
                    CodecSupport.encodeInt(entryBuffer, entry.getLastIncludedTerm());
                    CodecSupport.encodeLong(entryBuffer, entry.getMinOffset());
                    CodecSupport.encodeLong(entryBuffer, entry.getTimestamp());
                });
    }

    @Override
    protected GetSnapshotsResponse decodeLeaderResponse(JournalKeeperHeader header, ByteBuf buffer) throws Exception {
        return new GetSnapshotsResponse(
                new SnapshotsEntry((List) CodecSupport.decodeCollection(buffer, entryBuffer -> new SnapshotEntry(
                    CodecSupport.decodeString(entryBuffer),
                    CodecSupport.decodeLong(entryBuffer),
                    CodecSupport.decodeInt(entryBuffer),
                    CodecSupport.decodeLong(entryBuffer),
                    CodecSupport.decodeLong(entryBuffer)
        )).stream().collect(Collectors.toList())));
    }

    @Override
    public int type() {
        return RpcTypes.GET_SNAPSHOTS_RESPONSE;
    }
}