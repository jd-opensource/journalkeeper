/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jd.journalkeeper.rpc.codec;

import com.jd.journalkeeper.rpc.header.JournalKeeperHeader;
import com.jd.journalkeeper.rpc.remoting.serialize.CodecSupport;
import com.jd.journalkeeper.rpc.remoting.transport.command.Type;
import com.jd.journalkeeper.rpc.server.AsyncAppendEntriesResponse;
import com.jd.journalkeeper.rpc.server.RequestVoteResponse;
import io.netty.buffer.ByteBuf;

/**
 * @author liyue25
 * Date: 2019-04-02
 */
public class RequestVoteResponseCodec extends ResponseCodec<RequestVoteResponse> implements Type {
    @Override
    protected void encodeResponse(RequestVoteResponse response, ByteBuf buffer) throws Exception {
        // int term, boolean voteGranted
        CodecSupport.encodeInt(buffer, response.getTerm());
        CodecSupport.encodeBoolean(buffer, response.isVoteGranted());
    }

    @Override
    protected RequestVoteResponse decodeResponse(JournalKeeperHeader header, ByteBuf buffer) throws Exception {
        return new RequestVoteResponse(
                CodecSupport.decodeInt(buffer),
                CodecSupport.decodeBoolean(buffer)
        );
    }

    @Override
    public int type() {
        return RpcTypes.REQUEST_VOTE_RESPONSE;
    }
}
