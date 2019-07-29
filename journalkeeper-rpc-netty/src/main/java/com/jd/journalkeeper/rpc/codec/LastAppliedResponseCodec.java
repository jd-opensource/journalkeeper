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

import com.jd.journalkeeper.rpc.client.LastAppliedResponse;
import com.jd.journalkeeper.rpc.header.JournalKeeperHeader;
import com.jd.journalkeeper.rpc.remoting.serialize.CodecSupport;
import com.jd.journalkeeper.rpc.remoting.transport.command.Type;
import io.netty.buffer.ByteBuf;

import static com.jd.journalkeeper.rpc.codec.RpcTypes.LAST_APPLIED_RESPONSE;

/**
 * @author liyue25
 * Date: 2019-04-02
 */
public class LastAppliedResponseCodec extends LeaderResponseCodec<LastAppliedResponse> implements Type {
    @Override
    protected void encodeLeaderResponse(LastAppliedResponse leaderResponse, ByteBuf buffer) throws Exception {
        CodecSupport.encodeLong(buffer, leaderResponse.getLastApplied());
    }

    @Override
    protected LastAppliedResponse decodeLeaderResponse(JournalKeeperHeader header, ByteBuf buffer) throws Exception {
        return new LastAppliedResponse(CodecSupport.decodeLong(buffer));
    }

    @Override
    public int type() {
        return LAST_APPLIED_RESPONSE;
    }
}
