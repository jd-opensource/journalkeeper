/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.journalkeeper.rpc.codec;

import io.journalkeeper.rpc.LeaderResponse;
import io.journalkeeper.rpc.header.JournalKeeperHeader;
import io.journalkeeper.rpc.remoting.serialize.CodecSupport;
import io.netty.buffer.ByteBuf;

import java.net.URI;

/**
 * @author LiYue
 * Date: 2019-03-29
 */
public abstract class LeaderResponseCodec<R extends LeaderResponse> extends ResponseCodec<R> {
    @Override
    protected final void encodeResponse(JournalKeeperHeader header, R response, ByteBuf buffer) throws Exception {
        encodeLeaderResponse(header, response, buffer);
        CodecSupport.encodeString(buffer, response.getLeader() == null ? null : response.getLeader().toString());
    }

    @Override
    protected final R decodeResponse(JournalKeeperHeader header, ByteBuf buffer) throws Exception {
        R response = decodeLeaderResponse(header, buffer);
        String leaderStr = CodecSupport.decodeString(buffer);
        if (leaderStr.length() > 0) {
            response.setLeader(URI.create(leaderStr));
        }
        return response;
    }

    protected abstract void encodeLeaderResponse(JournalKeeperHeader header,R leaderResponse, ByteBuf buffer) throws Exception;

    protected abstract R decodeLeaderResponse(JournalKeeperHeader header, ByteBuf buffer) throws Exception;
}
