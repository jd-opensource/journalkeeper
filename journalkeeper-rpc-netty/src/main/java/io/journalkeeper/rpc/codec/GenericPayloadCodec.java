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
package io.journalkeeper.rpc.codec;

import io.journalkeeper.rpc.header.JournalKeeperHeader;
import io.journalkeeper.rpc.payload.GenericPayload;
import io.journalkeeper.rpc.remoting.transport.codec.PayloadCodec;
import io.netty.buffer.ByteBuf;

/**
 * @author LiYue
 * Date: 2019-03-29
 */
public abstract class GenericPayloadCodec<P> implements PayloadCodec<JournalKeeperHeader, GenericPayload<P>> {
    @Override
    public final Object decode(JournalKeeperHeader header, ByteBuf buffer) throws Exception {

        return new GenericPayload<>(decodePayload(header, buffer));
    }

    @Override
    public final void encode(GenericPayload<P> payload, ByteBuf buffer) throws Exception {
        encodePayload(payload.getPayload(), buffer);
    }

    protected abstract void encodePayload(P payload, ByteBuf buffer) throws Exception;
    protected abstract P decodePayload(JournalKeeperHeader header, ByteBuf buffer) throws Exception;
}
