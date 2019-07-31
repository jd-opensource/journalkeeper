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
package io.journalkeeper.rpc.remoting.example;

import io.journalkeeper.rpc.remoting.serialize.CodecSupport;
import io.journalkeeper.rpc.remoting.transport.codec.PayloadCodec;
import io.journalkeeper.rpc.remoting.transport.command.Types;
import io.netty.buffer.ByteBuf;

/**
 * @author LiYue
 * Date: 2019-03-28
 */
public class StringPayloadCodec implements PayloadCodec<TestHeader, StringPayload>, Types {
    @Override
    public Object decode(TestHeader header, ByteBuf buffer) throws Exception {
        return new StringPayload(CodecSupport.decodeString(buffer));
    }

    @Override
    public void encode(StringPayload payload, ByteBuf buffer) throws Exception {
        CodecSupport.encodeString(buffer,payload.getPayload());
    }

    @Override
    public int[] types() {
        return new int[] {-1, 1};
    }
}
