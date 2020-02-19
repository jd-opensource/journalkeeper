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

import io.journalkeeper.rpc.BaseResponse;
import io.journalkeeper.rpc.StatusCode;
import io.journalkeeper.rpc.header.JournalKeeperHeader;
import io.netty.buffer.ByteBuf;

/**
 * @author LiYue
 * Date: 2019-03-29
 */
public abstract class ResponseCodec<R extends BaseResponse> extends GenericPayloadCodec<R> {
    @Override
    public final R decodePayload(JournalKeeperHeader header, ByteBuf buffer) throws Exception {
        R response = decodeResponse(header, buffer);
        response.setStatusCode(StatusCode.valueOf(header.getStatus()));
        response.setError(header.getError());
        return response;
    }

    @Override
    public final void encodePayload(R response, ByteBuf buffer) throws Exception {
        encodeResponse(response, buffer);
    }

    protected abstract void encodeResponse(R response, ByteBuf buffer) throws Exception;

    protected abstract R decodeResponse(JournalKeeperHeader header, ByteBuf buffer) throws Exception;
}
