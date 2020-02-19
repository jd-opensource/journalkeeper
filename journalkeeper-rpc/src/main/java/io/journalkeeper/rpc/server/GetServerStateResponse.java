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
package io.journalkeeper.rpc.server;

import io.journalkeeper.rpc.BaseResponse;

/**
 * @author LiYue
 * Date: 2019-03-14
 */
public class GetServerStateResponse extends BaseResponse {
    //    private final S state;
    private final long lastIncludedIndex;
    private final int lastIncludedTerm;
    private final long offset;
    private final byte[] data;
    private final boolean done;
    private final int iteratorId;

    private GetServerStateResponse(Throwable exception, long lastIncludedIndex, int lastIncludedTerm, long offset, byte[] data, boolean done, int iteratorId) {
        super(exception);
        this.lastIncludedIndex = lastIncludedIndex;
        this.lastIncludedTerm = lastIncludedTerm;
        this.offset = offset;
        this.data = data;
        this.done = done;
        this.iteratorId = iteratorId;
    }

    public GetServerStateResponse(Throwable exception) {
        this(exception, -1L, -1, -1L, null, false, -1);
    }

    public GetServerStateResponse(long lastIncludedIndex, int lastIncludedTerm, long offset, byte[] data, boolean done, int iteratorId) {
        this(null, lastIncludedIndex, lastIncludedTerm, offset, data, done, iteratorId);
    }

    public long getLastIncludedIndex() {
        return lastIncludedIndex;
    }

    public int getLastIncludedTerm() {
        return lastIncludedTerm;
    }

    public long getOffset() {
        return offset;
    }

    public byte[] getData() {
        return data;
    }

    public boolean isDone() {
        return done;
    }

    public int getIteratorId() {
        return iteratorId;
    }
}
