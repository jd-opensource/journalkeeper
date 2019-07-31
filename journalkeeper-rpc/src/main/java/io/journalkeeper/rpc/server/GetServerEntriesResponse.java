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
package io.journalkeeper.rpc.server;

import io.journalkeeper.exceptions.IndexOverflowException;
import io.journalkeeper.exceptions.IndexUnderflowException;
import io.journalkeeper.rpc.BaseResponse;
import io.journalkeeper.rpc.StatusCode;

import java.util.List;

/**
 * @author LiYue
 * Date: 2019-03-14
 */
public class GetServerEntriesResponse extends BaseResponse {
    private final List<byte []> entries;
    private final long minIndex;
    private final long lastApplied;

    public GetServerEntriesResponse(Throwable exception) {
        this(exception, null, -1L, -1L);
    }

    public GetServerEntriesResponse(List<byte []> entries, long minIndex, long lastApplied) {
        this(null, entries, minIndex, lastApplied);
    }

    private GetServerEntriesResponse(Throwable exception, List<byte []> entries, long minIndex, long lastApplied) {
        super(exception);
        this.entries = entries;
        this.minIndex = minIndex;
        this.lastApplied = lastApplied;
    }

    public List<byte []> getEntries() {
        return entries;
    }

    public long getMinIndex() {
        return minIndex;
    }

    public long getLastApplied() {
        return lastApplied;
    }

    @Override
    public void setException(Throwable throwable) {
        try {
            throw throwable;
        } catch (IndexOverflowException e) {
            setStatusCode(StatusCode.INDEX_OVERFLOW);
        } catch (IndexUnderflowException e) {
            setStatusCode(StatusCode.INDEX_UNDERFLOW);
        } catch (Throwable t) {
            super.setException(throwable);
        }
    }
}
