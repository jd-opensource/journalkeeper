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

import io.journalkeeper.rpc.BaseResponse;

/**
 * @author LiYue
 * Date: 2019-03-14
 */
public class AsyncAppendEntriesResponse extends BaseResponse implements Termed{
    private final boolean success;
    private AsyncAppendEntriesResponse(Throwable exception, boolean success, long journalIndex, int term, int entryCount) {
        super(exception);
        this.success = success;
        this.journalIndex = journalIndex;
        this.term = term;
        this.entryCount = entryCount;
    }

    public AsyncAppendEntriesResponse(boolean success, long journalIndex, int term, int entryCount) {
        this(null, success, journalIndex, term, entryCount);    }

    public AsyncAppendEntriesResponse(Throwable exception) {
        this(exception, false, -1L, -1, -1);
    }

    private final long journalIndex;

    private final int term;

    private final int entryCount;

    public long getJournalIndex() {
        return journalIndex;
    }

    public int getTerm() {
        return term;
    }

    public int getEntryCount() {
        return entryCount;
    }

    public boolean isSuccess() {
        return success;
    }
}
