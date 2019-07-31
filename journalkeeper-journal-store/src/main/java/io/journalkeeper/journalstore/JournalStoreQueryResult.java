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
package io.journalkeeper.journalstore;

import io.journalkeeper.core.api.RaftEntry;
import io.journalkeeper.exceptions.IndexOverflowException;
import io.journalkeeper.exceptions.IndexUnderflowException;

import java.util.List;
import java.util.Map;

/**
 * @author LiYue
 * Date: 2019-05-09
 */
public class JournalStoreQueryResult{
    public static final int CODE_SUCCESS = 0;
    public static final int CODE_UNDERFLOW = -1;
    public static final int CODE_OVERFLOW = -2;
    public static final int CODE_EXCEPTION = -3;
    private final int cmd;
    private final int code;
    private final List<RaftEntry> entries;
    private final Map<Integer, Boundary> boundaries;

    public JournalStoreQueryResult(List<RaftEntry> entries, Map<Integer, Boundary> boundaries, int cmd) {
        this(entries, boundaries, cmd, CODE_SUCCESS);
    }

    public JournalStoreQueryResult(List<RaftEntry> entries, Map<Integer, Boundary> boundaries, int cmd, int code) {
        this.entries = entries;
        this.boundaries = boundaries;
        this.cmd = cmd;
        this.code = code;
    }

    public JournalStoreQueryResult(List<RaftEntry> entries) {
        this(entries, null, JournalStoreQuery.CMQ_QUERY_ENTRIES);
    }


    public JournalStoreQueryResult( Map<Integer, Boundary> boundaries) {
        this(null, boundaries, JournalStoreQuery.CMQ_QUERY_PARTITIONS);
    }

    public JournalStoreQueryResult(Throwable t, int cmd) {
        this.cmd = cmd;
        this.boundaries = null;
        this.entries = null;

        try {
            throw t;
        } catch (IndexUnderflowException e) {
            this.code = CODE_UNDERFLOW;
        } catch (IndexOverflowException e) {
            this.code = CODE_OVERFLOW;
        } catch (Throwable tr) {
            this.code = CODE_EXCEPTION;
        }
    }

    public static class Boundary {
        private final long min;
        private final long max;

        public Boundary(long min, long max) {
            this.min = min;
            this.max = max;
        }

        public long getMin() {
            return min;
        }

        public long getMax() {
            return max;
        }
    }

    public int getCmd() {
        return cmd;
    }

    public List<RaftEntry> getEntries() {
        return entries;
    }

    public Map<Integer, Boundary> getBoundaries() {
        return boundaries;
    }

    public int getCode() {
        return code;
    }
}
