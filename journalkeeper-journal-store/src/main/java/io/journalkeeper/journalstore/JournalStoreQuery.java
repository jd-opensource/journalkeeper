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
package io.journalkeeper.journalstore;

/**
 * @author LiYue
 * Date: 2019-05-08
 */
public class JournalStoreQuery {

    public static final int CMD_QUERY_ENTRIES = 0;
    public static final int CMD_QUERY_PARTITIONS = 1;
    public static final int CMD_QUERY_INDEX = 2;
    private final int cmd;
    private final int partition;
    private final long index;
    private final int size;
    private final long timestamp;


    JournalStoreQuery(int cmd, int partition, long index, int size, long timestamp) {
        this.cmd = cmd;
        this.partition = partition;
        this.index = index;
        this.size = size;
        this.timestamp = timestamp;
    }

    private JournalStoreQuery(int cmd) {
        this(cmd, 0, 0, 0, 0L);
    }

    private JournalStoreQuery(int partition, long timestamp) {
        this(CMD_QUERY_INDEX, partition, 0, 0, timestamp);
    }

    public static JournalStoreQuery createQueryEntries(int partition, long index, int size) {
        return new JournalStoreQuery(CMD_QUERY_ENTRIES, partition, index, size, 0L);
    }

    public static JournalStoreQuery createQueryPartitions() {
        return new JournalStoreQuery(CMD_QUERY_PARTITIONS);
    }

    public static JournalStoreQuery createQueryIndex(int partition, long timestamp) {
        return new JournalStoreQuery(partition, timestamp);
    }

    public int getCmd() {
        return cmd;
    }

    public long getIndex() {
        return index;
    }

    public int getSize() {
        return size;
    }

    public int getPartition() {
        return partition;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
