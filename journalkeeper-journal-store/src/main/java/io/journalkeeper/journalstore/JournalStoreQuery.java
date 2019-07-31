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

/**
 * @author LiYue
 * Date: 2019-05-08
 */
public class JournalStoreQuery {

    public static final int CMQ_QUERY_ENTRIES = 0;
    public static final int CMQ_QUERY_PARTITIONS = 1;
    private final int cmd;
    private final int partition;
    private final long index;
    private final int size;


    JournalStoreQuery(int cmd, int partition, long index, int size) {
        this.cmd = cmd;
        this.partition = partition;
        this.index = index;
        this.size = size;
    }
    private JournalStoreQuery(int cmd) {
        this(cmd, 0, 0, 0);
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

    public static JournalStoreQuery createQueryEntries(int partition, long index, int size) {
        return new JournalStoreQuery(CMQ_QUERY_ENTRIES, partition, index, size);
    }

    public static JournalStoreQuery createQueryPartitions() {
        return new JournalStoreQuery(CMQ_QUERY_PARTITIONS);
    }
}
