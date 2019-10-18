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
package io.journalkeeper.core.api;

import java.util.List;
import java.util.Set;

/**
 * @author LiYue
 * Date: 2019-04-24
 */
public interface RaftJournal {
    int DEFAULT_PARTITION = 0;
    int RESERVED_PARTITION = Short.MAX_VALUE;
    long minIndex();

    long maxIndex();

    long minIndex(int partition);

    long maxIndex(int partition);

    JournalEntry readByPartition(int partition, long index);

    List<JournalEntry> batchReadByPartition(int partition, long index, int maxSize);

    JournalEntry read(long index);

    List<JournalEntry> batchRead(long index, int size);

    /**
     * 根据JournalEntry存储时间获取索引。
     * @param partition 分区
     * @param timestamp 查询时间，单位MS
     * @return 如果找到，返回最后一条 “存储时间 <= timestamp” JournalEntry的索引。
     * 如果查询时间 < 第一条JournalEntry的时间，返回第一条JournalEntry；
     * 如果找到的JournalEntry前后有多条时间相同的JournalEntry，则返回这些JournalEntry中的的第一条；
     * 其它情况，返回负值。
     */
    long queryIndexByTimestamp(int partition, long timestamp);

    Set<Integer> getPartitions();
}
