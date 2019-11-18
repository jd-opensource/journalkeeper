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
 * 基于RAFT的Journal抽象
 * @author LiYue
 * Date: 2019-04-24
 */
public interface RaftJournal {
    /**
     * 默认分区。如果创建Server的时候不指定分区，创建一个默认分区。写入操作日志时，如果不指定分区，也写入到默认分区中。
     */
    int DEFAULT_PARTITION = 0;
    /**
     * JournalKeeper自用的保留分区，RAFT选举相关的操作日志都写入到这个分区中。
     */
    int RAFT_PARTITION = Short.MAX_VALUE;
    /**
     * 保留起始分区，这个分区号之后（含）的分区为保留分区，不允许用户使用。
     */
    int RESERVED_PARTITIONS_START = 30000;

    /**
     * 最小全局索引
     * @return 最小全局索引
     */
    long minIndex();

    /**
     * 最大全局索引
     * @return 最大全局索引
     */
    long maxIndex();

    /**
     * 分区最小索引
     * @param partition 分区
     * @return 分区最小索引
     */
    long minIndex(int partition);

    /**
     * 分区最大索引
     * @param partition 分区
     * @return 分区最大索引
     */
    long maxIndex(int partition);

    /**
     * 根据分区索引读取Journal
     * @param partition 分区
     * @param index 分区索引
     * @return See {@link JournalEntry}
     */
    JournalEntry readByPartition(int partition, long index);

    /**
     * 根据分区索引批量读取Journal
     * @param partition 分区
     * @param index 分区索引
     * @param maxSize 建议返回的最大数量
     * @return See {@link JournalEntry}
     */
    List<JournalEntry> batchReadByPartition(int partition, long index, int maxSize);

    /**
     * 使用全局索引读取Journal
     * @param index 全局索引
     * @return See {@link JournalEntry}
     */
    JournalEntry read(long index);

    /**
     * 使用全局索引读取Journal
     * @param index 全局索引
     * @param maxSize 建议返回的最大数量
     * @return See {@link JournalEntry}
     */
    List<JournalEntry> batchRead(long index, int maxSize);

    /**
     * 根据JournalEntry存储时间获取索引。
     * @param partition 分区
     * @param timestamp 查询时间，单位MS
     * @return 如果找到，返回最后一条 “存储时间 不大于 timestamp” JournalEntry的索引。
     * 如果查询时间 小于 第一条JournalEntry的时间，返回第一条JournalEntry；
     * 如果找到的JournalEntry前后有多条时间相同的JournalEntry，则返回这些JournalEntry中的的第一条；
     * 其它情况，返回负值。
     */
    long queryIndexByTimestamp(int partition, long timestamp);

    /**
     * 获取当前所有分区，含保留分区。
     * @return 当前所有分区
     */
    Set<Integer> getPartitions();
}
