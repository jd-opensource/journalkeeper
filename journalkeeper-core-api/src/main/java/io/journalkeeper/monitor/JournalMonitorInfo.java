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
package io.journalkeeper.monitor;

import java.util.Collection;

/**
 * @author LiYue
 * Date: 2019/11/19
 */
public class JournalMonitorInfo {
    // 最小索引序号
    private long minIndex = -1L;
    // 最大索引序号
    private long maxIndex = -1L;
    // 刷盘索引序号
    private long flushIndex = -1L;
    // 已提交索引序号
    private long commitIndex = -1L;
    // 状态机执行索引序号
    private long appliedIndex = -1L;
    // Journal存储最小物理位置
    private long minOffset = -1L;
    // Journal存储最大物理位置
    private long maxOffset = -1L;
    // Journal存储物理刷盘位置
    private long flushOffset = -1L;
    // 索引存储最小物理位置
    private long indexMinOffset = -1L;
    // 索引存储最大物理位置
    private long indexMaxOffset = -1L;
    // 索引存储物理刷盘位置
    private long indexFlushOffset = -1L;
    // 分区信息
    private Collection<JournalPartitionMonitorInfo> partitions = null;

    public long getMinIndex() {
        return minIndex;
    }

    public void setMinIndex(long minIndex) {
        this.minIndex = minIndex;
    }

    public long getMaxIndex() {
        return maxIndex;
    }

    public void setMaxIndex(long maxIndex) {
        this.maxIndex = maxIndex;
    }

    public long getFlushIndex() {
        return flushIndex;
    }

    public void setFlushIndex(long flushIndex) {
        this.flushIndex = flushIndex;
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public void setCommitIndex(long commitIndex) {
        this.commitIndex = commitIndex;
    }

    public long getAppliedIndex() {
        return appliedIndex;
    }

    public void setAppliedIndex(long appliedIndex) {
        this.appliedIndex = appliedIndex;
    }

    public long getMinOffset() {
        return minOffset;
    }

    public void setMinOffset(long minOffset) {
        this.minOffset = minOffset;
    }

    public long getMaxOffset() {
        return maxOffset;
    }

    public void setMaxOffset(long maxOffset) {
        this.maxOffset = maxOffset;
    }

    public long getFlushOffset() {
        return flushOffset;
    }

    public void setFlushOffset(long flushOffset) {
        this.flushOffset = flushOffset;
    }

    public long getIndexMinOffset() {
        return indexMinOffset;
    }

    public void setIndexMinOffset(long indexMinOffset) {
        this.indexMinOffset = indexMinOffset;
    }

    public long getIndexMaxOffset() {
        return indexMaxOffset;
    }

    public void setIndexMaxOffset(long indexMaxOffset) {
        this.indexMaxOffset = indexMaxOffset;
    }

    public long getIndexFlushOffset() {
        return indexFlushOffset;
    }

    public void setIndexFlushOffset(long indexFlushOffset) {
        this.indexFlushOffset = indexFlushOffset;
    }

    public Collection<JournalPartitionMonitorInfo> getPartitions() {
        return partitions;
    }

    public void setPartitions(Collection<JournalPartitionMonitorInfo> partitions) {
        this.partitions = partitions;
    }

    @Override
    public String toString() {
        return "JournalMonitorInfo{" +
                "minIndex=" + minIndex +
                ", maxIndex=" + maxIndex +
                ", flushIndex=" + flushIndex +
                ", commitIndex=" + commitIndex +
                ", appliedIndex=" + appliedIndex +
                ", minOffset=" + minOffset +
                ", maxOffset=" + maxOffset +
                ", flushOffset=" + flushOffset +
                ", indexMinOffset=" + indexMinOffset +
                ", indexMaxOffset=" + indexMaxOffset +
                ", indexFlushOffset=" + indexFlushOffset +
                ", partitions=" + partitions +
                '}';
    }
}
