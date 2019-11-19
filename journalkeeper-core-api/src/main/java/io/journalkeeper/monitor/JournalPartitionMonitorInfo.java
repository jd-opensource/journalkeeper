package io.journalkeeper.monitor;

/**
 * @author LiYue
 * Date: 2019/11/19
 */
public class JournalPartitionMonitorInfo {
    // 分区
    private int partition = -1;
    // 分区最小索引序号
    private long minIndex = -1L;
    // 分区最大索引序号
    private long maxIndex = -1L;
    // 分区索引存储最小物理位置
    private long minOffset = -1L;
    // 分区索引存储最大物理位置
    private long maxOffset = -1L;
    // 分区索引存储物理刷盘位置
    private long flushOffset = -1L;

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

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

    @Override
    public String toString() {
        return "JournalPartitionMonitorInfo{" +
                "partition=" + partition +
                ", minIndex=" + minIndex +
                ", maxIndex=" + maxIndex +
                ", minOffset=" + minOffset +
                ", maxOffset=" + maxOffset +
                ", flushOffset=" + flushOffset +
                '}';
    }
}
