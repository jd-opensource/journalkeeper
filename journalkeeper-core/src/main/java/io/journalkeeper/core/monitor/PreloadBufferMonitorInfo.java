package io.journalkeeper.core.monitor;

import io.journalkeeper.utils.format.Format;

import java.util.Collection;

/**
 * @author LiYue
 * Date: 2019/12/9
 */
public class PreloadBufferMonitorInfo {
    private long max;
    private long totalUsed;
    private long directUsed;
    private long mapUsed;
    private Collection<PreloadCacheMonitorInfo> caches;

    public long getMax() {
        return max;
    }

    public void setMax(long max) {
        this.max = max;
    }

    public long getTotalUsed() {
        return totalUsed;
    }

    public void setTotalUsed(long totalUsed) {
        this.totalUsed = totalUsed;
    }

    public long getDirectUsed() {
        return directUsed;
    }

    public void setDirectUsed(long directUsed) {
        this.directUsed = directUsed;
    }

    public long getMapUsed() {
        return mapUsed;
    }

    public void setMapUsed(long mapUsed) {
        this.mapUsed = mapUsed;
    }

    public Collection<PreloadCacheMonitorInfo> getCaches() {
        return caches;
    }

    public void setCaches(Collection<PreloadCacheMonitorInfo> caches) {
        this.caches = caches;
    }

    @Override
    public String toString() {
        return "PreloadBufferMonitorInfo{" +
                "max=" + max +
                ", totalUsed=" + Format.formatSize(totalUsed) +
                ", directUsed=" + Format.formatSize(directUsed) +
                ", mapUsed=" + Format.formatSize(mapUsed) +
                ", caches=" + caches +
                '}';
    }
}
