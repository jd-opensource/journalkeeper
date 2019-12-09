package io.journalkeeper.monitor;

import java.nio.file.Path;

/**
 * @author LiYue
 * Date: 2019/12/9
 */
public class DiskMonitorInfo {
    private Path path;
    private long free;
    private long total;

    public Path getPath() {
        return path;
    }

    public void setPath(Path path) {
        this.path = path;
    }

    public long getFree() {
        return free;
    }

    public void setFree(long free) {
        this.free = free;
    }

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    @Override
    public String toString() {
        return "DiskMonitorInfo{" +
                "path=" + path +
                ", free=" + free +
                ", total=" + total +
                '}';
    }
}
