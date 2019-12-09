package io.journalkeeper.persistence;

import java.nio.file.Path;

/**
 * Monitored persistence
 * This is a optional interface which will be used to monitor disk usage.
 * @author LiYue
 * Date: 2019/12/9
 */
public interface MonitoredPersistence {
    Path getPath();
    long getFreeSpace();
    long getTotalSpace();
}
