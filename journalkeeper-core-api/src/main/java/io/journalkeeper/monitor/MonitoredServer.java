package io.journalkeeper.monitor;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * 可监控的Server节点
 * @author LiYue
 * Date: 2019/11/19
 */
public interface MonitoredServer {
    URI uri();
    /**
     * 采集监控数据
     * @return 节点监控数据
     */
    ServerMonitorInfo collect();
}
