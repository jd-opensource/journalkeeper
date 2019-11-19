package io.journalkeeper.monitor;

/**
 * 监控采集器。
 * @author LiYue
 * Date: 2019/11/19
 */
public interface MonitorCollector {
    /**
     * 添加一个可监控的Server节点
     * @param server 可监控的Server
     */
    void addServer(MonitoredServer server);
    /**
     * 删除一个可监控的Server节点
     * @param server 可监控的Server
     */
    void removeServer(MonitoredServer server);
}
