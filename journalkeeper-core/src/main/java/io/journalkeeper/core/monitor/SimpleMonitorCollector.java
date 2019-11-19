package io.journalkeeper.core.monitor;

import io.journalkeeper.monitor.MonitorCollector;
import io.journalkeeper.monitor.MonitoredServer;
import io.journalkeeper.monitor.ServerMonitorInfo;
import io.journalkeeper.utils.spi.Singleton;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 采集进程内所有JournalKeeper Server 节点的监控信息
 * @author LiYue
 * Date: 2019/11/19
 */
@Singleton
public class SimpleMonitorCollector implements MonitorCollector {
    private Map<URI,MonitoredServer> monitoredServers = new HashMap<>();
    @Override
    public synchronized void addServer(MonitoredServer server) {
        monitoredServers.put(server.uri(), server);
    }

    @Override
    public synchronized void removeServer(MonitoredServer server) {
        monitoredServers.remove(server);
    }

    public MonitoredServer getMonitoredServer(URI uri) {
        return monitoredServers.get(uri);
    }

    public Collection<MonitoredServer> getMonitoredServers() {
        return monitoredServers.values();
    }

    public Collection<ServerMonitorInfo> collectAll() {
        List<ServerMonitorInfo> monitorInfos = new ArrayList<>();
        if(null != monitoredServers) {
            for (MonitoredServer monitoredServer : monitoredServers.values()) {
                ServerMonitorInfo serverMonitorInfo = monitoredServer.collect();
                monitorInfos.add(serverMonitorInfo);
            }
        }
        return monitorInfos;
    }
}
