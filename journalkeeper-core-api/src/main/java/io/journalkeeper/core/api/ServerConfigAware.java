package io.journalkeeper.core.api;

import java.net.URI;
import java.util.List;

/**
 * @author LiYue
 * Date: 2019-09-09
 */
public interface ServerConfigAware {
    /**
     * 客户端使用
     * 更新可供连接的server列表
     */
    void updateServers(List<URI> servers);
}
