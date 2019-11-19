package io.journalkeeper.monitor;

import java.net.URI;
import java.util.Collection;

/**
 * @author LiYue
 * Date: 2019/11/19
 */
public class NodeMonitorInfo {
    // 标识是否处于集群节点配置变更的中间状态
    private boolean jointConsensus = false;
    // 集群当前配置	nodes.jointConsensus为false时有效
    private Collection<URI> config = null;
    // 集群新配置	nodes.jointConsensus为true时有效
    private Collection<URI> newConfig = null;
    // 集群旧配置	nodes.jointConsensus为true时有效
    private Collection<URI> oldConfig = null;

    public boolean isJointConsensus() {
        return jointConsensus;
    }

    public void setJointConsensus(boolean jointConsensus) {
        this.jointConsensus = jointConsensus;
    }

    public Collection<URI> getConfig() {
        return config;
    }

    public void setConfig(Collection<URI> config) {
        this.config = config;
    }

    public Collection<URI> getNewConfig() {
        return newConfig;
    }

    public void setNewConfig(Collection<URI> newConfig) {
        this.newConfig = newConfig;
    }

    public Collection<URI> getOldConfig() {
        return oldConfig;
    }

    public void setOldConfig(Collection<URI> oldConfig) {
        this.oldConfig = oldConfig;
    }

    @Override
    public String toString() {
        return "NodeMonitorInfo{" +
                "jointConsensus=" + jointConsensus +
                ", config=" + config +
                ", newConfig=" + newConfig +
                ", oldConfig=" + oldConfig +
                '}';
    }
}
