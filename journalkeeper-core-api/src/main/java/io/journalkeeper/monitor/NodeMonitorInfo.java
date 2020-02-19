/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
