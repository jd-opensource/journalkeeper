/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.journalkeeper.core.api;

import java.net.URI;
import java.util.List;
import java.util.Set;

/**
 * Raft集群配置（服务器信息）
 * @author LiYue
 * Date: 2019-03-14
 */
public class ClusterConfiguration {
    private URI leader;
    private List<URI> voters;
    private List<URI> observers;

    public ClusterConfiguration() {}
    public ClusterConfiguration(URI leader, List<URI> voters, List<URI> observers){
        this.leader = leader;
        this.voters = voters;
        this.observers = observers;
    }


    public URI getLeader() {
        return leader;
    }

    public void setLeader(URI leader) {
        this.leader = leader;
    }

    public List<URI> getVoters() {
        return voters;
    }

    public void setVoters(List<URI> voters) {
        this.voters = voters;
    }

    public List<URI> getObservers() {
        return observers;
    }

    public void setObservers(List<URI> observers) {
        this.observers = observers;
    }
}
