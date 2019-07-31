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

import io.journalkeeper.utils.state.StateServer;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Properties;

/**
 * Raft节点
 * @param <E> 日志类型
 * @param <Q> 状态查询条件类型
 * @param <R> 状态查询结果类型
 * @author LiYue
 * Date: 2019-03-14
 */
public abstract class RaftServer<E, Q, R> implements StateServer {

    protected final StateFactory<E, Q, R> stateFactory;
    /**
     * 属性集
     */
    protected final Properties properties;
    public RaftServer(StateFactory<E, Q, R> stateFactory, Properties properties){
        this.stateFactory = stateFactory;
        this.properties = properties;
    }
    public Properties properties() {
        return properties;
    }
    public abstract Roll roll();
    public abstract void init(URI uri, List<URI> voters) throws IOException;
    public abstract void recover() throws IOException;
    public abstract URI serverUri();
    public enum Roll {VOTER, OBSERVER}

}
