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

/**
 * 定义返回响应的级别：
 * ONE_WAY: 客户端单向向Server发送请求，无应答直接返回；
 * RECEIVE: Server收到请求后应答；
 * PERSISTENCE：Server将消息写入磁盘后应答；
 * REPLICATION：Server将消息复制到集群大多数节点后应答，默认值；
 * ALL：Server将消息复制到集群大多数节点后应答并且在LEADER写入磁盘后应答；
 *
 * @author LiYue
 * Date: 2019-04-23
 */
public enum ResponseConfig {
    ONE_WAY(0),
    RECEIVE(1),
    PERSISTENCE(2),
    REPLICATION(3),
    ALL(9);

    private int value;

    ResponseConfig(int value) {
        this.value = value;
    }

    public int value() {
        return value;
    }

    public static ResponseConfig valueOf(final int value) {
        switch (value) {
            case 0:
                return ONE_WAY;
            case 1:
                return RECEIVE;
            case 2:
                return PERSISTENCE;
            case 9:
                return ALL;
            default:
                return REPLICATION;
        }
    }
}
