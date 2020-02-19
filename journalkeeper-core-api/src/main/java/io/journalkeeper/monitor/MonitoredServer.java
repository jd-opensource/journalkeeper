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
