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
