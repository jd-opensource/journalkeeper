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
package io.journalkeeper.sql.state;

import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * SQLExecutorManager
 * author: gaohaoxiang
 * date: 2019/8/1
 */
public class SQLExecutorManager {

    private static final Map<String, SQLExecutorFactory> executors;

    static {
        executors = loadExecutors();
    }

    protected static Map<String, SQLExecutorFactory> loadExecutors() {
        Map<String, SQLExecutorFactory> result = new HashMap<>();
        ServiceLoader<SQLExecutorFactory> factoryLoader = ServiceLoader.load(SQLExecutorFactory.class);
        Iterator<SQLExecutorFactory> iterator = factoryLoader.iterator();

        while (iterator.hasNext()) {
            SQLExecutorFactory factory = iterator.next();
            result.put(factory.type(), factory);
        }
        return result;
    }

    public static SQLExecutorFactory getExecutor(String type) {
        if (StringUtils.isBlank(type)) {
            return executors.values().iterator().next();
        }
        return executors.get(type);
    }
}