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
package io.journalkeeper.sql.state.support;

import io.journalkeeper.sql.exception.SQLException;
import io.journalkeeper.sql.state.SQLTransactionExecutor;
import io.journalkeeper.sql.state.config.SQLConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * SQLTransactionExecutorManager
 * author: gaohaoxiang
 * date: 2019/8/7
 */
public class SQLTransactionExecutorManager {

    protected static final Logger logger = LoggerFactory.getLogger(SQLTransactionExecutorManager.class);

    private int maxActive;
    private SQLTransactionExecutorCleaner transactionExecutorCleaner;
    private final ConcurrentMap<String, SQLTransactionExecutorEntry> transactionExecutors = new ConcurrentHashMap<>();

    public SQLTransactionExecutorManager(Properties properties) {
        this.maxActive = Integer.valueOf(properties.getProperty(SQLConfigs.TRANSACTION_MAX_ACTIVE,
                String.valueOf(SQLConfigs.DEFAULT_TRANSACTION_MAX_ACTIVE)));
        this.transactionExecutorCleaner = new SQLTransactionExecutorCleaner(this, properties);
    }

    public SQLTransactionExecutor begin(String id, SQLTransactionExecutor executor) {
        if (transactionExecutors.size() >= maxActive) {
            throw new SQLException("active transaction out of range");
        }
        if (transactionExecutors.containsKey(id)) {
            throw new SQLException("transaction is exist");
        }
        transactionExecutors.put(id, new SQLTransactionExecutorEntry(id, executor, System.currentTimeMillis()));
        return executor;
    }

    public SQLTransactionExecutor get(String id) {
        SQLTransactionExecutorEntry transactionExecutorEntry = transactionExecutors.get(id);
        if (transactionExecutorEntry == null) {
            return null;
        }
        return transactionExecutorEntry.getTransactionExecutor();
    }

    public boolean commit(String id) {
        transactionExecutors.remove(id);
        return true;
    }

    public boolean rollback(String id) {
        transactionExecutors.remove(id);
        return true;
    }

    public Map<String, SQLTransactionExecutorEntry> getAll() {
        return transactionExecutors;
    }

    public class SQLTransactionExecutorEntry {
        private String id;
        private SQLTransactionExecutor transactionExecutor;
        private long createTime;

        public SQLTransactionExecutorEntry() {

        }

        public SQLTransactionExecutorEntry(String id, SQLTransactionExecutor transactionExecutor, long createTime) {
            this.id = id;
            this.transactionExecutor = transactionExecutor;
            this.createTime = createTime;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public SQLTransactionExecutor getTransactionExecutor() {
            return transactionExecutor;
        }

        public void setTransactionExecutor(SQLTransactionExecutor transactionExecutor) {
            this.transactionExecutor = transactionExecutor;
        }

        public long getCreateTime() {
            return createTime;
        }

        public void setCreateTime(long createTime) {
            this.createTime = createTime;
        }
    }
}