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

import io.journalkeeper.sql.state.config.SQLConfigs;
import io.journalkeeper.utils.threads.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * SQLTransactionExecutorCleaner
 * author: gaohaoxiang
 * date: 2019/8/8
 */
public class SQLTransactionExecutorCleaner implements Runnable {

    protected static final Logger logger = LoggerFactory.getLogger(SQLTransactionExecutorCleaner.class);

    private SQLTransactionExecutorManager transactionExecutorManager;
    private int timeout;
    private int interval;
    private ScheduledExecutorService clearThread;

    public SQLTransactionExecutorCleaner(SQLTransactionExecutorManager transactionExecutorManager, Properties properties) {
        this.transactionExecutorManager = transactionExecutorManager;
        this.timeout = Integer.valueOf(properties.getProperty(SQLConfigs.TRANSACTION_TIMEOUT,
                String.valueOf(SQLConfigs.DEFAULT_TRANSACTION_TIMEOUT)));
        this.interval = Integer.valueOf(properties.getProperty(SQLConfigs.TRANSACTION_CLEAR_INTERVAL,
                String.valueOf(SQLConfigs.DEFAULT_TRANSACTION_CLEAR_INTERVAL)));
        this.clearThread = Executors.newScheduledThreadPool(1, new NamedThreadFactory("sql-transaction-cleaner"));
        this.clearThread.scheduleWithFixedDelay(this, interval, interval, TimeUnit.MILLISECONDS);
    }

    @Override
    public void run() {
        Map<String, SQLTransactionExecutorManager.SQLTransactionExecutorEntry> transactionExecutorEntryMap = transactionExecutorManager.getAll();
        for (Map.Entry<String, SQLTransactionExecutorManager.SQLTransactionExecutorEntry> entry : transactionExecutorEntryMap.entrySet()) {
            SQLTransactionExecutorManager.SQLTransactionExecutorEntry transactionExecutorEntry = entry.getValue();
            if (!isTimeout(transactionExecutorEntry)) {
                continue;
            }
            doClear(transactionExecutorEntry);
        }
    }

    protected boolean isTimeout(SQLTransactionExecutorManager.SQLTransactionExecutorEntry entry) {
        return System.currentTimeMillis() - entry.getCreateTime() > timeout;
    }

    protected void doClear(SQLTransactionExecutorManager.SQLTransactionExecutorEntry entry) {
        try {
            transactionExecutorManager.rollback(entry.getId());
            entry.getTransactionExecutor().rollback();
            logger.info("clear timeout transaction success, id: {}", entry.getId());
        } catch (Exception e) {
            logger.error("clear timeout transaction exception, id: {}", entry.getId(), e);
        }
    }
}