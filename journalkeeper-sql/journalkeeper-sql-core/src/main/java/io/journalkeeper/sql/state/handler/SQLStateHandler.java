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
package io.journalkeeper.sql.state.handler;

import io.journalkeeper.metric.JMetric;
import io.journalkeeper.metric.JMetricFactory;
import io.journalkeeper.metric.JMetricFactoryManager;
import io.journalkeeper.metric.JMetricSupport;
import io.journalkeeper.sql.client.domain.Codes;
import io.journalkeeper.sql.client.domain.OperationTypes;
import io.journalkeeper.sql.client.domain.ReadRequest;
import io.journalkeeper.sql.client.domain.ReadResponse;
import io.journalkeeper.sql.client.domain.WriteRequest;
import io.journalkeeper.sql.client.domain.WriteResponse;
import io.journalkeeper.sql.client.support.TransactionIdGenerator;
import io.journalkeeper.sql.state.SQLExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * SQLStateHandler
 * author: gaohaoxiang
 * date: 2019/8/1
 */
public class SQLStateHandler {

    protected static final Logger logger = LoggerFactory.getLogger(SQLStateHandler.class);

    private Properties properties;
    private SQLExecutor sqlExecutor;
    private TransactionIdGenerator transactionIdGenerator;
    private SQLStateReadHandler readHandler;
    private SQLStateWriteHandler writeHandler;

    private JMetricFactory metricFactory;
    private final ConcurrentMap<String, JMetric> metricMap = new ConcurrentHashMap<>();

    public SQLStateHandler(Properties properties, SQLExecutor sqlExecutor) {
        this.properties = properties;
        this.sqlExecutor = sqlExecutor;
        this.transactionIdGenerator = new TransactionIdGenerator();
        this.readHandler = new SQLStateReadHandler(properties, sqlExecutor);
        this.writeHandler = new SQLStateWriteHandler(properties, sqlExecutor, transactionIdGenerator);
        this.metricFactory = JMetricFactoryManager.getFactory();

        // TODO 临时代码
        Executors.newScheduledThreadPool(1).scheduleWithFixedDelay(() -> {
            for (Map.Entry<String, JMetric> entry : metricMap.entrySet()) {
                logger.info("sql: {}, metric: {}", entry.getKey(), JMetricSupport.format(entry.getValue().get(), TimeUnit.MILLISECONDS));
            }
        }, Integer.valueOf(properties.getProperty("sql.metric.interval", String.valueOf(1000 * 10))),
                Integer.valueOf(properties.getProperty("sql.metric.interval", String.valueOf(1000 * 10))),
                TimeUnit.MILLISECONDS);
    }

    public WriteResponse handleWrite(WriteRequest request) {
        JMetric jMetric = null;

        if (!(request.getType() == OperationTypes.TRANSACTION_BEGIN.getType() ||
                request.getType() == OperationTypes.TRANSACTION_COMMIT.getType() ||
                request.getType() == OperationTypes.TRANSACTION_ROLLBACK.getType())) {

            jMetric = getJMetric(request.getSql(), request.getParams());
            jMetric.start();
        }

        try {
            return writeHandler.handle(request);
        } catch (Exception e) {
            logger.error("sql write exception, request: {}", request, e);
            return new WriteResponse(Codes.ERROR.getCode(), e.toString());
        } finally {
            if (jMetric != null) {
                jMetric.end();
            }
        }
    }

    public ReadResponse handleRead(ReadRequest request) {
        JMetric jMetric = getJMetric(request.getSql(), request.getParams());
        jMetric.start();

        try {
            return readHandler.handle(request);
        } catch (Exception e) {
            logger.error("sql read exception, request: {}", request, e);
            return new ReadResponse(Codes.ERROR.getCode(), e.toString());
        } finally {
            if (jMetric != null) {
                jMetric.end();
            }
        }
    }

    protected JMetric getJMetric(String sql, Object... params) {
        JMetric jMetric = metricMap.get(sql);
        if (jMetric != null) {
            return jMetric;
        }

        jMetric = metricFactory.create(sql);
        JMetric oldJMetric = metricMap.putIfAbsent(sql, jMetric);

        if (oldJMetric != null) {
            return oldJMetric;
        }
        return jMetric;
    }
}