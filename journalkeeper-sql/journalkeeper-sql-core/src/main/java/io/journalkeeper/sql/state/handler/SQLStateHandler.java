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
package io.journalkeeper.sql.state.handler;

import io.journalkeeper.metric.JMetric;
import io.journalkeeper.metric.JMetricFactory;
import io.journalkeeper.metric.JMetricFactoryManager;
import io.journalkeeper.sql.client.domain.Codes;
import io.journalkeeper.sql.client.domain.ReadRequest;
import io.journalkeeper.sql.client.domain.ReadResponse;
import io.journalkeeper.sql.client.domain.WriteRequest;
import io.journalkeeper.sql.client.domain.WriteResponse;
import io.journalkeeper.sql.state.SQLExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * SQLStateHandler
 * author: gaohaoxiang
 * date: 2019/8/1
 */
public class SQLStateHandler {

    protected static final Logger logger = LoggerFactory.getLogger(SQLStateHandler.class);
    private final ConcurrentMap<String, JMetric> metricMap = new ConcurrentHashMap<>();
    private Properties properties;
    private SQLExecutor sqlExecutor;
    private SQLStateReadHandler readHandler;
    private SQLStateWriteHandler writeHandler;
    private JMetricFactory metricFactory;

    public SQLStateHandler(Properties properties, SQLExecutor sqlExecutor) {
        this.properties = properties;
        this.sqlExecutor = sqlExecutor;
        this.readHandler = new SQLStateReadHandler(properties, sqlExecutor);
        this.writeHandler = new SQLStateWriteHandler(properties, sqlExecutor);
        this.metricFactory = JMetricFactoryManager.getFactory();
    }

    public WriteResponse handleWrite(WriteRequest request) {
        List<JMetric> metricList = new LinkedList<>();
        if (request.getSqlList() == null) {
            metricList.add(getMetric(request.getSql()));
        } else {
            for (String sql : request.getSqlList()) {
                metricList.add(getMetric(sql));
            }
        }

        for (JMetric metric : metricList) {
            metric.start();
        }

        try {
            return writeHandler.handle(request);
        } catch (Throwable e) {
            logger.error("sql write exception, request: {}", request, e);
            return new WriteResponse(Codes.ERROR.getCode(), e.toString());
        } finally {
            for (JMetric metric : metricList) {
                metric.end();
            }
        }
    }

    public ReadResponse handleRead(ReadRequest request) {
        JMetric metric = getMetric(request.getSql());
        metric.start();

        try {
            return readHandler.handle(request);
        } catch (Throwable e) {
            logger.error("sql read exception, request: {}", request, e);
            return new ReadResponse(Codes.ERROR.getCode(), e.toString());
        } finally {
            metric.end();
        }
    }

    protected JMetric getMetric(String sql) {
        JMetric metric = metricMap.get(sql);
        if (metric != null) {
            return metric;
        }

        metric = metricFactory.create(sql);
        JMetric oldMetric = metricMap.putIfAbsent(sql, metric);

        if (oldMetric != null) {
            return oldMetric;
        }
        return metric;
    }
}