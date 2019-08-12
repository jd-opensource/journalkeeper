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
package io.journalkeeper.sql.state.jdbc;

import io.journalkeeper.sql.exception.SQLException;
import io.journalkeeper.sql.state.SQLExecutor;
import io.journalkeeper.sql.state.SQLTransactionExecutor;
import io.journalkeeper.sql.state.support.SQLTransactionExecutorManager;

import javax.sql.DataSource;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * JDBCSQLExecutor
 * author: gaohaoxiang
 * date: 2019/8/1
 */
public class JDBCSQLExecutor implements SQLExecutor {

    private DataSourceFactory dataSourceFactory;
    private JDBCExecutor executor;

    private DataSource dataSource;
    private SQLTransactionExecutorManager transactionExecutorManager;

    public JDBCSQLExecutor(Path path, Properties properties, DataSourceFactory dataSourceFactory,
                           JDBCExecutor executor, SQLTransactionExecutorManager transactionExecutorManager) {
        this.dataSourceFactory = dataSourceFactory;
        this.executor = executor;
        this.dataSource = dataSourceFactory.createDataSource(path, properties);
        this.transactionExecutorManager = transactionExecutorManager;
    }

    @Override
    public String insert(String sql, Object... params) {
        Connection connection = getConnection();
        try {
            return executor.insert(connection, sql, params);
        } finally {
            releaseConnection(connection);
        }
    }

    @Override
    public int update(String sql, Object... params) {
        Connection connection = getConnection();
        try {
            return executor.update(connection, sql, params);
        } finally {
            releaseConnection(connection);
        }
    }

    @Override
    public int delete(String sql, Object... params) {
        Connection connection = getConnection();
        try {
            return executor.delete(connection, sql, params);
        } finally {
            releaseConnection(connection);
        }
    }

    @Override
    public List<Map<String, String>> query(String sql, Object... params) {
        Connection connection = getConnection();
        try {
            return executor.query(connection, sql, params);
        } finally {
            releaseConnection(connection);
        }
    }

    @Override
    public SQLTransactionExecutor beginTransaction(String id) {
        Connection connection = getTransactionConnection();
        JDBCSQLTransactionExecutor transactionExecutor = new JDBCSQLTransactionExecutor(id, connection, executor, transactionExecutorManager);
        try {
            transactionExecutorManager.begin(id, transactionExecutor);
        } catch (Exception e) {
            releaseConnection(connection);
            throw e;
        }
        return transactionExecutor;
    }

    @Override
    public SQLTransactionExecutor getTransaction(String id) {
        return transactionExecutorManager.get(id);
    }

    protected Connection getTransactionConnection() {
        try {
            Connection connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            return connection;
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    protected Connection getConnection() {
        try {
            return dataSource.getConnection();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    protected void releaseConnection(Connection connection) {
        try {
            connection.close();
        } catch (java.sql.SQLException e) {
            throw new SQLException();
        }
    }
}