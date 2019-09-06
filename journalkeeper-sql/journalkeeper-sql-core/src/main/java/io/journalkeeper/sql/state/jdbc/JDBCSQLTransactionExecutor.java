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

import io.journalkeeper.sql.client.domain.ResultSet;
import io.journalkeeper.sql.exception.SQLException;
import io.journalkeeper.sql.state.SQLTransactionExecutor;
import io.journalkeeper.sql.state.support.SQLTransactionExecutorManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;

/**
 * JDBCSQLTransactionExecutor
 * author: gaohaoxiang
 * date: 2019/8/1
 */
public class JDBCSQLTransactionExecutor implements SQLTransactionExecutor {

    protected static final Logger logger = LoggerFactory.getLogger(JDBCSQLTransactionExecutor.class);

    private String id;
    private Connection connection;
    private JDBCExecutor executor;
    private SQLTransactionExecutorManager transactionExecutorManager;

    public JDBCSQLTransactionExecutor(String id, Connection connection, JDBCExecutor executor,
                                      SQLTransactionExecutorManager transactionExecutorManager) {
        this.id = id;
        this.connection = connection;
        this.executor = executor;
        this.transactionExecutorManager = transactionExecutorManager;
    }

    @Override
    public String insert(String sql, Object... params) {
        return executor.insert(connection, sql, params);
    }

    @Override
    public int update(String sql, Object... params) {
        return executor.update(connection, sql, params);
    }

    @Override
    public int delete(String sql, Object... params) {
        return executor.delete(connection, sql, params);
    }

    @Override
    public ResultSet query(String sql, Object... params) {
        return executor.query(connection, sql, params);
    }

    @Override
    public boolean commit() {
        try {
            connection.commit();
            return true;
        } catch (java.sql.SQLException e) {
            logger.error("commit transaction exception, id: {}", id, e);
            throw new SQLException(e);
        } finally {
            try {
                connection.close();
            } catch (java.sql.SQLException e) {
                logger.error("close transaction exception, id: {}", id, e);
            }
            transactionExecutorManager.commit(id);
        }
    }

    @Override
    public boolean rollback() {
        try {
            connection.rollback();
            return true;
        } catch (java.sql.SQLException e) {
            logger.error("rollback transaction exception, id: {}", id, e);
            throw new SQLException(e);
        } finally {
            try {
                connection.close();
            } catch (java.sql.SQLException e) {
                logger.error("close transaction exception, id: {}", id, e);
            }
            transactionExecutorManager.rollback(id);
        }
    }
}