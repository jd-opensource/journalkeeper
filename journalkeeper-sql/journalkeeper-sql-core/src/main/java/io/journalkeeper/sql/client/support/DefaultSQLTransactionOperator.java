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
package io.journalkeeper.sql.client.support;

import io.journalkeeper.sql.client.SQLClient;
import io.journalkeeper.sql.client.SQLTransactionOperator;
import io.journalkeeper.sql.client.helper.ParamHelper;
import io.journalkeeper.sql.exception.SQLException;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * DefaultSQLTransactionOperator
 * author: gaohaoxiang
 * date: 2019/8/7
 */
public class DefaultSQLTransactionOperator implements SQLTransactionOperator {

    private String id;
    private SQLClient client;

    public DefaultSQLTransactionOperator(String id, SQLClient client) {
        this.id = id;
        this.client = client;
    }

    @Override
    public String insert(String sql, Object... params) {
        try {
            return client.insert(id, sql, ParamHelper.toString(params)).get();
        } catch (Exception e) {
            throw convertException(e);
        }
    }

    @Override
    public int update(String sql, Object... params) {
        try {
            return client.update(id, sql, ParamHelper.toString(params)).get();
        } catch (Exception e) {
            throw convertException(e);
        }
    }

    @Override
    public int delete(String sql, Object... params) {
        try {
            return client.delete(id, sql, ParamHelper.toString(params)).get();
        } catch (Exception e) {
            throw convertException(e);
        }
    }

    @Override
    public List<Map<String, String>> query(String sql, Object... params) {
        try {
            return client.query(id, sql, ParamHelper.toString(params)).get();
        } catch (Exception e) {
            throw convertException(e);
        }
    }

    protected SQLException convertException(Throwable e) {
        if (e instanceof SQLException) {
            return (SQLException) e;
        } else if (e instanceof ExecutionException) {
            return convertException(((ExecutionException) e).getCause());
        }
        return new SQLException(e);
    }

    @Override
    public boolean commit() {
        client.commitTransaction(id);
        return true;
    }

    @Override
    public boolean rollback() {
        client.rollbackTransaction(id);
        return true;
    }
}