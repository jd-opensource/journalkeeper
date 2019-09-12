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
import io.journalkeeper.sql.client.SQLOperator;
import io.journalkeeper.sql.client.SQLTransactionOperator;
import io.journalkeeper.sql.client.domain.ResultSet;
import io.journalkeeper.sql.client.helper.ParamHelper;
import io.journalkeeper.sql.exception.SQLException;

import java.util.concurrent.ExecutionException;

/**
 * DefaultSQLOperator
 * author: gaohaoxiang
 * date: 2019/8/7
 */
public class DefaultSQLOperator implements SQLOperator {

    private SQLClient client;
    private TransactionIdGenerator transactionIdGenerator;

    public DefaultSQLOperator(SQLClient client) {
        this.client = client;
        this.transactionIdGenerator = new TransactionIdGenerator();
    }

    @Override
    public String insert(String sql, Object... params) {
        try {
            return client.insert(sql, ParamHelper.toString(params)).get();
        } catch (Exception e) {
            throw convertException(e);
        }
    }

    @Override
    public int update(String sql, Object... params) {
        try {
            return client.update(sql, ParamHelper.toString(params)).get();
        } catch (Exception e) {
            throw convertException(e);
        }
    }

    @Override
    public int delete(String sql, Object... params) {
        try {
            return client.delete(sql, ParamHelper.toString(params)).get();
        } catch (Exception e) {
            throw convertException(e);
        }
    }

    @Override
    public ResultSet query(String sql, Object... params) {
        try {
            return client.query(sql, ParamHelper.toString(params)).get();
        } catch (Exception e) {
            throw convertException(e);
        }
    }

    @Override
    public SQLTransactionOperator beginTransaction() {
        try {
            String id = transactionIdGenerator.generate();
            client.beginTransaction(id).get();
            return new DefaultSQLTransactionOperator(id, client);
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
}