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

import io.journalkeeper.sql.client.BatchSQLOperator;
import io.journalkeeper.sql.client.SQLClient;
import io.journalkeeper.sql.client.helper.ParamHelper;
import io.journalkeeper.sql.exception.SQLException;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * DefaultBatchSQLOperator
 * author: gaohaoxiang
 * date: 2019/8/7
 */
public class DefaultBatchSQLOperator implements BatchSQLOperator {

    private SQLClient client;
    private int timeout;
    private List<String> sqlList = new LinkedList<>();
    private List<List<Object>> paramList = new LinkedList<>();

    public DefaultBatchSQLOperator(SQLClient client, int timeout) {
        this.client = client;
        this.timeout = timeout;
    }

    @Override
    public void insert(String sql, Object... params) {
        sqlList.add(sql);
        paramList.add(ParamHelper.toList(params));
    }

    @Override
    public void update(String sql, Object... params) {
        sqlList.add(sql);
        paramList.add(ParamHelper.toList(params));
    }

    @Override
    public void delete(String sql, Object... params) {
        sqlList.add(sql);
        paramList.add(ParamHelper.toList(params));
    }

    @Override
    public List<Object> commit() {
        if (sqlList == null || sqlList.isEmpty()) {
            return null;
        }
        try {
            return client.batch(sqlList, paramList).get(timeout, TimeUnit.MILLISECONDS);
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