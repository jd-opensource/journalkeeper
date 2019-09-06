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
import io.journalkeeper.sql.state.jdbc.utils.DBUtils;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * JDBCExecutor
 * author: gaohaoxiang
 * date: 2019/8/1
 */
public class JDBCExecutor {

    public String insert(Connection connection, String sql, Object... params) {
        try {
            return DBUtils.insert(connection, sql, params);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int update(Connection connection, String sql, Object... params) {
        try {
            return DBUtils.update(connection, sql, params);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int delete(Connection connection, String sql, Object... params) {
        try {
            return DBUtils.delete(connection, sql, params);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public ResultSet query(Connection connection, String sql, Object... params) {
        try {
            List<Map<String, String>> rows = DBUtils.query(connection, sql, params);
            return new ResultSet(rows);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }
}