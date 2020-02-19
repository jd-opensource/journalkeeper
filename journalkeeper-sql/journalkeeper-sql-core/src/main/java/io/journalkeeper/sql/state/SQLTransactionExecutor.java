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
package io.journalkeeper.sql.state;

import io.journalkeeper.sql.client.domain.ResultSet;

import java.util.List;

/**
 * SQLTransactionExecutor
 * author: gaohaoxiang
 * date: 2019/8/1
 */
public interface SQLTransactionExecutor {

    String insert(String sql, List<Object> params);

    int update(String sql, List<Object> params);

    int delete(String sql, List<Object> params);

    ResultSet query(String sql, List<Object> params);

    boolean commit();

    boolean rollback();
}