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
package io.journalkeeper.sql.client;

import io.journalkeeper.sql.client.domain.OperationTypes;

/**
 * SQLEvent
 * author: gaohaoxiang
 *
 * date: 2019/6/11
 */
public class SQLEvent {

    private OperationTypes type;
    private String sql;
    private String[] params;

    public SQLEvent() {

    }

    public SQLEvent(OperationTypes type, String sql) {
        this.type = type;
        this.sql = sql;
    }

    public SQLEvent(OperationTypes type, String sql, String[] params) {
        this.type = type;
        this.sql = sql;
        this.params = params;
    }

    public OperationTypes getType() {
        return type;
    }

    public void setType(OperationTypes type) {
        this.type = type;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String[] getParams() {
        return params;
    }

    public void setParams(String[] params) {
        this.params = params;
    }
}