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
package io.journalkeeper.sql.client.domain;

import java.util.List;

/**
 * ReadRequest
 * author: gaohaoxiang
 * date: 2019/5/30
 */
public class ReadRequest extends StateRequest {

    private String sql;
    private List<Object> params;

    public ReadRequest() {

    }

    public ReadRequest(int type, String sql, List<Object> params) {
        super(type);
        this.sql = sql;
        this.params = params;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public List<Object> getParams() {
        return params;
    }

    public void setParams(List<Object> params) {
        this.params = params;
    }

    @Override
    public String toString() {
        return "ReadRequest{" +
                "type='" + getType() + '\'' +
                ", sql='" + sql + '\'' +
                ", params=" + params +
                '}';
    }
}