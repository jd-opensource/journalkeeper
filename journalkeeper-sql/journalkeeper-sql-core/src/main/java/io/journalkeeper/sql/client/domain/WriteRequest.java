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

import java.util.Arrays;

/**
 * WriteRequest
 * author: gaohaoxiang
 * date: 2019/5/30
 */
public class WriteRequest extends StateRequest {

    private String id;
    private String sql;
    private String[] params;

    public WriteRequest() {

    }

    public WriteRequest(int type) {
        super(type);
    }

    public WriteRequest(int type, String id) {
        super(type);
        this.id = id;
    }

    public WriteRequest(int type, String id, String sql, String[] params) {
        super(type);
        this.id = id;
        this.sql = sql;
        this.params = params;
    }

    public WriteRequest(int type, String sql, String[] params) {
        super(type);
        this.sql = sql;
        this.params = params;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
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

    @Override
    public String toString() {
        return "WriteRequest{" +
                "id='" + id + '\'' +
                "type='" + getType() + '\'' +
                ", sql='" + sql + '\'' +
                ", params=" + Arrays.toString(params) +
                '}';
    }
}