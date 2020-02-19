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
package io.journalkeeper.sql.client.domain;

import java.util.List;

/**
 * WriteRequest
 * author: gaohaoxiang
 * date: 2019/5/30
 */
public class WriteRequest extends StateRequest {

    private String sql;
    private List<Object> params;
    private List<String> sqlList;
    private List<List<Object>> paramList;

    public WriteRequest() {

    }

    public WriteRequest(int type) {
        super(type);
    }

    public WriteRequest(int type, String sql, List<Object> params) {
        super(type);
        this.sql = sql;
        this.params = params;
    }

    public WriteRequest(int type, List<String> sqlList, List<List<Object>> paramList) {
        super(type);
        this.sqlList = sqlList;
        this.paramList = paramList;
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

    public List<String> getSqlList() {
        return sqlList;
    }

    public void setSqlList(List<String> sqlList) {
        this.sqlList = sqlList;
    }

    public List<List<Object>> getParamList() {
        return paramList;
    }

    public void setParamList(List<List<Object>> paramList) {
        this.paramList = paramList;
    }

    @Override
    public String toString() {
        return "WriteRequest{" +
                "type='" + getType() + '\'' +
                ", sql='" + sql + '\'' +
                ", params=" + params +
                ", sqlList='" + sqlList + '\'' +
                ", paramList=" + paramList +
                '}';
    }
}