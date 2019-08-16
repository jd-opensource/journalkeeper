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

import java.io.Serializable;

/**
 * ReadResponse
 * author: gaohaoxiang
 * date: 2019/5/30
 */
public class ReadResponse implements Serializable {

    private int code;
    private String msg;
    private ResultSet resultSet;

    public ReadResponse() {

    }

    public ReadResponse(int code) {
        this.code = code;
    }

    public ReadResponse(int code, ResultSet resultSet) {
        this.code = code;
        this.resultSet = resultSet;
    }

    public ReadResponse(int code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public void setResultSet(ResultSet resultSet) {
        this.resultSet = resultSet;
    }

    public ResultSet getResultSet() {
        return resultSet;
    }
}