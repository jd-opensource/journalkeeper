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
import java.util.List;

/**
 * WriteResponse
 * author: gaohaoxiang
 * date: 2019/5/30
 */
public class WriteResponse implements Serializable {

    private int code;
    private String msg;
    private Object result;
    private List<Object> resultList;

    public WriteResponse() {

    }

    public WriteResponse(int code) {
        this.code = code;
    }

    public WriteResponse(int code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    public WriteResponse(int code, Object result, String msg) {
        this.code = code;
        this.result = result;
        this.msg = msg;
    }

    public WriteResponse(int code, Object result) {
        this.code = code;
        this.result = result;
    }

    public WriteResponse(int code, List<Object> resultList, String msg) {
        this.code = code;
        this.resultList = resultList;
        this.msg = msg;
    }

    public WriteResponse(int code, List<Object> resultList) {
        this.code = code;
        this.resultList = resultList;
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

    public void setResult(Object result) {
        this.result = result;
    }

    public Object getResult() {
        return result;
    }

    public List<Object> getResultList() {
        return resultList;
    }

    public void setResultList(List<Object> resultList) {
        this.resultList = resultList;
    }

    @Override
    public String toString() {
        return "WriteResponse{" +
                "code=" + code +
                ", msg='" + msg + '\'' +
                ", result='" + result + '\'' +
                ", resultList='" + resultList + '\'' +
                '}';
    }
}