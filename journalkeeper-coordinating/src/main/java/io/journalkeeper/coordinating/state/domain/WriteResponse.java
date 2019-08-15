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
package io.journalkeeper.coordinating.state.domain;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * ReadResponse
 * author: gaohaoxiang
 *
 * date: 2019/5/30
 */
public class ReadResponse implements Serializable {

    private int code;
    private String msg;
    private byte[] value;
    private List<byte[]> values;

    public ReadResponse() {

    }

    public ReadResponse(int code) {
        this.code = code;
    }

    public ReadResponse(int code, byte[] value) {
        this.code = code;
        this.value = value;
    }

    public ReadResponse(int code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    public ReadResponse(int code, String msg, byte[] value) {
        this.code = code;
        this.msg = msg;
        this.value = value;
    }

    public ReadResponse(int code, String msg, List<byte[]> values) {
        this.code = code;
        this.msg = msg;
        this.values = values;
    }

    public ReadResponse(int code, List<byte[]> values) {
        this.code = code;
        this.values = values;
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

    public void setValue(byte[] value) {
        this.value = value;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValues(List<byte[]> values) {
        this.values = values;
    }

    public List<byte[]> getValues() {
        return values;
    }

    @Override
    public String toString() {
        return "ReadResponse{" +
                "code=" + code +
                ", msg='" + msg + '\'' +
                ", value=" + Arrays.toString(value) +
                ", values=" + values +
                '}';
    }
}