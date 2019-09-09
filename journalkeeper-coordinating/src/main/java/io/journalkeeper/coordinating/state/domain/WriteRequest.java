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

import java.util.Arrays;

/**
 * WriteRequest
 * author: gaohaoxiang
 *
 * date: 2019/5/30
 */
public class WriteRequest extends StateRequest {

    private byte[] key;
    private byte[] expect;
    private byte[] value;

    public WriteRequest() {

    }

    public WriteRequest(int type, byte[] key) {
        super(type);
        this.key = key;
    }

    public WriteRequest(int type, byte[] key, byte[] value) {
        super(type);
        this.key = key;
        this.value = value;
    }

    public WriteRequest(int type, byte[] key, byte[] expect, byte[] value) {
        super(type);
        this.key = key;
        this.expect = expect;
        this.value = value;
    }

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public void setExpect(byte[] expect) {
        this.expect = expect;
    }

    public byte[] getExpect() {
        return expect;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "WriteRequest{" +
                "type=" + getType() +
                "key=" + Arrays.toString(key) +
                ", expect=" + Arrays.toString(expect) +
                ", value=" + Arrays.toString(value) +
                '}';
    }
}