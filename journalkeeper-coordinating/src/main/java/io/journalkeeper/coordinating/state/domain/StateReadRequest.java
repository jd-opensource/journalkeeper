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
import java.util.List;

/**
 * StateReadRequest
 * author: gaohaoxiang
 *
 * date: 2019/5/30
 */
public class StateReadRequest extends StateRequest {

    private byte[] key;
    private List<byte[]> keys;

    public StateReadRequest() {

    }

    public StateReadRequest(int type, byte[] key) {
        super(type);
        this.key = key;
    }

    public StateReadRequest(int type, List<byte[]> keys) {
        super(type);
        this.keys = keys;
    }

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public void setKeys(List<byte[]> keys) {
        this.keys = keys;
    }

    public List<byte[]> getKeys() {
        return keys;
    }

    @Override
    public String toString() {
        return "StateReadRequest{" +
                "type=" + getType() +
                "key=" + Arrays.toString(key) +
                ", keys=" + keys +
                '}';
    }
}