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
package com.jd.journalkeeper.examples.kv;

import java.util.List;

/**
 * @author liyue25
 * Date: 2019-04-03
 */
public class KvResult {
    /**
     * GET返回的值
     */
    private String value;
    /**
     * LIST_KEYS 返回的keys
     */
    private List<String> keys;

    public KvResult() {}

    public KvResult(String value, List<String> keys) {
        this.value = value;
        this.keys = keys;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public void setKeys(List<String> keys) {
        this.keys = keys;
    }

    public String getValue() {
        return value;
    }

    public List<String> getKeys() {
        return keys;
    }
}
