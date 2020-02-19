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
package io.journalkeeper.core.api;

import java.util.HashMap;
import java.util.Map;

/**
 * @author LiYue
 * Date: 2019/11/20
 */
public class StateResult {
    private final byte [] userResult;
    private final Map<String, String> eventData;
    private long lastApplied;

    public StateResult(byte [] userResult) {
        this(userResult, new HashMap<>());
    }

    public StateResult(byte [] userResult, Map<String, String> eventData) {
        this.userResult = userResult;
        this.eventData = eventData == null ? new HashMap<>() : eventData;
    }

    public byte [] getUserResult() {
        return userResult;
    }

    public Map<String, String> getEventData() {
        return eventData;
    }

    public String putEventData(String key, String value) {
        return eventData.put(key, value);
    }

    public long getLastApplied() {
        return lastApplied;
    }

    public void setLastApplied(long lastApplied) {
        this.lastApplied = lastApplied;
    }
}
