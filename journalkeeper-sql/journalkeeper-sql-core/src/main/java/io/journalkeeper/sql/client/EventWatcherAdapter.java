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
package io.journalkeeper.sql.client;

import io.journalkeeper.sql.client.domain.OperationTypes;
import io.journalkeeper.utils.event.Event;
import io.journalkeeper.utils.event.EventType;
import io.journalkeeper.utils.event.EventWatcher;

import java.util.Map;
import java.util.Objects;

/**
 * EventWatcherAdapter
 * author: gaohaoxiang
 * date: 2019/6/11
 */
// TODO 适配
public class EventWatcherAdapter implements EventWatcher {

    private byte[] key;
    private SQLEventListener listener;

    public EventWatcherAdapter(SQLEventListener listener) {
        this.listener = listener;
    }

    public EventWatcherAdapter(byte[] key, SQLEventListener listener) {
        this.key = key;
        this.listener = listener;
    }

    @Override
    public void onEvent(Event event) {
        if (event.getEventType() != EventType.ON_STATE_CHANGE) {
            return;
        }

        Map<String, String> eventData = event.getEventData();
        if (eventData == null || eventData.isEmpty()) {
            return;
        }

        SQLEvent SQLEvent = null;
        OperationTypes type = OperationTypes.valueOf(Integer.valueOf(eventData.get("type")));
        String key = eventData.get("key");
        String value = eventData.get("value");

        switch (type) {
//            case SET:
//            case COMPARE_AND_SET: {
//                SQLEvent = new SQLEvent(type, key.getBytes(Charset.forName("UTF-8")), value.getBytes(Charset.forName("UTF-8")));
//                break;
//            }
//            case REMOVE: {
//                SQLEvent = new SQLEvent(type, key.getBytes(Charset.forName("UTF-8")));
//                break;
//            }
        }

//        if (SQLEvent != null && (this.key == null || Objects.deepEquals(this.key, SQLEvent.getKey()))) {
//            listener.onEvent(SQLEvent);
//        }
    }

    @Override
    public int hashCode() {
        return listener.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof EventWatcherAdapter)) {
            return false;
        }

        return ((EventWatcherAdapter) obj).getListener().equals(listener) &&
                (key == null && ((EventWatcherAdapter) obj).getKey() == null || Objects.deepEquals(((EventWatcherAdapter) obj).getKey(), key));
    }

    public byte[] getKey() {
        return key;
    }

    public SQLEventListener getListener() {
        return listener;
    }
}