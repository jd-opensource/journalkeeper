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
package io.journalkeeper.coordinating.client;

import io.journalkeeper.coordinating.state.domain.StateTypes;
import io.journalkeeper.utils.event.Event;
import io.journalkeeper.utils.event.EventType;
import io.journalkeeper.utils.event.EventWatcher;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.Objects;

/**
 * EventWatcherAdapter
 * author: gaohaoxiang
 *
 * date: 2019/6/11
 */
public class EventWatcherAdapter implements EventWatcher {

    private byte[] key;
    private CoordinatingEventListener listener;

    public EventWatcherAdapter(CoordinatingEventListener listener) {
        this.listener = listener;
    }

    public EventWatcherAdapter(byte[] key, CoordinatingEventListener listener) {
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

        CoordinatingEvent coordinatingEvent = null;
        StateTypes type = StateTypes.valueOf(Integer.valueOf(eventData.get("type")));
        String key = eventData.get("key");
        String value = eventData.get("value");

        switch (type) {
            case SET:
            case COMPARE_AND_SET: {
                coordinatingEvent = new CoordinatingEvent(type, key.getBytes(Charset.forName("UTF-8")), value.getBytes(Charset.forName("UTF-8")));
                break;
            }
            case REMOVE: {
                coordinatingEvent = new CoordinatingEvent(type, key.getBytes(Charset.forName("UTF-8")));
                break;
            }
        }

        if (coordinatingEvent != null && (this.key == null || Objects.deepEquals(this.key, coordinatingEvent.getKey()))) {
            listener.onEvent(coordinatingEvent);
        }
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

    public CoordinatingEventListener getListener() {
        return listener;
    }
}