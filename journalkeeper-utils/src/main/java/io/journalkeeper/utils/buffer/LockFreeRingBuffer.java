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
package io.journalkeeper.utils.buffer;

/**
 * Original Author: GeniusJkq@blog.csdn.net
 * 版权声明：本文为博主原创文章，遵循 CC 4.0 BY-SA 版权协议，转载请附上原文出处链接和本声明。
 * 本文链接：https://blog.csdn.net/jkqwd1222/article/details/82194305
 */

import java.lang.reflect.Array;
import java.util.Arrays;

/**
 * @author LiYue
 * Date: 2019-09-19
 */
public class LockFreeRingBuffer<T> {


    private final static int DEFAULT_SIZE = 1024;
    private final T[] buffer;
    private final int bufferSize;
    private final Class<T> type;
    private int head = 0;
    private int tail = 0;

    public LockFreeRingBuffer(Class<T> type) {
        this(type, DEFAULT_SIZE);
    }

    public LockFreeRingBuffer(Class<T> type, int initSize) {
        this.type = type;
        this.bufferSize = initSize;
        this.buffer = createArray(bufferSize);
    }

    public boolean empty() {
        return head == tail;
    }

    public boolean full() {
        return (tail + 1) % bufferSize == head;
    }

    public void clear() {
        Arrays.fill(buffer, null);
        this.head = 0;
        this.tail = 0;
    }

    public boolean put(T v) {
        if (full()) {
            return false;
        }
        buffer[tail] = v;
        tail = (tail + 1) % bufferSize;
        return true;
    }

    public T remove() {
        if (empty()) {
            return null;
        }
        T result = buffer[head];
        head = (head + 1) % bufferSize;
        return result;
    }

    public T get() {
        if (empty()) {
            return null;
        }
        return buffer[head];
    }

    public int size() {
        int copyTail = tail;
        return head < copyTail ? copyTail - head : bufferSize - head + copyTail;
    }

    public T[] removeAll() {
        if (empty()) {
            return createArray(0);
        }
        int copyTail = tail;
        int cnt = head < copyTail ? copyTail - head : bufferSize - head + copyTail;
        T[] result = createArray(cnt);
        if (head < copyTail) {
            for (int i = head; i < copyTail; i++) {
                result[i - head] = buffer[i];
            }
        } else {
            for (int i = head; i < bufferSize; i++) {
                result[i - head] = buffer[i];
            }
            for (int i = 0; i < copyTail; i++) {
                result[bufferSize - head + i] = buffer[i];
            }
        }
        head = copyTail;
        return result;
    }

    @SuppressWarnings("unchecked")
    private T[] createArray(int size) {
        return (T[]) Array.newInstance(type, size);
    }

}
