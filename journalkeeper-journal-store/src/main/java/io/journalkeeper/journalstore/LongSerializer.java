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
package io.journalkeeper.journalstore;

import io.journalkeeper.base.Serializer;

import java.nio.ByteBuffer;

/**
 * @author LiYue
 * Date: 2019-08-12
 */
public class LongSerializer implements Serializer<Long> {
    @Override
    public byte[] serialize(Long entry) {
        byte[] bytes = new byte[Long.BYTES];
        if (null == entry) {
            entry = -1L;
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.putLong(entry);
        return bytes;
    }

    @Override
    public Long parse(byte[] bytes) {
        if (null == bytes || bytes.length < Long.BYTES) {
            return null;
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        Long value = buffer.getLong();
        if (value < 0) value = null;
        return value;
    }
}
