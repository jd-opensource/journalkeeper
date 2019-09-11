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
package io.journalkeeper.core.entry.reserved;

import io.journalkeeper.base.Serializer;

import java.nio.ByteBuffer;

/**
 * @author LiYue
 * Date: 2019-05-09
 *
 * Type:                                    1 byte
 * preferred leader URI String
 *  Length of the URI String in bytes:    2 bytes
 *  URI String in bytes                   variable length
 * ...
 *
 */
public class SetPreferredLeaderEntrySerializer implements Serializer<SetPreferredLeaderEntry> {
    private int sizeOf(SetPreferredLeaderEntry entry) {
        return Byte.BYTES +  // Type:                              1 byte
                Short.BYTES  +  // Length of the URI String in bytes: 2 bytes
                entry.getPreferredLeader().toASCIIString().length(); // URI String in bytes: variable length
    }

    @Override
    public byte[] serialize(SetPreferredLeaderEntry entry) {
        byte [] bytes = new byte[sizeOf(entry)];
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        UriSerializeSupport.serializerUri(buffer, entry.getPreferredLeader());
        return bytes;
    }


    @Override
    public SetPreferredLeaderEntry parse(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes, Byte.BYTES, bytes.length - Byte.BYTES);

        return new SetPreferredLeaderEntry(UriSerializeSupport.parseUri(buffer));
    }

}
