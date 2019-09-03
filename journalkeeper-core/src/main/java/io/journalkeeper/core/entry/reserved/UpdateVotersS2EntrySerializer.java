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

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Stream;

import static io.journalkeeper.core.entry.reserved.UriSerializeSupport.parseUriList;
import static io.journalkeeper.core.entry.reserved.UriSerializeSupport.serializeUriList;

/**
 * @author LiYue
 * Date: 2019-05-09
 *
 * Type:                                    1 byte
 * Size of the Old Config List:             2 bytes
 *  URI String
 *    Length of the URI String in bytes:    2 bytes
 *    URI String in bytes                   variable length
 *  URI String
 *  URI String
 *  ...
 * Size of the New Config List:             2 bytes
 *  URI String
 *  URI String
 *  URI String
 * ...
 *
 */
public class UpdateVotersS2EntrySerializer implements Serializer<UpdateVotersS2Entry> {
    private int sizeOf(UpdateVotersS2Entry entry) {
        return Byte.BYTES +  // Type:                              1 byte
                Short.BYTES * 2 +  // Size of the Old Config List: 2 bytes
                Short.BYTES * 2 +  // Size of the New Config List: 2 bytes
                Stream.concat(entry.getConfigNew().stream(), entry.getConfigOld().stream())
                        .map(URI::toASCIIString)
                        .map(s -> s.getBytes(StandardCharsets.US_ASCII))
                        .mapToInt(b -> b.length + Short.BYTES)
                        .sum();
    }

    @Override
    public byte[] serialize(UpdateVotersS2Entry entry) {
        byte [] bytes = new byte[sizeOf(entry)];
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.put((byte) entry.getType());

        List<URI> config = entry.getConfigOld();
        serializeUriList(buffer, config);
        config = entry.getConfigNew();
        serializeUriList(buffer, config);
        return bytes;
    }


    @Override
    public UpdateVotersS2Entry parse(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes, Byte.BYTES, bytes.length - Byte.BYTES);

        List<URI> configOld = parseUriList(buffer);
        List<URI> configNew = parseUriList(buffer);

        return new UpdateVotersS2Entry(configOld, configNew);
    }

}
