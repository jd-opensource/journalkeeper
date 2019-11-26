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
package io.journalkeeper.core.entry.internal;

import io.journalkeeper.core.exception.SerializeException;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * @author LiYue
 * Date: 2019-08-27
 */
public class UriSerializeSupport {
    public static void serializeUriList(ByteBuffer buffer, List<URI> config) {
        if(config.size() >= Short.MAX_VALUE) {
            throw new SerializeException(String.format("Size of config overflow! Max: %d, actual: %d. ",
                    Short.MAX_VALUE, config.size()));
        }
        buffer.putShort((short) config.size());
        for (URI uri : config) {
            serializerUri(buffer, uri);
        }
    }

    public static void serializerUri(ByteBuffer buffer, URI uri) {
        byte [] asciiBytes = uri.toASCIIString().getBytes(StandardCharsets.US_ASCII);
        if(asciiBytes.length >= Short.MAX_VALUE) {
            throw new SerializeException(String.format("URI length too large! Max: %d, actual: %d, uri: %s. ",
                    Short.MAX_VALUE, asciiBytes.length, uri));
        }
        buffer.putShort((short) asciiBytes.length);
        buffer.put(asciiBytes);
    }

    public static List<URI> parseUriList(ByteBuffer buffer) {
        int sizeOfList = buffer.getShort();
        List<URI> config = new ArrayList<>(sizeOfList);
        for (int i = 0; i < sizeOfList; i++) {
            URI uri = parseUri(buffer);
            config.add(uri);
        }
        return config;
    }

    public static URI parseUri(ByteBuffer buffer) {
        int length = buffer.getShort();
        byte [] asciiBytes = new byte[length];
        buffer.get(asciiBytes);
        return URI.create(new String(asciiBytes, StandardCharsets.US_ASCII));
    }

}
