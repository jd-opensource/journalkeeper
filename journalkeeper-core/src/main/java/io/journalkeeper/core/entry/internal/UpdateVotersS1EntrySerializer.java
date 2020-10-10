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
package io.journalkeeper.core.entry.internal;

import io.journalkeeper.core.state.ConfigState;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Stream;

import static io.journalkeeper.core.entry.internal.UriSerializeSupport.parseUriList;
import static io.journalkeeper.core.entry.internal.UriSerializeSupport.serializeUriList;

/**
 * @author LiYue
 * Date: 2019-05-09
 *
 * Type:                                    1 byte
 * epoch                                    8 bytes
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
public class UpdateVotersS1EntrySerializer extends InternalEntrySerializer<UpdateVotersS1Entry> {
    private int sizeOf(UpdateVotersS1Entry entry) {
        return
                Long.BYTES + // Size of epoch
                Short.BYTES * 2 +  // Size of the Old Config List: 2 bytes
                Short.BYTES * 2 +  // Size of the New Config List: 2 bytes
                Stream.concat(entry.getConfigNew().stream(), entry.getConfigOld().stream())
                        .map(URI::toASCIIString)
                        .map(s -> s.getBytes(StandardCharsets.US_ASCII))
                        .mapToInt(b -> b.length + Short.BYTES)
                        .sum();
    }
    @Override
    protected byte[] serialize(UpdateVotersS1Entry entry, byte[] header) {
        byte[] bytes = new byte[header.length + sizeOf(entry)];
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.put(header);
        buffer.putLong(entry.getEpoch());
        List<URI> config = entry.getConfigOld();
        serializeUriList(buffer, config);
        config = entry.getConfigNew();
        serializeUriList(buffer, config);
        return bytes;
    }

    @Override
    protected UpdateVotersS1Entry parse(ByteBuffer byteBuffer, int type, int version) {
        long epoch = ConfigState.EPOCH_UNKNOWN;
        if (version != InternalEntry.VERSION_LEGACY) {
            epoch = byteBuffer.getLong();
        }
        List<URI> configOld = parseUriList(byteBuffer);
        List<URI> configNew = parseUriList(byteBuffer);

        return new UpdateVotersS1Entry(configOld, configNew, epoch, version);
    }

}
