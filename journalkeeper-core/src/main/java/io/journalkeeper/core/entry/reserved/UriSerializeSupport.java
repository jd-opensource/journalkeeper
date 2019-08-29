package io.journalkeeper.core.entry.reserved;

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
            byte [] asciiBytes = uri.toASCIIString().getBytes(StandardCharsets.US_ASCII);
            if(asciiBytes.length >= Short.MAX_VALUE) {
                throw new SerializeException(String.format("URI length too large! Max: %d, actual: %d, uri: %s. ",
                        Short.MAX_VALUE, config.size(), uri));
            }
            buffer.putShort((short) asciiBytes.length);
            buffer.put(asciiBytes);
        }
    }

    public static List<URI> parseUriList(ByteBuffer buffer) {
        int sizeOfList = buffer.getShort();
        List<URI> config = new ArrayList<>(sizeOfList);
        for (int i = 0; i < sizeOfList; i++) {
            int length = buffer.getShort();
            byte [] asciiBytes = new byte[length];
            buffer.get(asciiBytes);
            URI uri = URI.create(new String(asciiBytes, StandardCharsets.US_ASCII));
            config.add(uri);
        }
        return config;
    }

}
