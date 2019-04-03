package com.jd.journalkeeper.utils.test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Create random bytes
 * @author liyue25
 * Date: 2019-04-03
 */
public class ByteUtils {
    public static List<byte []> createByteList(int maxLength, int size) {
        List<byte []> bytesList = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            byte[] bytes = createBytes(maxLength);
            bytesList.add(bytes);
        }
        return bytesList;
    }

    public static byte[] createBytes(int maxLength) {
        byte [] bytes = new byte[ThreadLocalRandom.current().nextInt(maxLength)];
        for (int j = 0; j < bytes.length; j++) {
            bytes[j] = (byte ) j;
        }
        return bytes;
    }

    public static byte [] concatBytes(List<byte []> bytesList) {
        return bytesList.stream()
                .collect(
                        ByteArrayOutputStream::new,
                        (b, e) -> {
                            try {
                                b.write(e);
                            } catch (IOException e1) {
                                throw new RuntimeException(e1);
                            }
                        },
                        (a, b) -> {}).toByteArray();
    }
}
