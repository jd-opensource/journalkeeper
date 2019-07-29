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
    public static List<byte []> createRandomSizeByteList(int maxLength, int size) {
        List<byte []> bytesList = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            byte[] bytes = createRandomSizeBytes(maxLength);
            bytesList.add(bytes);
        }
        return bytesList;
    }

    public static byte[] createRandomSizeBytes(int maxLength) {
        byte [] bytes = new byte[ThreadLocalRandom.current().nextInt(maxLength)];
        for (int j = 0; j < bytes.length; j++) {
            bytes[j] = (byte ) j;
        }
        return bytes;
    }

    public static List<byte []> createFixedSizeByteList(int length, int size) {
        List<byte []> bytesList = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            byte[] bytes = createFixedSizeBytes(length);
            bytesList.add(bytes);
        }
        return bytesList;
    }

    public static byte[] createFixedSizeBytes(int length) {
        byte [] bytes = new byte[length];
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
