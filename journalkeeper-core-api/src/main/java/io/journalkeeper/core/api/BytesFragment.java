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
package io.journalkeeper.core.api;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * @author LiYue
 * Date: 2019/10/12
 */
public class BytesFragment {
    private final byte[] bytes;
    private final int offset;
    private final int length;

    public BytesFragment(byte[] bytes) {
        this(bytes, 0, bytes.length);
    }

    public BytesFragment(byte[] bytes, int offset) {
        this(bytes, offset, bytes.length - offset);
    }

    public BytesFragment(byte[] bytes, int offset, int length) {
        if (bytes.length != 0 || offset != 0 || length != 0) {
            if (offset < 0 || offset > bytes.length) {
                throw new IllegalArgumentException("Invalid offset!");
            }

            if (length < 0 || length + offset > bytes.length) {
                throw new IllegalArgumentException("Invalid length!");
            }
        }

        this.bytes = bytes;
        this.offset = offset;
        this.length = length;
    }

    private static int hashCode(byte a[], int offset, int length) {
        if (a == null)
            return 0;

        int result = 1;
        for (int i = offset; i < length; i++) {
            byte element = a[i];
            result = 31 * result + element;
        }

        return result;
    }

    public byte[] getBytes() {

        return offset == 0 && length == bytes.length ?
                bytes :
                Arrays.copyOfRange(bytes, offset, offset + length);
    }

    public ByteBuffer getBuffer() {
        return ByteBuffer.wrap(bytes, offset, length);
    }

    public int getOffset() {
        return offset;
    }

    public int getLength() {
        return length;
    }

    @Override
    public int hashCode() {

        return hashCode(bytes, offset, length);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BytesFragment that = (BytesFragment) o;

        if (this.length == that.length) {
            for (int i = 0; i < this.length; i++) {
                if (this.bytes[this.offset + i] != that.bytes[that.offset + i]) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }

    }
}
