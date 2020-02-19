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
package io.journalkeeper.core.state;

import io.journalkeeper.utils.files.DoubleCopy;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author LiYue
 * Date: 2019-03-25
 */
public class StateMetadata extends DoubleCopy {
    private long lastApplied = 0;
    private int lastIncludedTerm = 0;

    public StateMetadata(File file) throws IOException {
        super(file, 128);
    }

    @Override
    protected String getName() {
        return "StateMetadata";
    }

    @Override
    protected byte[] serialize() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(Long.BYTES + Integer.BYTES);
        byteBuffer.putLong(getLastApplied());
        byteBuffer.putInt(getLastIncludedTerm());
        byteBuffer.flip();
        return byteBuffer.array();
    }

    @Override
    protected void parse(byte[] data) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(data);
        setLastApplied(byteBuffer.getLong());
        setLastIncludedTerm(byteBuffer.getInt());
    }

    public long getLastApplied() {
        return lastApplied;
    }

    public void setLastApplied(long lastApplied) {
        increaseVersion();
        this.lastApplied = lastApplied;
    }

    public int getLastIncludedTerm() {
        return lastIncludedTerm;
    }

    public void setLastIncludedTerm(int lastIncludedTerm) {
        increaseVersion();
        this.lastIncludedTerm = lastIncludedTerm;
    }
}
