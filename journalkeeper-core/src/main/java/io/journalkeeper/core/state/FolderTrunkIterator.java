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

import io.journalkeeper.base.ReplicableIterator;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author LiYue
 * Date: 2019/11/21
 */
public class FolderTrunkIterator implements ReplicableIterator {
    private final Path root;
    private final List<Path> files;
    private int fileIndex = 0;
    private long offsetOfCurrentFile = 0;
    private final int maxTrunkSize;
    private final long lastIncludedIndex;
    private final int lastIncludedTerm;
    private long offset;

    public FolderTrunkIterator(Path root, List<Path> files, int maxTrunkSize, long lastIncludedIndex, int lastIncludedTerm) {
        this.root = root;
        this.maxTrunkSize = maxTrunkSize;
        this.lastIncludedIndex = lastIncludedIndex;
        this.lastIncludedTerm = lastIncludedTerm;
        this.files = toRelative(root, files);
        this.offset = 0L;
    }

    private List<Path> toRelative(Path root, List<Path> files) {
        return files.stream()
                .map(root::relativize)
                .collect(Collectors.toList());
    }

    @Override
    public long lastIncludedIndex() {
        return lastIncludedIndex;
    }

    @Override
    public int lastIncludedTerm() {
        return lastIncludedTerm;
    }

    @Override
    public long offset() {
        return offset;
    }

    // Trunk Header
    //      Length of file name bytes:  4 bytes
    //      File name bytes:            Variable bytes
    //      Trunk offset of the file    8 bytes
    // Trunk data                       Variable bytes
    @Override
    public byte[] nextTrunk() throws IOException {
        Path relFile = files.get(fileIndex);
        Path file = root.resolve(relFile);
        long fileSize = Files.size(file);
        long remainingSize ;

        byte [] trunk;
        ByteBuffer buffer;

        byte [] filenameBytes = relFile.toString().getBytes(StandardCharsets.UTF_8);
        remainingSize =
                fileSize +
                        Integer.BYTES + filenameBytes.length  + Long.BYTES - // Header size
                        offsetOfCurrentFile;

        trunk = new byte[(int) Math.min(remainingSize , maxTrunkSize)];
        buffer = ByteBuffer.wrap(trunk);
        buffer.putInt(filenameBytes.length);
        buffer.put(filenameBytes);
        buffer.putLong(offsetOfCurrentFile);

        int sizeToRead = buffer.remaining();
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "r"); FileChannel fc = raf.getChannel()) {
            fc.position(offsetOfCurrentFile);
            while (buffer.hasRemaining()) {
                fc.read(buffer);
            }
        }

        offsetOfCurrentFile += sizeToRead;

        if (offsetOfCurrentFile == fileSize) {
            fileIndex ++;
            offsetOfCurrentFile = 0;
        }
        offset += trunk.length;
        return trunk;
    }

    @Override
    public boolean hasMoreTrunks() {
        return fileIndex < files.size();
    }
}
