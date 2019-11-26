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
package io.journalkeeper.core.server;

import io.journalkeeper.core.exception.InstallSnapshotException;
import io.journalkeeper.utils.files.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 *
 * @author LiYue
 * Date: 2019/11/22
 */
class PartialSnapshot {
    private static final Logger logger = LoggerFactory.getLogger(PartialSnapshot.class);
    private final Path partialSnapshotPath;
    private Path snapshotPath = null;
    private long offset = 0;

    PartialSnapshot(Path partialSnapshotPath) {
        this.partialSnapshotPath = partialSnapshotPath;
    }

    private Path getSnapshotPath() {
        return snapshotPath;
    }

    public long getOffset() {
        return offset;
    }

    /**
     * 安装快照。
     * 反复调用install复制序列化的状态数据。
     * 状态数据先被安装在{@link #partialSnapshotPath}中，当全部状态数据安装完成后，
     * 再复制到{@link #snapshotPath}中
     * 所有数据都复制完成后，将状态。
     * @param offset 快照偏移量
     * @param data 快照数据
     * @param snapshotPath 安装路径
     * @throws IOException 发生IO异常时抛出
     */
    void installTrunk(long offset, byte[] data, Path snapshotPath) throws IOException {

        if (offset == 0) {
            begin(snapshotPath);
        } else {
            if (!isPartial()) {
                throw new InstallSnapshotException(
                        String.format("No partial snapshot exists! Install path: %s.", snapshotPath)
                );
            }

            if (!snapshotPath.equals(getSnapshotPath())) {
                throw new InstallSnapshotException(
                        String.format("Partial snapshot path not match! Partial snapshot: %s, install path: %s.", this, snapshotPath)
                );
            }

            if (offset != getOffset()) {
                throw new InstallSnapshotException(
                        String.format("Partial snapshot offset not match! Partial snapshot: %s, request offset: %d.", this, offset)
                );
            }

        }

        ByteBuffer buffer = ByteBuffer.wrap(data);
        int filenameLength = buffer.getInt();
        byte [] filenameBytes = new byte[filenameLength];
        buffer.get(filenameBytes);
        String filePathString = new String(filenameBytes, StandardCharsets.UTF_8);
        long offsetOfFile = buffer.getLong();

        Path filePath = this.partialSnapshotPath.resolve(filePathString);

        if(offsetOfFile == 0) {
            Files.createDirectories(filePath.getParent());
        }

        logger.info("Installing snapshot file: {}...", filePath);

        if( offsetOfFile == 0 || Files.size(filePath) == offsetOfFile) {
            try (FileOutputStream output = new FileOutputStream(filePath.toFile(), true)) {
                output.write(data, buffer.position(), buffer.remaining());
            }
            this.offset += data.length;
        } else {
            throw new InstallSnapshotException(
                    String.format(
                            "Current file size %d should equal trunk offset %d! File: %s",
                            Files.size(filePath), offsetOfFile, filePath.toString()
                    )
            );
        }

    }

    void finish() throws IOException {
        FileUtils.deleteFolder(snapshotPath);
        FileUtils.dump(partialSnapshotPath, snapshotPath);
        snapshotPath = null;
        offset = 0;
    }

    private void begin(Path path) throws IOException {
        if(null == path) {
            throw new IllegalArgumentException("Path can not be null!");
        }
        this.snapshotPath = path;
        offset = 0;

        if (Files.exists(partialSnapshotPath)) {
            FileUtils.deleteFolder(partialSnapshotPath);
        }
        Files.createDirectories(partialSnapshotPath);
    }

    private boolean isPartial() {
        return snapshotPath != null;
    }

    @Override
    public String toString() {
        return "PartialSnapshot{" +
                "path=" + snapshotPath +
                ", offset=" + offset +
                '}';
    }
}
