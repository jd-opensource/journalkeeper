package io.journalkeeper.core.server;

import io.journalkeeper.core.exception.InstallSnapshotException;
import io.journalkeeper.rpc.server.InstallSnapshotRequest;
import io.journalkeeper.utils.files.FileUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * @author LiYue
 * Date: 2019/11/22
 */
public class PartialSnapshot {
    private Path path = null;
    private long offset = 0;

    public Path getPath() {
        return path;
    }

    public long getOffset() {
        return offset;
    }

    /**
     * 恢复状态。
     * 反复调用install复制序列化的状态数据。
     * 所有数据都复制完成后，最后调用installFinish恢复状态。
     * @param request 安装快照请求
     * @param snapshotPath 安装路径
     * @throws IOException 发生IO异常时抛出
     */
    public synchronized void installTrunk(InstallSnapshotRequest request, Path snapshotPath) throws IOException {

        if (offset == 0) {
           if(isPartial()) {
               clear();
           }
           begin(snapshotPath);
        } else {
            if (!isPartial()) {
                throw new InstallSnapshotException(
                        String.format("No partial snapshot exists! Request: %s.", request)
                );
            }

            if(!snapshotPath.equals(getPath())) {
                throw new InstallSnapshotException(
                        String.format("Partial snapshot path not match! Partial snapshot: %s, request: %s.", this, request)
                );
            }

            if(request.getOffset() != getOffset()) {
                throw new InstallSnapshotException(
                        String.format("Partial snapshot offset not match! Partial snapshot: %s, request: %s.", this, request)
                );
            }

        }

        ByteBuffer buffer = ByteBuffer.wrap(request.getData());
        int filenameLength = buffer.getInt();
        byte [] filenameBytes = new byte[filenameLength];
        buffer.get(filenameBytes);
        String filePathString = new String(filenameBytes, StandardCharsets.UTF_8);
        long offsetOfFile = buffer.getLong();

        Path filePath = path.resolve(filePathString);

        if(offsetOfFile == 0) {
            Files.createDirectories(filePath.getParent());
        }
        if( offsetOfFile == 0 || Files.size(filePath) == offsetOfFile) {
            try (FileOutputStream output = new FileOutputStream(filePath.toFile(), true)) {
                output.write(request.getData(), buffer.position(), buffer.remaining());
            }
        } else {
            throw new InstallSnapshotException(
                    String.format(
                            "Current file size %d should equal trunk offset %d! File: %s",
                            Files.size(filePath), offsetOfFile, filePath.toString()
                    )
            );
        }

        if (request.isDone()) {
           finish();
        }
    }

    public synchronized void clear() throws IOException{
        if (null != path) {
            FileUtils.deleteFolder(path.toFile());
            finish();
        }
    }

    public synchronized void finish() {
        path = null;
        offset = 0;
    }

    public synchronized void begin(Path path) throws IOException {
        if(null == path) {
            throw new IllegalArgumentException("Path can not be null!");
        }
        this.path = path;
        offset = 0;

        if(Files.exists(path)) {
            FileUtils.deleteFolder(path.toFile());
        }
        Files.createDirectories(path);
    }

    public boolean isPartial() {
        return path != null;
    }

    @Override
    public String toString() {
        return "PartialSnapshot{" +
                "path=" + path +
                ", offset=" + offset +
                '}';
    }
}
