package com.jd.journalkeeper.core.state;

import com.jd.journalkeeper.core.api.State;
import com.jd.journalkeeper.core.api.StateFactory;
import com.jd.journalkeeper.core.exception.StateInstallException;
import com.jd.journalkeeper.core.exception.StateRecoverException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 使用本地文件存储的状态机实现
 */
public abstract class LocalState<E, Q, R> implements State<E, Q, R>, Flushable {
    private static final Logger logger = LoggerFactory.getLogger(LocalState.class);
    public final static short CRLF = ByteBuffer.wrap(new byte[] {0x0D, 0x0A}).getShort();

    protected long lastApplied = 0;
    protected int lastIncludedTerm = 0;
    protected Path path;
    protected Properties properties;
    protected final StateFactory<E, Q, R> factory;

    protected LocalState(StateFactory<E, Q, R> stateFactory) {
        this.factory = stateFactory;
    }

    @Override
    public long lastApplied() {
        return lastApplied;
    }

    @Override
    public int lastIncludedTerm() {
        return lastIncludedTerm;
    }

    @Override
    public final void recover(Path path, Properties properties) {
        this.path = path;
        this.properties = properties;
        try (StateMetadata stateMetadata = new StateMetadata(path.resolve(metadataPath()).toFile())) {
            stateMetadata.recover();
            lastApplied = stateMetadata.getLastApplied();
            lastIncludedTerm = stateMetadata.getLastIncludedTerm();

            Files.createDirectories(localStatePath());

            recoverLocalState(localStatePath(), properties);
        } catch (IOException e) {
            throw new StateRecoverException(e);
        }

    }

    private Path localStatePath() {
        return path.resolve("data");
    }

    /**
     * 从本地文件恢复状态，如果不存在则创建新的。
     */
    protected abstract void recoverLocalState(Path path, Properties properties);

    /**
     * 列出所有复制时需要拷贝的文件。
     */
    protected List<Path> listAllFiles() {
        return FileUtils.listFiles(
                localStatePath().toFile(),
                new RegexFileFilter("^(.*?)"),
                DirectoryFileFilter.DIRECTORY
        ).stream().map(File::toPath).collect(Collectors.toList());
    }

    @Override
    public State<E, Q, R> takeASnapshot(Path destPath) throws IOException {
        flushState(localStatePath());
        State<E, Q, R> state = factory.createState();
        List<Path> srcFiles = listAllFiles();

        List<Path> destFiles = srcFiles.stream()
                .map(src -> path.relativize(src))
                .map(destPath::resolve)
                .collect(Collectors.toList());
        Files.createDirectories(destPath);
        for (int i = 0; i < destFiles.size(); i++) {
            Path srcFile = srcFiles.get(i);
            Path destFile = destFiles.get(i);
            Files.createDirectories(destFile.getParent());
            FileUtils.copyFile(srcFile.toFile(), destFile.toFile());
        }

        state.recover(destPath, properties);
        return state;
    }

    /**
     * [File Size (4 bytes)][File relative path][CRLF]
     * [File Size (4 bytes)][File relative path][CRLF]
     * ...
     * [serialized files data]
     */

    @Override
    public byte[] readSerializedData(long startOffset, int size) throws IOException {
        long headerSize = serializedHeaderSize();

        if(startOffset < headerSize) {
            return readSerializedHeader(startOffset);
        } else {
            return readSerializedFileData(startOffset - headerSize, size);
        }
    }

    private byte[] readSerializedHeader(long startOffset) {
        byte [] serializedHeader = listAllFiles().stream()
                 .map(src -> src.relativize(path))
                 .map(Path::toString)
                 .map(file -> {
                     byte [] filename = file.getBytes(StandardCharsets.UTF_8);
                     byte [] bytes = new byte[filename.length + Integer.BYTES + 2];
                     ByteBuffer buffer = ByteBuffer.wrap(bytes);
                     buffer.putInt(filename.length);
                     buffer.put(filename);
                     buffer.putShort(CRLF);
                     return bytes;
                 }).collect(
                     ByteArrayOutputStream::new,
                     (b, e) -> {
                         try {
                             b.write(e);
                         } catch (IOException e1) {
                             throw new RuntimeException(e1);
                         }
                     },
                     (a, b) -> {}
                 ).toByteArray();
        if(startOffset > 0) {
            return Arrays.copyOfRange(serializedHeader, (int) startOffset, serializedHeader.length);
        } else {
            return serializedHeader;
        }
    }

    private byte[] readSerializedFileData(long startOffset, int size) throws IOException {
        List<File> sortedFiles = listAllFiles().stream()
                .map(Path::toFile)
                .filter(File::isFile)
                .sorted(Comparator.comparing(File::getAbsolutePath))
                .collect(Collectors.toList());

        long fileOffset = 0L;
        long offset = startOffset;

        for(File file: sortedFiles) {
            if(fileOffset <= offset && offset < fileOffset + file.length()) {
                try(RandomAccessFile raf = new RandomAccessFile(file, "r"); FileChannel fc = raf.getChannel()) {
                    int relOffset = (int) (offset - fileOffset);
                    ByteBuffer buffer = ByteBuffer.allocate(Math.min(size, (int) (file.length() - relOffset)));
                    fc.position(relOffset);
                    int readBytes;
                    while (buffer.hasRemaining() && (readBytes = fc.read(buffer)) > 0) {
                        offset += readBytes;
                    }
                    return buffer.array();
                }
            }
        }
        return new byte[0];
    }

    @Override
    public long serializedDataSize() {
        List<Path> paths = listAllFiles();
        long fileDataSize = paths.stream().map(Path::toFile).mapToLong(File::length).sum();
        long headerSize = paths.stream()
                .map(src -> src.relativize(path))
                .map(Path::toString)
                .mapToLong(file -> file.getBytes(StandardCharsets.UTF_8).length + Integer.BYTES + 2)
                .sum();
        return headerSize + fileDataSize;
    }

    private long serializedHeaderSize() {
        return listAllFiles().stream()
                .map(src -> src.relativize(path))
                .map(Path::toString)
                .mapToLong(file -> file.getBytes(StandardCharsets.UTF_8).length + Integer.BYTES + 2)
                .sum();
    }

    private NavigableMap<Long, Path> installingFiles = new TreeMap<>();
    @Override
    public void installSerializedData(byte[] data, long offset) throws IOException {
        if(offset == 0L) {
            installSerializedHeader(data);

        } else {
            installSerializedFile(data, offset);
        }
    }

    private void installSerializedFile(byte[] data, long offset) throws IOException {
        Map.Entry<Long, Path> entry = installingFiles.floorEntry(offset);
        if (null == entry) {
            throw new StateInstallException();
        }
        if (Files.size(entry.getValue()) != (offset - entry.getKey())) {
            throw new StateInstallException();
        }
        FileUtils.writeByteArrayToFile(entry.getValue().toFile(), data, true);
    }

    private void installSerializedHeader(byte[] data) throws IOException {
        installingFiles.clear();
        // write headers
        long nextFileOffset = data.length;
        // Most unix filesystems has a maximum path of 4096 characters
        byte [] filenameBuffer = new byte[Math.min(data.length, 4096)];
        ByteBuffer byteBuffer = ByteBuffer.wrap(data);

        while (byteBuffer.hasRemaining()) {
            int fileLength = byteBuffer.getInt();
            int filenameLength = 0;
            while (filenameLength < byteBuffer.remaining()) {
                if(byteBuffer.getShort(byteBuffer.position() + filenameLength) == CRLF) {
                    String filePathStr = new String(filenameBuffer, 0, filenameLength, StandardCharsets.UTF_8);
                    Path filePath = path.resolve(filePathStr);
                    createOrTruncateFile(filePath);
                    installingFiles.put(nextFileOffset, filePath);
                    nextFileOffset += fileLength;
                    byteBuffer.position(byteBuffer.position() + 2);

                    break;
                } else {
                    filenameBuffer[filenameLength++] = byteBuffer.get();
                }
            }
        }
    }

    /**
     * 如果文件所在的目录不存在则创建；
     * 如果文件存在：清空文件。
     * 如果文件不存在：创建空文件。
     */
    private void createOrTruncateFile(Path filePath) throws IOException {
        File parentDir = filePath.getParent().toFile();
        File file = filePath.toFile();
        if(parentDir.isDirectory() || parentDir.mkdirs()) {
            if(file.exists()) {
                try (FileChannel outChan = new FileOutputStream(file, true).getChannel()) {
                    outChan.truncate(0L);
                }
            } else {
                filePath.toFile().setLastModified(System.currentTimeMillis());  // touch to create file
            }
        } else {
            throw new StateInstallException(String.format("Cannot create directory: %s.", parentDir.getAbsolutePath()));
        }
    }

    /**
     * 存放state元数据文件的路径
     */
    protected Path metadataPath() {
       return Paths.get("metadata");
    }
    @Override
    public void installFinish(long lastApplied, int lastIncludedTerm) {
        try {
            try (StateMetadata stateMetadata = new StateMetadata(path.resolve(metadataPath()).toFile())) {
                stateMetadata.setLastApplied(lastApplied);
                stateMetadata.setLastIncludedTerm(lastIncludedTerm);
                stateMetadata.flush();
            }

        } catch (IOException e) {
            logger.warn("Install state exception: ", e);
        }
    }

    @Override
    public void flush() throws IOException {
        try (StateMetadata stateMetadata = new StateMetadata(path.resolve(metadataPath()).toFile())) {
            stateMetadata.setLastApplied(lastApplied);
            stateMetadata.setLastIncludedTerm(lastIncludedTerm);
            stateMetadata.flush();
        }

        flushState(localStatePath());

    }

    protected abstract void flushState(Path statePath) throws IOException;
    @Override
    public void clear() {
        lastApplied = -1L;
        lastIncludedTerm = 0;
        try {
            FileUtils.cleanDirectory(path.toFile());
        } catch (IOException e) {
            throw new StateInstallException(e);
        }
    }

    @Override
    public void setLastApplied(long lastApplied) {
        this.lastApplied = lastApplied;
    }
}
