package io.journalkeeper.core.state;

import io.journalkeeper.base.Replicable;
import io.journalkeeper.base.Serializer;
import io.journalkeeper.core.api.JournalEntry;
import io.journalkeeper.core.api.RaftJournal;
import io.journalkeeper.core.api.State;
import io.journalkeeper.core.api.StateFactory;
import io.journalkeeper.core.api.StateResult;
import io.journalkeeper.core.entry.reserved.ReservedEntriesSerializeSupport;
import io.journalkeeper.core.entry.reserved.ReservedEntryType;
import io.journalkeeper.core.entry.reserved.ScalePartitionsEntry;
import io.journalkeeper.core.entry.reserved.SetPreferredLeaderEntry;
import io.journalkeeper.core.exception.StateInstallException;
import io.journalkeeper.core.exception.StateRecoverException;
import io.journalkeeper.persistence.MetadataPersistence;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.Flushable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.StampedLock;
import java.util.stream.Collectors;

import static io.journalkeeper.core.api.RaftJournal.INTERNAL_PARTITION;
import static io.journalkeeper.core.api.RaftJournal.RESERVED_PARTITIONS_START;

/**
 *
 * {@link #serializedDataSize()}, {@link #readSerializedTrunk(long, int)},
 * {@link #installSerializedTrunk(byte[], long, boolean)}
 * 以上这三个方法用于将当前状态数据序列化，然后复制的远端节点上，再恢复成状态，具体的流程是：
 * 1. 调用{@link #serializedDataSize()} 计算当前状态数据序列化之后的长度；
 * 2. 反复在读取状态的节点调用{@link #readSerializedTrunk(long, int)}读取序列化后的状态数据，复制到目标节点后调用 {@link #installSerializedTrunk(byte[], long, boolean)}写入状态数据；
 *
 * @author LiYue
 * Date: 2019/11/20
 */
public class JournalKeeperState <E, ER, Q, QR> implements Replicable {
    private static final Logger logger = LoggerFactory.getLogger(JournalKeeperState.class);
    private static final String USER_STATE_PATH = "user";
    private static final String INTERNAL_STATE_PATH = "internal";
    private static final String INTERNAL_STATE_FILE = "state";
    private final static short CRLF = ByteBuffer.wrap(new byte[] {0x0D, 0x0A}).getShort();
    private Path path;
    private State<E, ER, Q, QR> userState;
    private InternalState internalState;
    private final Map<ReservedEntryType, ApplyInternalEntryInterceptor> internalEntryInterceptors = new HashMap<>();
    private final List<ApplyReservedEntryInterceptor> reservedEntryInterceptors = new CopyOnWriteArrayList<>();
    private final StateFactory<E, ER, Q, QR> userStateFactory;
    private final MetadataPersistence metadataPersistence;

    /**
     * State文件读写锁
     */
    private final ReadWriteLock stateFilesLock = new ReentrantReadWriteLock();

    /**
     * 状态读写锁
     * 读取状态是加乐观锁或读锁，变更状态时加写锁。
     */
    private final StampedLock stateLock = new StampedLock();

    public JournalKeeperState(StateFactory<E, ER, Q, QR> userStateFactory, MetadataPersistence metadataPersistence) {
        this.userStateFactory = userStateFactory;
        this.metadataPersistence = metadataPersistence;
    }
    public void init(Path path,  List<URI> voters, Set<Integer> partitions, URI preferredLeader) throws IOException {
        Files.createDirectories(path.resolve(USER_STATE_PATH));
        InternalState internalState = new InternalState(new ConfigState(voters), partitions, preferredLeader);
        File lockFile = path.getParent().resolve(path.getFileName() + ".lock").toFile();
        try (RandomAccessFile raf = new RandomAccessFile(lockFile, "rw");
             FileChannel fileChannel = raf.getChannel()){
            FileLock lock = fileChannel.tryLock();
            if (null == lock) {
                throw new ConcurrentModificationException(
                        String.format(
                                "Some other thread is operating the state files! State: %s.", path.toString()
                        ));
            } else {
                flushInternalState(internalStateFile(path), internalState);
                lock.release();
            }
        } finally {
            lockFile.delete();
        }
    }

    public void flush () throws IOException {
        try {
            stateFilesLock.writeLock().lock();
            flushInternalState();
            flushUserState();
        } finally {
            stateFilesLock.writeLock().unlock();
        }
    }

    private void flushUserState(State userState) throws IOException {
        if(userState instanceof Flushable) {
            ((Flushable) userState).flush();
        }
    }

    private void flushUserState() throws IOException {
        flushUserState(userState);
    }

    private void flushInternalState() throws IOException {
        flushInternalState(internalStateFile(path), internalState);
    }

    private Path internalStateFile(Path statePath) {
        return statePath.resolve(INTERNAL_STATE_PATH).resolve(INTERNAL_STATE_FILE);
    }

    private void flushInternalState(Path internalStateFile, InternalState internalState ) throws IOException {
        metadataPersistence.save(internalStateFile, new PersistInternalState().fromInternalState(internalState));
    }
    public void recover(Path path, Properties properties) {
        this.path = path;
        stateFilesLock.writeLock().lock();
        try {
            this.internalState = recoverInternalState(internalStateFile(path));
            this.userState = userStateFactory.createState();
            userState.recover(path.resolve(USER_STATE_PATH), properties);
        } catch (IOException e) {
            throw new StateRecoverException(e);
        } finally {
            stateFilesLock.writeLock().unlock();
        }
    }

    private InternalState recoverInternalState(Path internalStateFile) throws IOException {
        return metadataPersistence.load(internalStateFile, PersistInternalState.class).toInternalState();
    }

    public void addInterceptor(ReservedEntryType type, ApplyInternalEntryInterceptor internalEntryInterceptor) {
        this.internalEntryInterceptors.put(type, internalEntryInterceptor);
    }

    public void removeInterceptor(ReservedEntryType type) {
        this.internalEntryInterceptors.remove(type);
    }

    public void addInterceptor(ApplyReservedEntryInterceptor interceptor) {
        reservedEntryInterceptors.add(interceptor);
    }

    public void removeInterceptor(ApplyReservedEntryInterceptor interceptor) {
        reservedEntryInterceptors.remove(interceptor);
    }
    public Path getPath() {
        return path;
    }

    public long lastIncludedIndex() {
        return internalState.getLastIncludedIndex();
    }

    public int lastIncludedTerm() {
        return internalState.getLastIncludedTerm();
    }

    public long lastApplied() {
        return lastIncludedIndex() + 1;
    }

    public StateResult<ER> applyEntry(JournalEntry journalEntry, Serializer<E> entrySerializer, RaftJournal journal) {
        byte [] payloadBytes = journalEntry.getPayload().getBytes();
        int partition = journalEntry.getPartition();
        int batchSize = journalEntry.getBatchSize();

        StateResult<ER> result = new StateResult<>(null);
        long stamp = stateLock.writeLock();
        try {
            if(partition< RESERVED_PARTITIONS_START) {
                E entry = entrySerializer.parse(payloadBytes);
                result = userState.execute(entry, partition, lastApplied(), batchSize, journal);
            } else if (partition == INTERNAL_PARTITION){
                applyRaftPartition(journalEntry.getPayload().getBytes());

            } else {
                for (ApplyReservedEntryInterceptor reservedEntryInterceptor : reservedEntryInterceptors) {
                    reservedEntryInterceptor.applyReservedEntry(journalEntry, lastApplied());
                }
            }
            internalState.setLastIncludedTerm(journalEntry.getTerm());
            internalState.next();
            result.setLastApplied(lastApplied());
        } finally {
            stateLock.unlockWrite(stamp);
        }
        return result;
    }


    private void applyRaftPartition(byte [] reservedEntry) {
        ReservedEntryType type = ReservedEntriesSerializeSupport.parseEntryType(reservedEntry);
        logger.info("Apply reserved entry, type: {}", type);
        switch (type) {
            case TYPE_SCALE_PARTITIONS:
                internalState.setPartitions(ReservedEntriesSerializeSupport.parse(reservedEntry, ScalePartitionsEntry.class).getPartitions());
                break;
            case TYPE_SET_PREFERRED_LEADER:
                SetPreferredLeaderEntry setPreferredLeaderEntry = ReservedEntriesSerializeSupport.parse(reservedEntry);
                URI old = internalState.getPreferredLeader();
                internalState.setPreferredLeader(setPreferredLeaderEntry.getPreferredLeader());
                logger.info("Set preferred leader from {} to {}.", old, internalState.getPreferredLeader());
                break;
            default:
        }

        ApplyInternalEntryInterceptor interceptor = internalEntryInterceptors.get(type);
        if (null != interceptor) {
            interceptor.applyInternalEntry(type, reservedEntry);
        }

        try {
            flushInternalState();
        } catch (IOException e) {
            logger.warn("Flush internal state exception! Path: {}.", path, e);
        }
    }

    public StateQueryResult<QR> query(Q query, RaftJournal journal) {
        StateQueryResult<QR> result;

        long stamp = stateLock.tryOptimisticRead();
        result = new StateQueryResult<>(userState.query(query, journal), lastApplied());

        if (!stateLock.validate(stamp)) {
            stamp = stateLock.readLock();
            try {
                result = new StateQueryResult<>(userState.query(query, journal), lastApplied());
            } finally {
                stateLock.unlockRead(stamp);
            }
        }
        return result;
    }

    public void dump(Path destPath) throws IOException {
        flush();
        try {
            stateFilesLock.readLock().lock();

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
                Files.copy(srcFile, destFile);
            }
        } finally {
            stateFilesLock.readLock().unlock();
        }
    }

    /**
     * 列出所有复制时需要拷贝的文件。
     * @return 所有需要复制的文件的Path
     */
    private List<Path> listAllFiles() {
        return FileUtils.listFiles(
                path.toFile(),
                new RegexFileFilter("^(.*?)"),
                DirectoryFileFilter.DIRECTORY
        ).stream().map(File::toPath).collect(Collectors.toList());
    }

    public List<URI> voters() {
        return internalState.getConfigState().voters();
    }

    /**
     * File count:                          4 Bytes
     * File name array:
     *      File name:
     *          File length:                4 Bytes
     *          File relative path:         Variable Bytes
     *          CRLF:                       2 Bytes
     *      File name
     *      File name
     *      ...
     * [serialized files data]
     *
     * @param startOffset 偏移量
     * @param size 本次读取的长度
     * @return 序列化后的状态数据
     * @throws IOException 发生IO异常时抛出
     */

    @Override
    public byte[] readSerializedTrunk(long startOffset, int size) throws IOException {
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
    public void installSerializedTrunk(byte[] data, long offset, boolean isLastTrunk) throws IOException {
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
     * @param filePath 文件目录
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


    public void setConfigState(ConfigState configState) {
        internalState.setConfigState(configState);
    }

    public void close() {
        userState.close();
    }

    public void clear() throws IOException{
        FileUtils.cleanDirectory(path.toFile());
    }

    public ConfigState getConfigState() {
        return internalState.getConfigState();
    }

    public Set<Integer> getPartitions() {
        return internalState.getPartitions();
    }

    public URI getPreferredLeader() {
        return internalState.getPreferredLeader();
    }
}
