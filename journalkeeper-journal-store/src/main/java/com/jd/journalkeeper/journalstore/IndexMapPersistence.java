package com.jd.journalkeeper.journalstore;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.NavigableMap;

/**
 * 存放Journal 索引序号与Raft日志索引序号的对应关系。
 * 文件结构：
 *
 * Header
 * -------
 * skipped                 4 Bytes              已删除的Entry数量。
 *
 * Data
 * -------
 * deleted              skipped * 2 * 8 Bytes   已删除的部分。
 * Journal index 1      8 Bytes                 Journal索引序号1
 * Raft index 1         8 Bytes                 Raft日志的索引序号1
 * Journal index 2      8 Bytes                 Journal索引序号2
 * Raft index 2         8 Bytes                 Raft日志的索引序号2
 * Journal index 3      8 Bytes                 Journal索引序号3
 * Raft index 3         8 Bytes                 Raft日志的索引序号3
 * ...
 *
 * 具有和Journal类似的特性：
 * 1. 只能从最小位置连续删除；
 * 2. 只能追加写入；
 * 3. 数据不变性
 *
 * 考虑到删除效率，每次删除时不直接删除数据，而是记录已删除的Entry数量(skip)。
 * 然后择机执行物理删除。
 *
 * @author liyue25
 * Date: 2019-04-23
 */
public class IndexMapPersistence {
    private static final Logger logger = LoggerFactory.getLogger(IndexMapPersistence.class);
    static void restore(NavigableMap<Long, Long> indexMap, File file) throws IOException {
        byte [] bytes = FileUtils.readFileToByteArray(file);
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        if(bytes.length >= Integer.BYTES) {
            int skip = buffer.getInt();
            buffer.position(buffer.position() + skip * 2 * Long.BYTES);
            while (buffer.remaining() >= 2 * Long.BYTES) {
                indexMap.put(buffer.getLong(), buffer.getLong());
            }

            if(buffer.hasRemaining()) {
                logger.warn("Skip tail {} bytes of file: {}.", buffer.remaining(), file.getAbsolutePath());
            }
        }
    }

    static void delete(int deleteSize, File file) throws IOException{

        if(deleteSize > 0) {
            try (RandomAccessFile raf = new RandomAccessFile(file, "rw");
                 FileChannel fileChannel = raf.getChannel()) {
                ByteBuffer intBuffer = ByteBuffer.allocate(Integer.BYTES);
                fileChannel.read(intBuffer, 0L);
                intBuffer.flip();
                int skip = intBuffer.getInt();
                intBuffer.flip();
                intBuffer.putInt(skip + deleteSize);
                intBuffer.flip();
                fileChannel.write(intBuffer, 0L);
            }
        }
    }

    static void add(long k, long v, File file) throws IOException {
        byte [] bytes = new byte[Long.BYTES * 2];
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.putLong(k);
        buffer.putLong(v);
        FileUtils.writeByteArrayToFile(file, bytes,true);
    }

    static int skipped(File file) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw");
             FileChannel fileChannel = raf.getChannel()) {
            ByteBuffer intBuffer = ByteBuffer.allocate(Integer.BYTES);
            fileChannel.read(intBuffer, 0L);
            intBuffer.flip();
            return intBuffer.getInt();
        }

    }

    static void compact(File file) throws IOException {
        byte [] bytes = FileUtils.readFileToByteArray(file);
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        if(bytes.length >= Integer.BYTES) {
            int skip = buffer.getInt();
            if(skip > 0) {
                int offset = skip * 2 * Long.BYTES;
                buffer.position(offset);
                buffer.putInt(skip, buffer.position());
                try (RandomAccessFile raf = new RandomAccessFile(file, "rw");
                     FileChannel fileChannel = raf.getChannel()) {
                    fileChannel.truncate(bytes.length - offset);
                    while (buffer.hasRemaining()) {
                        fileChannel.write(buffer);
                    }
                }

            }
        }
    }

    /**
     * 更新Map中的Entry
     * @param k Key
     * @param v Value
     * @param index Map中的Entry序号，从0开始，包含被删除的。
     * @param file 文件
     */
    static void update(long k, long v, int index, File file) throws IOException{
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw");
             FileChannel fileChannel = raf.getChannel()) {
            byte [] bytes = new byte[Long.BYTES * 2];
            ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
            byteBuffer.putLong(k);
            byteBuffer.putLong(v);
            byteBuffer.flip();
            int relPosition = Integer.BYTES + 2 * Long.BYTES * index;
            fileChannel.position(relPosition);
            while (byteBuffer.hasRemaining()) {
                fileChannel.write(byteBuffer);
            }
        }
    }
}
