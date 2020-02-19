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
package io.journalkeeper.utils.files;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;
import java.util.zip.Checksum;


/**
 * 重要的小文件，双写，避免写文件时程序以外退出文件损坏。
 */
public abstract class DoubleCopy implements Closeable {


    private static Logger logger = LoggerFactory.getLogger(DoubleCopy.class);
    // 第二份数据的位置
    private final int NEXT;
    private final AtomicLong dataVersion = new AtomicLong(0L);
    private final AtomicLong flushVersion = new AtomicLong(0L);
    // 文件
    protected File file;
    // 时间戳
    private long timestamp;
    private RandomAccessFile raf;

    /**
     * 构造函数
     *
     * @param file 本地存储文件
     * @param maxDataSize 最大数据长度
     * @throws IOException 发生IO异常时抛出
     */
    public DoubleCopy(File file, int maxDataSize) throws IOException {

        this.file = file;
        NEXT = maxDataSize;
        validate();
    }

    protected abstract String getName();

    protected abstract byte[] serialize();

    protected abstract void parse(byte[] data);

    protected int next() {
        return NEXT;
    }

    protected void increaseVersion() {
        dataVersion.incrementAndGet();
    }

    protected void validate() throws IOException {
        try {
            if (!(file.exists() || file.createNewFile())) {
                throw new IOException(String.format("create file error,%s", file.getPath()));
            }
            if (!file.canWrite()) {
                throw new IOException(String.format("file can not be written,%s", file.getPath()));
            }
            if (!file.canRead()) {
                throw new IOException(String.format("file can not be read,%s", file.getPath()));
            }
            if (raf == null) {
                raf = new RandomAccessFile(file, "rw");
            }
        } catch (IOException e) {
            logger.warn("File Exception, file: {}:", file.getAbsolutePath(), e);
            throw e;
        }
    }

    @Override
    public void close() {
        try {
            flush();
            raf.close();
            raf = null;
        } catch (IOException e) {
            logger.warn("Close file exception: ", e);
        }
    }

    /**
     * 恢复
     * @throws IOException 发生IO异常时抛出
     */
    public synchronized void recover() throws IOException {
        long length = raf.length();
        if (length > 0) {
            // 读取第一份数据
            raf.seek(0);

            // 读取第一份数据
            boolean success = false;
            try {
                success = tryToRecover();
            } catch (Exception e) {
                logger.warn("Exception while recover first copy of " + getName(), e);
            }


            if (!success) {
                try {
                    raf.seek(next());
                    success = tryToRecover();
                } catch (Exception e) {
                    logger.warn("Exception while recover second copy of " + getName(), e);
                }
            }

            if (!success) {
                throw new IOException(String.format("Recover file %s failed!", getName()));
            }
        }
        flushVersion.set(dataVersion.get());
        logger.info(getName() + " recover success, file: {}.", file.getAbsolutePath());
    }


    private boolean tryToRecover() throws IOException {
        int length = raf.readInt();
        long timestamp = raf.readLong();
        long checksum = raf.readLong();

        byte[] data = new byte[length];

        int size = raf.read(data, 0, length);

        if (size == length) {
            if (checksum == getChecksum(data)) {
                // 数据正常
                this.timestamp = timestamp;
                parse(data);
                return true;
            }

        }
        return false;
    }


    /**
     * 写入到磁盘中
     */
    private void doFlush() {
        byte[] data = serialize();
        int length = data.length;
        long timestamp = System.currentTimeMillis();
        long checksum = getChecksum(data);

        try {
            // 双写
            raf.seek(0);
            raf.writeInt(length);
            raf.writeLong(timestamp);
            raf.writeLong(checksum);
            raf.write(data);

            raf.seek(next());
            raf.writeInt(length);
            raf.writeLong(timestamp);
            raf.writeLong(checksum);
            raf.write(data);

            raf.getFD().sync();
        } catch (IOException e) {
            logger.error(getName() + "flush error.", e);
        } finally {
            this.timestamp = timestamp;
        }
    }

    /**
     * 刷盘
     */
    public synchronized void flush() {
        long version = dataVersion.get();
        if (version != flushVersion.get()) {
            doFlush();
            flushVersion.set(dataVersion.get());
        }
    }

    public long getTimestamp() {
        return timestamp;
    }

    private long getChecksum(byte[] data) {
        Checksum crc = new CRC32();
        crc.update(data, 0, data.length);
        return crc.getValue();
    }


}