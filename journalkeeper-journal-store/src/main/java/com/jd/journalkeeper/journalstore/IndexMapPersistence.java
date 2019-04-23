package com.jd.journalkeeper.journalstore;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NavigableMap;

/**
 * 存放Journal 索引序号与Raft日志索引序号的对应关系。
 * 文件结构：
 *
 *
 * @author liyue25
 * Date: 2019-04-23
 */
public class IndexMapPersistence {
    static void restore(NavigableMap<Long, Long> indexMap, File file) throws IOException {
        byte [] bytes = FileUtils.readFileToByteArray(file);
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        if(bytes.length >= Integer.BYTES) {
            int skip = buffer.getInt();
            buffer.position(buffer.position() + skip * 2 * Long.BYTES);
            while (buffer.hasRemaining()) {
                indexMap.put(buffer.getLong(), buffer.getLong());
            }
        }
    }
}
