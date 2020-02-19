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
package io.journalkeeper.persistence;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;

/**
 * 日志持久化接口
 * @author LiYue
 * Date: 2019-03-14
 */
public interface JournalPersistence extends Closeable {
    /**
     * 最小位置，初始化为0
     * @return 最小位置
     */
    long min();

    /**
     * 物理最小位置，初始化为0
     * @return 物理最小位置
     */
    long physicalMin();

    /**
     * 最大位置， 初始化为0
     * @return 最大位置
     */
    long max();

    /**
     * 当前刷盘位置
     * @return 当前刷盘位置
     */
    default long flushed() {
        return max();
    }

    /**
     * 执行一次刷盘操作
     * @throws IOException 发生IO异常时抛出
     */
    default void flush() throws IOException {
    }

    ;

    /**
     * 截断最新的日志。
     * @param givenMax 新的最大位置.
     * @throws IllegalArgumentException 如果givenMax不在最大最小位置之间时抛出此异常
     * @throws IOException 发生IO异常时抛出
     */
    void truncate(long givenMax) throws IOException;

    /**
     * 删除旧日志。考虑到大多数基于文件的实现做到精确按位置删除代价较大，
     * 不要求精确删除到给定位置。但不能删除给定位置之后的数据。
     * @param givenMin 给定删除位置，这个位置之前都可以删除。
     * @return 删除后当前最小位置。
     * @throws IOException 发生IO异常时抛出
     */
    long compact(long givenMin) throws IOException;

    /**
     * 追加写入
     * @param entry 待写入的entry
     * @return 写入后新的位置
     * @throws TooManyBytesException 当写入数据超长时抛出
     * @throws IOException 发生IO异常时抛出
     */
    long append(byte[] entry) throws IOException;

    /**
     * 读取数据
     * @param position 起始位置
     * @param length 读取长度
     * @return 存放数据的ByteBuffer
     * @throws IOException 发生IO异常时抛出
     */
    byte[] read(long position, int length) throws IOException;

    /**
     * 从指定Path恢复Journal，如果没有则创建一个空的。
     * @param path journal存放路径
     * @param min 逻辑最小位置
     * @param properties 属性
     * @throws IOException 发生IO异常时抛出
     */
    void recover(Path path, long min, Properties properties) throws IOException;

    /**
     * 从指定Path恢复Journal，如果没有则创建一个空的。给定最小位置0.
     * @param path journal存放路径
     * @param properties 属性
     * @throws IOException 发生IO异常时抛出
     */
    default void recover(Path path, Properties properties) throws IOException {
        recover(path, 0L, properties);
    }

    /**
     * 清空所有数据
     * @throws IOException 发生IO异常时抛出
     */
    void delete() throws IOException;


    Path getBasePath();
}
