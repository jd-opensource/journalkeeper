/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.journalkeeper.persistence;

import java.io.Closeable;
import java.io.File;
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
     */
    long min();

    /**
     * 最大位置， 初始化为0
     */
    long max();

    /**
     * 当前刷盘位置
     */
    default long flushed() { return max();}

    /**
     * 执行一次刷盘操作
     * @return 刷盘位置
     */
    default void flush() throws IOException {};
    /**
     * 截断最新的日志。
     * 如果givenMax不在最大最小位置之间，则清空所有数据，并将最大最小位置置为givenMax
     * @param givenMax 新的最大位置.
     */
    void truncate(long givenMax) throws IOException;

    /**
     * 删除旧日志。考虑到大多数基于文件的实现做到精确按位置删除代价较大，
     * 不要求精确删除到给定位置。但不能删除给定位置之后的数据。
     * @param givenMin 给定删除位置，这个位置之前都可以删除。
     * @return 删除后当前最小位置。
     */
    long compact(long givenMin) throws IOException;

    /**
     * 追加写入
     * @return 写入后新的位置
     * @throws TooManyBytesException 当写入数据超长时抛出
     */
    long append(byte [] entry) throws IOException;

    /**
     * 读取数据
     * @param position 起始位置
     * @param length 读取长度
     * @return 存放数据的ByteBuffer
     */
    byte [] read(long position, int length) throws IOException;

    /**
     * 从指定Path恢复Journal，如果没有则创建一个空的。
     * @param path journal存放路径
     * @param properties 属性
     */
    void recover(Path path, Properties properties) throws IOException;

    /**
     * 清空所有数据
     * @throws IOException
     */
    void delete() throws IOException;


    Path getBasePath();
}
