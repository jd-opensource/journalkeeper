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
package io.journalkeeper.core.api;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;

/**
 * 状态机接口。
 *
 * 可选实现：
 * {@link java.io.Flushable}：将状态机中未持久化的输入写入磁盘；
 *
 * @author LiYue
 * Date: 2019-03-20
 */
public interface State {
    /**
     * 在状态state上执行命令entries，JournalKeeper保证执行操作命令的线性语义。要求：
     * <ul>
     *     <li>原子性</li>
     *     <li>幂等性</li>
     * </ul>
     * 成功返回执行结果，否则抛异常。
     *
     * @param entry 待执行的命令
     * @param partition 分区
     * @param index entry在Journal中的索引序号
     * @param batchSize 如果当前entry是一个批量entry，batchSize为这批entry的数量，否则为1；
     * @param journal 当前的journal
     * @return 执行结果。See {@link StateResult}
     */
    StateResult execute(byte[] entry, int partition, long index, int batchSize, RaftJournal journal);

    /**
     * 查询
     * @param query 查询条件
     * @param journal 当前的journal
     * @return 查询结果
     */
    byte[] query(byte[] query, RaftJournal journal);

    /**
     * 从磁盘中恢复状态机中的状态数据，在状态机启动的时候调用。
     * @param path 存放state文件的路径
     * @param properties 属性
     * @throws IOException 发生IO异常时抛出
     */
    void recover(Path path, Properties properties) throws IOException;

    /**
     * 安全的关闭状态机
     */
    default void close() {
    }
}
