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
package io.journalkeeper.core.api;

import io.journalkeeper.base.Queryable;
import io.journalkeeper.base.Replicable;
import io.journalkeeper.utils.event.EventType;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;

/**
 * 状态机接口。
 *
 * 状态机负责实现{@link #execute(Object, int, long, int)}接口执行操作日志{@link E}，并返回执行结果{@link ER}。
 * 状态机还可以实现{@link State#query(Object)}查询接口，用于查询状态机中的状态数据。
 *
 * 可选实现：
 * {@link java.io.Flushable}：将状态机中未持久化的输入写入磁盘；
 * {@link java.io.Closeable}：安全关闭状态机；
 *
 * {@link #serializedDataSize()}, {@link #readSerializedTrunk(long, int)},
 * {@link #installSerializedTrunk(byte[], long, boolean)}
 * 以上这三个方法用于将当前状态数据序列化，然后复制的远端节点上，再恢复成状态，具体的流程是：
 * 1. 调用{@link #serializedDataSize()} 计算当前状态数据序列化之后的长度；
 * 2. 反复在读取状态的节点调用{@link #readSerializedTrunk(long, int)}读取序列化后的状态数据，复制到目标节点后调用 {@link #installSerializedTrunk(byte[], long, boolean)}写入状态数据；
 *
 * @author LiYue
 * Date: 2019-03-20
 */
public interface State<
        E, // 操作日志类型
        ER, // 状态机执行操作日志后返回结果类型
        Q, // 查询接口请求参数的类型
        QR // 查询接口返回查询结果类型
        > extends Queryable<Q, QR>, Replicable {
    /**
     * 在状态state上执行命令entries。要求：
     * 1. 线性语义
     * 2. 原子性
     * 3. 幂等性
     *
     * 成功返回执行结果，否则抛异常。
     * @param entry 待执行的命令
     * @param partition 分区
     * @param index entry在Journal中的索引序号
     * @param batchSize 如果当前entry是一个批量entry，batchSize为这批entry的数量，否则为1；
     * @return 执行结果
     */
    StateResult<ER> execute(E entry, int partition, long index, int batchSize);

    /**
     * 恢复数据
     * @param path 存放state文件的路径
     * @param raftJournal 当前的journal
     * @param properties 属性
     */
    void recover(Path path, RaftJournal raftJournal, Properties properties);

    /**
     * 删除所有状态数据。
     */
    void clear();


    default void close(){}
}
