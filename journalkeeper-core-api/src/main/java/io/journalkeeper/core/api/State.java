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
import io.journalkeeper.utils.event.EventType;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;

/**
 * 状态机接口。
 *
 * 状态机负责实现{@link State#execute(Object, int, long, int, Map)}接口执行操作日志{@link E}，并返回执行结果{@link ER}。
 * 状态机还可以实现{@link State#query(Object)}查询接口，用于查询状态机中的状态数据。
 *
 * 可选实现：
 * {@link java.io.Flushable}：将状态机中未持久化的输入写入磁盘；
 * {@link java.io.Closeable}：安全关闭状态机；
 *
 * 为了支持RAFT协议进行状态快照的复制，状态机中的状态数据需要支持：
 * {@link #takeASnapshot(Path, RaftJournal)} 给当前状态数据生成一个快照，快照中的状态数据与当前状态完全一样，
 * 新的快照数据保存到参数path目录中。
 *
 * {@link #serializedDataSize()}, {@link #readSerializedData(long, int)},
 * {@link #installSerializedData(byte[], long)}, {@link #installFinish(long, int)}
 * 以上这四个方法用于将当前状态数据序列化，然后复制的远端节点上，再恢复成状态，具体的流程是：
 * 1. 调用{@link #serializedDataSize()} 计算当前状态数据序列化之后的长度；
 * 2. 反复在读取状态的节点调用{@link #readSerializedData(long, int)}读取序列化后的状态数据，复制到目标节点后调用 {@link #installSerializedData(byte[], long)}写入状态数据；
 * 3. 复制完成后，在目标节点调用{@link #installFinish(long, int)}恢复状态数据，完成状态复制。
 *
 * @author LiYue
 * Date: 2019-03-20
 */
public interface State<
        E, // 操作日志类型
        ER, // 状态机执行操作日志后返回结果类型
        Q, // 查询接口请求参数的类型
        QR // 查询接口返回查询结果类型
        > extends Queryable<Q, QR> {
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
     * @param eventParams 提供给事件 {@link EventType#ON_STATE_CHANGE} 的参数；
     * @return 执行结果
     */
    ER execute(E entry, int partition, long index, int batchSize, Map<String, String> eventParams);

    /**
     * 当前状态对应的日志位置
     * @return 状态机执行位置
     */
    long lastApplied();

    /**
     * 状态中包含的最后日志条目的索引值。
     * @return 最后日志条目的索引值
     */
    default long lastIncludedIndex() {return lastApplied() - 1;}

    /**
     * 状态中包含的最后日志条目的任期号
     * @return 最后日志条目的任期号
     */
    int lastIncludedTerm();

    /**
     * 恢复数据
     * @param path 存放state文件的路径
     * @param raftJournal 当前的journal
     * @param properties 属性
     */
    void recover(Path path, RaftJournal raftJournal, Properties properties);

    /**
     * 将状态物理复制一份，保存到path
     * @param path 新的状态的保存目录
     * @param raftJournal 当前的journal
     * @return 新的状态机快照的实例
     * @throws IOException 发生IO异常时抛出
     */
    State<E, ER, Q, QR> takeASnapshot(Path path, RaftJournal raftJournal) throws IOException;

    /**
     * 读取序列化后的状态数据。
     * @param offset 偏移量
     * @param size 本次读取的长度
     * @return 序列化后的状态数据
     * @throws IOException 发生IO异常时抛出
     */
    byte [] readSerializedData(long offset, int size) throws IOException;

    /**
     * 序列化后的状态长度。
     * @return 序列化后的状态长度。
     */
    long serializedDataSize();
    /**
     * 恢复状态。
     * 反复调用install复制序列化的状态数据。
     * 所有数据都复制完成后，最后调用installFinish恢复状态。
     * @param data 日志数据片段
     * @param offset 从这个全局偏移量开始安装
     * @throws IOException 发生IO异常时抛出
     */
    void installSerializedData(byte [] data, long offset) throws IOException;

    /**
     * 完整状态安装
     * @param lastApplied 状态机执行位置
     * @param lastIncludedTerm 最后日志条目的任期号
     */
    void installFinish(long lastApplied, int lastIncludedTerm);

    /**
     * 删除所有状态数据。
     */
    void clear();

    /**
     * lastApplied += 1
     */
    void next();
    /**
     * 跳过一个Raft Entry
     */
    void skip();

}
