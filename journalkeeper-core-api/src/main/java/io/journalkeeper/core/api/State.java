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
 * 状态机
 * 状态数据
 * 状态持久化
 * 对应日志位置
 * 可选实现：{@link java.io.Flushable}, {@link java.io.Closeable}
 * @author LiYue
 * Date: 2019-03-20
 */
public interface State<E, Q, R> extends Queryable<Q, R> {
    /**
     * 在状态state上执行命令entries。要求线性语义和原子性.
     * 成功返回新状态，否则抛异常。
     * @param entry 待执行的命令
     * @param index entry在Journal中的索引序号
     * @param batchSize 如果当前entry是一个批量entry，batchSize为这批entry的数量，否则为1；
     * @return 提供给事件 {@link EventType#ON_STATE_CHANGE} 的参数，如果没有参数可以返回null；
     */
    Map<String, String> execute(E entry, int partition, long index, int batchSize);

    /**
     * 当前状态对应的日志位置
     * lastApplied
     */
    long lastApplied();

    /**
     * 状态中包含的最后日志条目的索引值。
     */
    default long lastIncludedIndex() {return lastApplied() - 1;}

    /**
     * 状态中包含的最后日志条目的任期号
     */
    int lastIncludedTerm();

    /**
     * 恢复数据
     * @param path 存放state文件的路径
     * @param properties 属性
     */
    void recover(Path path, RaftJournal raftJournal, Properties properties);

    /**
     * 将状态物理复制一份，保存到path
     */
    State<E, Q, R> takeASnapshot(Path path, RaftJournal raftJournal) throws IOException;

    /**
     * 读取序列化后的状态数据。
     * @param offset 偏移量
     * @param size 本次读取的长度
     *
     */
    byte [] readSerializedData(long offset, int size) throws IOException;

    /**
     * 序列化后的状态长度。
     */
    long serializedDataSize();
    /**
     * 恢复状态。
     * 反复调用install复制序列化的状态数据。
     * 所有数据都复制完成后，最后调用installFinish恢复状态。
     * @param data 日志数据片段
     */
    void installSerializedData(byte [] data, long offset) throws IOException;
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
