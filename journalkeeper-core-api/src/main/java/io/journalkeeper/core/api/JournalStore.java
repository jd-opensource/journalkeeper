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

import io.journalkeeper.utils.event.Watchable;
import io.journalkeeper.exceptions.IndexOverflowException;
import io.journalkeeper.exceptions.IndexUnderflowException;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Journal Store 客户端。实现：
 * Journal Store API(JK-JS API)
 * 一致性日志接口和事件，兼容openmessaging-storage Minimal API。
 * @author LiYue
 * Date: 2019-04-23
 */
public interface JournalStore extends Watchable {
    /**
     * 写入日志。集群保证按照提供的顺序写入，保证原子性，服务是线性的，任一时间只能有一个客户端使用该服务。
     * 日志在集群中被复制到大多数节点后返回。
     *
     * @param entries 待写入的日志列表。
     * @return 成功时返回当前日志最大索引序号，写入失败抛出异常。
     */
    CompletableFuture<Void> append(List<byte[]> entries);

    /**
     * 写入日志。集群保证按照提供的顺序写入，保证原子性，服务是线性的，任一时间只能有一个客户端使用该服务。
     *
     * @param entries 待写入的日志列表。
     * @param responseConfig 返回响应的配置。See {@link ResponseConfig}
     * @return 成功时返回当前日志最大索引序号，写入失败抛出异常。
     */
    CompletableFuture<Void> append(List<byte[]> entries, ResponseConfig responseConfig);

    /**
     * 查询日志
     * @param index 查询起始位置。
     * @param size 查询条数。
     * @return 读到的日志，返回的数据条数为min(maxIndex - index, size)。
     * @throws IndexOverflowException 参数index必须小于当前maxIndex。
     * @throws IndexUnderflowException 参数index不能小于当前minIndex。
     */
    CompletableFuture<List<byte[]>> get(long index, int size);

    /**
     * 查询当前最小已提交日志索引序号。
     * @return 当前最小已提交日志索引序号。
     */
    CompletableFuture<Long> minIndex();

    /**
     * 查询当前最大已提交日志索引序号。
     * @return 当前最大已提交日志索引序号。
     */
    CompletableFuture<Long> maxIndex();

    /**
     * 删除旧日志，只允许删除最旧的部分日志（即增加minIndex，删除之前的日志）。
     * 保证原子性，服务是线性的，任一时间只能有一个客户端使用该服务。
     * 在集群中复制到大多数节点都完成删除后返回。
     *
     * @param minIndex 删除日志索引位置，小于这个位置的日志将被删除。
     */
    CompletableFuture<Void> compact(long minIndex);


}
