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
package com.jd.journalkeeper.base;

import java.util.concurrent.CompletableFuture;

/**
 * 可查询的
 * @author liyue25
 * Date: 2019-03-14
 * @param <Q> 查询条件
 * @param <R> 查询结果
 */
public interface Queryable<Q, R> {
    /**
     * 查询
     * @param query 查询条件
     * @return 查询结果
     */
    CompletableFuture<R> query(Q query);
}
