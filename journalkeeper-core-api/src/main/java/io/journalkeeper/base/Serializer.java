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
package io.journalkeeper.base;

/**
 * 序列化接口
 * @author LiYue
 * Date: 2019-03-14
 */
public interface Serializer<T> {
    /**
     * 将日志序列化
     * @param entry 日志
     * @return 序列化后的字节数组
     */
    byte [] serialize(T entry);

    /**
     * 反序列化日志
     * @param bytes 存放序列化日志的buffer
     * @return 解析出来的日志
     */
    T parse(byte [] bytes);
}
