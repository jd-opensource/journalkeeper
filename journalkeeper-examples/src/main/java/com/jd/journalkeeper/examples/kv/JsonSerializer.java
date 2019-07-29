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
package com.jd.journalkeeper.examples.kv;

import com.google.gson.Gson;
import com.jd.journalkeeper.base.Serializer;

import java.nio.charset.StandardCharsets;

/**
 * 用JSON实现的通用序列化反序列化器，适用于性能要求不高的场景。
 * @author liyue25
 * Date: 2019-04-03
 */
public class JsonSerializer<T> implements Serializer<T> {
    private final Gson gson = new Gson();
    private final Class<T> tClass;
    public JsonSerializer(Class<T> tClass) {
        this.tClass = tClass;
    }
    @Override
    public int sizeOf(T t) {
        return serialize(t).length;
    }

    @Override
    public byte[] serialize(T entry) {
        return gson.toJson(entry).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public T parse(byte[] bytes) {
        return gson.fromJson(new String(bytes, StandardCharsets.UTF_8), tClass);
    }
}
