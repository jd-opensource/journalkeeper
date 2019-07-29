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
package com.jd.journalkeeper.coordinating.state.store.rocksdb;

import com.jd.journalkeeper.coordinating.state.store.KVStore;
import com.jd.journalkeeper.coordinating.state.store.KVStoreFactory;

import java.nio.file.Path;
import java.util.Properties;

/**
 * RocksDBKVStoreFactory
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/5/30
 */
public class RocksDBKVStoreFactory implements KVStoreFactory {

    @Override
    public KVStore create(Path path, Properties properties) {
        return new RocksDBKVStore(path, properties);
    }

    @Override
    public String type() {
        return "rocksdb";
    }
}