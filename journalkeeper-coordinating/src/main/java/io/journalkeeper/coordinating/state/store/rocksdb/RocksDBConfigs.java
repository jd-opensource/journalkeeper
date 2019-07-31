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
package io.journalkeeper.coordinating.state.store.rocksdb;

/**
 * RocksDBConfigs
 * author: gaohaoxiang
 *
 * date: 2019/5/30
 */
public class RocksDBConfigs {

    public static final String PREFIX = "rocksdb.";

    public static final String OPTIONS_PREFIX = PREFIX + "options.";

    public static final String TABLE_OPTIONS_PREFIX = PREFIX + "table.options.";

    public static final String FILTER_BITSPER_KEY = PREFIX + "filter.bitsPerKey";
}