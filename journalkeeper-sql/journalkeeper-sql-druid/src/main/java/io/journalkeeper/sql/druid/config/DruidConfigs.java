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
package io.journalkeeper.sql.druid.config;

import io.journalkeeper.sql.state.config.SQLConfigs;

/**
 * DruidConfigs
 * author: gaohaoxiang
 * date: 2019/8/7
 */
public class DruidConfigs {

    public static final String PREFIX = SQLConfigs.PREFIX + ".datasource.druid.";
    public static final String URL = PREFIX + "url";
    public static final String DRIVER_CLASS = PREFIX + "driverClass";
    public static final String USERNAME = PREFIX + "username";
    public static final String PASSWORD = PREFIX + "password";
    public static final String INITIAL_SIZE = PREFIX + "initialSize";
    public static final String MIN_IDLE = PREFIX + "minIdle";
    public static final String MAX_ACTIVE = PREFIX + "maxActive";
    public static final String MAX_WAIT = PREFIX + "maxWait";
    public static final String TIME_BETWEEN_EVICTION_RUNS_MILLIS = PREFIX + "timeBetweenEvictionRunsMillis";
    public static final String VALIDATION_QUERY = PREFIX + "validationQuery";
    public static final String DEFAULT_VALIDATION_QUERY = "SELECT NOW()";
    public static final String TEST_WHILE_IDLE = PREFIX + "testWhileIdle";
    public static final String TEST_ONBORROW = PREFIX + "testOnBorrow";
}