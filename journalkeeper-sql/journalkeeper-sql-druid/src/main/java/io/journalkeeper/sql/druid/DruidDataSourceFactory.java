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
package io.journalkeeper.sql.druid;

import com.alibaba.druid.pool.DruidDataSource;
import io.journalkeeper.sql.druid.config.DruidConfigs;
import io.journalkeeper.sql.state.jdbc.DataSourceFactory;
import io.journalkeeper.sql.state.jdbc.config.JDBCConfigs;

import javax.sql.DataSource;
import java.nio.file.Path;
import java.util.Properties;

/**
 * DruidDataSourceFactory
 * author: gaohaoxiang
 * date: 2019/8/7
 */
public class DruidDataSourceFactory implements DataSourceFactory {

    @Override
    public DataSource createDataSource(Path path, Properties properties) {
        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setUrl(properties.getProperty(DruidConfigs.URL).replace(JDBCConfigs.DATASOURCE_PATH_PLACEHOLDER, path.toString()));
        dataSource.setDriverClassName(properties.getProperty(DruidConfigs.DRIVER_CLASS));
        dataSource.setUsername(properties.getProperty(DruidConfigs.USERNAME, dataSource.getUsername()));
        dataSource.setPassword(properties.getProperty(DruidConfigs.PASSWORD, dataSource.getPassword()));
        dataSource.setInitialSize(Integer.valueOf(properties.getProperty(DruidConfigs.INITIAL_SIZE, String.valueOf(dataSource.getInitialSize()))));
        dataSource.setMinIdle(Integer.valueOf(properties.getProperty(DruidConfigs.MIN_IDLE, String.valueOf(dataSource.getMinIdle()))));
        dataSource.setMaxActive(Integer.valueOf(properties.getProperty(DruidConfigs.MAX_ACTIVE, String.valueOf(dataSource.getMaxActive()))));
        dataSource.setMaxWait(Integer.valueOf(properties.getProperty(DruidConfigs.MAX_WAIT, String.valueOf(dataSource.getMaxWait()))));
        dataSource.setTimeBetweenEvictionRunsMillis(Integer.valueOf(properties.getProperty(DruidConfigs.TIME_BETWEEN_EVICTION_RUNS_MILLIS,
                String.valueOf(dataSource.getTimeBetweenEvictionRunsMillis()))));
        dataSource.setValidationQuery(properties.getProperty(DruidConfigs.VALIDATION_QUERY, DruidConfigs.DEFAULT_VALIDATION_QUERY));
        dataSource.setTestWhileIdle(Boolean.valueOf(properties.getProperty(DruidConfigs.TEST_WHILE_IDLE, String.valueOf(dataSource.isTestWhileIdle()))));
        dataSource.setTestOnBorrow(Boolean.valueOf(properties.getProperty(DruidConfigs.TEST_ONBORROW, String.valueOf(dataSource.isTestOnBorrow()))));
        return dataSource;
    }

    @Override
    public String type() {
        return DruidConsts.TYPE;
    }
}