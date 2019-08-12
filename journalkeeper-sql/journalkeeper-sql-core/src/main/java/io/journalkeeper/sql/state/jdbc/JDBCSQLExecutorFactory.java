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
package io.journalkeeper.sql.state.jdbc;

import io.journalkeeper.sql.state.SQLExecutor;
import io.journalkeeper.sql.state.SQLExecutorFactory;
import io.journalkeeper.sql.state.jdbc.config.JDBCConfigs;
import io.journalkeeper.sql.state.support.SQLTransactionExecutorManager;

import java.nio.file.Path;
import java.util.Properties;

/**
 * SQLExecutorFactory
 * author: gaohaoxiang
 * date: 2019/8/1
 */
public class JDBCSQLExecutorFactory implements SQLExecutorFactory {

    @Override
    public SQLExecutor create(Path path, Properties properties) {
        DataSourceFactory dataSourceFactory = DataSourceFactoryManager.getFactory(properties.getProperty(JDBCConfigs.DATASOURCE_TYPE));
        if (dataSourceFactory == null) {
            throw new IllegalArgumentException("datasource not exist");
        }

        JDBCExecutor jdbcExecutor = new JDBCExecutor();
        SQLTransactionExecutorManager transactionExecutorManager = new SQLTransactionExecutorManager(properties);
        return new JDBCSQLExecutor(path, properties, dataSourceFactory, jdbcExecutor, transactionExecutorManager);
    }

    @Override
    public String type() {
        return JDBCConsts.TYPE;
    }
}