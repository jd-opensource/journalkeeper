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
package io.journalkeeper.sql.h2;

import io.journalkeeper.sql.h2.config.H2Configs;
import io.journalkeeper.sql.state.jdbc.DataSourceFactory;
import org.apache.commons.lang3.StringUtils;
import org.h2.jdbcx.JdbcDataSource;

import javax.sql.DataSource;
import java.nio.file.Path;
import java.util.Properties;

/**
 * H2DataSourceFactory
 * author: gaohaoxiang
 * date: 2019/8/7
 */
public class H2DataSourceFactory implements DataSourceFactory {

    @Override
    public DataSource createDataSource(Path path, Properties properties) {
        String url = properties.getProperty(H2Configs.DATASOURCE_URL);
        if (StringUtils.isBlank(url)) {
            url = String.format("jdbc:h2:file:%s/data;AUTO_SERVER=TRUE;MVCC=TRUE;LOCK_TIMEOUT=30000", path.toString());
        }
        JdbcDataSource datasource = new JdbcDataSource();
        datasource.setURL(url);
        return datasource;
    }

    @Override
    public String type() {
        return H2Consts.TYPE;
    }
}