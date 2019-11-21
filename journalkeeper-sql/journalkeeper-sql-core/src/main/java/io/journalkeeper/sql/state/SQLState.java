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
package io.journalkeeper.sql.state;

import io.journalkeeper.core.api.RaftJournal;
import io.journalkeeper.core.api.State;
import io.journalkeeper.core.api.StateFactory;
import io.journalkeeper.core.api.StateResult;
import io.journalkeeper.sql.client.domain.ReadRequest;
import io.journalkeeper.sql.client.domain.ReadResponse;
import io.journalkeeper.sql.client.domain.WriteRequest;
import io.journalkeeper.sql.client.domain.WriteResponse;
import io.journalkeeper.sql.exception.SQLException;
import io.journalkeeper.sql.state.config.SQLConfigs;
import io.journalkeeper.sql.state.handler.SQLStateHandler;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.Properties;

/**
 * SQLState
 * author: gaohaoxiang
 * date: 2019/8/1
 */
public class SQLState implements State<WriteRequest, WriteResponse, ReadRequest, ReadResponse> {

    private static final Logger logger = LoggerFactory.getLogger(SQLState.class);

    private Properties properties;
    private SQLExecutor executor;
    private SQLStateHandler handler;


    @Override
    public void recover(Path path, Properties properties) throws IOException {
        this.executor = SQLExecutorManager.getExecutor(properties.getProperty(SQLConfigs.EXECUTOR_TYPE)).create(path, properties);
        if (this.executor == null) {
            throw new IllegalArgumentException("executor not exist");
        }
        this.properties = properties;
        this.handler = new SQLStateHandler(properties, executor);
        initExecutor(properties);
    }

    protected void initExecutor(Properties properties) {
        String initFile = properties.getProperty(SQLConfigs.INIT_FILE);
        if (StringUtils.isBlank(initFile)) {
            return;
        }

        try {
            InputStream initFileStream = SQLState.class.getResourceAsStream(initFile);
            if (initFileStream == null) {
                logger.warn("init file not exist, file: {}", initFile);
                return;
            }

            String sql = IOUtils.toString(initFileStream, Charset.forName("UTF-8"));
            executor.update(sql, null);
        } catch (Exception e) {
            logger.error("init exception", e);
            throw new SQLException(e);
        }
    }

    @Override
    public StateResult<WriteResponse> execute(WriteRequest request, int partition, long index, int batchSize, RaftJournal raftJournal) {
        WriteResponse response = handler.handleWrite(request);
        StateResult<WriteResponse> result = new StateResult<>(response);
        result.getEventData().put("type", String.valueOf(request.getType()));
        // TODO 参数处理
//        eventParams.put("sql", request.getSql());
//        eventParams.put("params", request.getParams().toString());
        return result;
    }

    @Override
    public ReadResponse query(ReadRequest request, RaftJournal raftJournal) {
       return handler.handleRead(request);
    }
}