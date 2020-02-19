/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.journalkeeper.sql.state.handler;

import io.journalkeeper.sql.client.domain.Codes;
import io.journalkeeper.sql.client.domain.OperationTypes;
import io.journalkeeper.sql.client.domain.ReadRequest;
import io.journalkeeper.sql.client.domain.ReadResponse;
import io.journalkeeper.sql.client.domain.ResultSet;
import io.journalkeeper.sql.state.SQLExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * SQLStateReadHandler
 * author: gaohaoxiang
 * date: 2019/8/1
 */
public class SQLStateReadHandler {

    protected static final Logger logger = LoggerFactory.getLogger(SQLStateReadHandler.class);

    private Properties properties;
    private SQLExecutor sqlExecutor;

    public SQLStateReadHandler(Properties properties, SQLExecutor sqlExecutor) {
        this.properties = properties;
        this.sqlExecutor = sqlExecutor;
    }

    public ReadResponse handle(ReadRequest request) {
        OperationTypes type = OperationTypes.valueOf(request.getType());
        switch (type) {
            case QUERY: {
                return doQuery(request);
            }
            default: {
                throw new UnsupportedOperationException(String.format("unsupported type, %s", type));
            }
        }
    }

    protected ReadResponse doQuery(ReadRequest request) {
        ResultSet result = sqlExecutor.query(request.getSql(), request.getParams());
        return new ReadResponse(Codes.SUCCESS.getCode(), result);
    }
}
