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
package io.journalkeeper.sql.state.handler;

import io.journalkeeper.sql.client.domain.Codes;
import io.journalkeeper.sql.client.domain.ReadRequest;
import io.journalkeeper.sql.client.domain.ReadResponse;
import io.journalkeeper.sql.client.domain.WriteRequest;
import io.journalkeeper.sql.client.domain.WriteResponse;
import io.journalkeeper.sql.client.support.TransactionIdGenerator;
import io.journalkeeper.sql.state.SQLExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * SQLStateHandler
 * author: gaohaoxiang
 * date: 2019/8/1
 */
public class SQLStateHandler {

    protected static final Logger logger = LoggerFactory.getLogger(SQLStateHandler.class);

    private Properties properties;
    private SQLExecutor sqlExecutor;
    private TransactionIdGenerator transactionIdGenerator;
    private SQLStateReadHandler readHandler;
    private SQLStateWriteHandler writeHandler;

    public SQLStateHandler(Properties properties, SQLExecutor sqlExecutor) {
        this.properties = properties;
        this.sqlExecutor = sqlExecutor;
        this.transactionIdGenerator = new TransactionIdGenerator();
        this.readHandler = new SQLStateReadHandler(properties, sqlExecutor);
        this.writeHandler = new SQLStateWriteHandler(properties, sqlExecutor, transactionIdGenerator);
    }

    public WriteResponse handleWrite(WriteRequest request) {
        try {
            return writeHandler.handle(request);
        } catch (Exception e) {
            logger.error("sql write exception, request: {}", request, e);
            return new WriteResponse(Codes.ERROR.getCode(), e.toString());
        }
    }

    public ReadResponse handleRead(ReadRequest request) {
        try {
            return readHandler.handle(request);
        } catch (Exception e) {
            logger.error("sql read exception, request: {}", request, e);
            return new ReadResponse(Codes.ERROR.getCode(), e.toString());
        }
    }
}