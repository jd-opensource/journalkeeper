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
import io.journalkeeper.sql.client.domain.WriteRequest;
import io.journalkeeper.sql.client.domain.WriteResponse;
import io.journalkeeper.sql.state.SQLExecutor;
import io.journalkeeper.sql.state.SQLTransactionExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * SQLStateWriteHandler
 * author: gaohaoxiang
 * date: 2019/8/1
 */
public class SQLStateWriteHandler {

    protected static final Logger logger = LoggerFactory.getLogger(SQLStateWriteHandler.class);

    private Properties properties;
    private SQLExecutor sqlExecutor;

    public SQLStateWriteHandler(Properties properties, SQLExecutor sqlExecutor) {
        this.properties = properties;
        this.sqlExecutor = sqlExecutor;
    }

    public WriteResponse handle(WriteRequest request) {
        OperationTypes type = OperationTypes.valueOf(request.getType());
        switch (type) {
            case INSERT: {
                return doInsert(request);
            }
            case UPDATE: {
                return doUpdate(request);
            }
            case DELETE: {
                return doDelete(request);
            }
            case BATCH: {
                return doBatch(request);
            }
            default: {
                throw new UnsupportedOperationException(String.format("unsupported type, %s", type));
            }
        }
    }

    protected WriteResponse doInsert(WriteRequest request) {
        String result = sqlExecutor.insert(request.getSql(), request.getParams());
        return new WriteResponse(Codes.SUCCESS.getCode(), result);
    }

    protected WriteResponse doUpdate(WriteRequest request) {
        int result = sqlExecutor.update(request.getSql(), request.getParams());
        return new WriteResponse(Codes.SUCCESS.getCode(), result);
    }

    protected WriteResponse doDelete(WriteRequest request) {
        int result = sqlExecutor.delete(request.getSql(), request.getParams());
        return new WriteResponse(Codes.SUCCESS.getCode(), result);
    }

    protected WriteResponse doBatch(WriteRequest request) {
        SQLTransactionExecutor transaction = sqlExecutor.beginTransaction();
        try {
            List<Object> resultList = new ArrayList<>(request.getSqlList().size());
            for (int i = 0; i < request.getSqlList().size(); i++) {
                String sql = request.getSqlList().get(i);
                List<Object> params = request.getParamList().get(i);
                Object result = transaction.update(sql, params);
                resultList.add(result);
            }
            transaction.commit();
            return new WriteResponse(Codes.SUCCESS.getCode(), resultList);
        } catch (Exception e) {
            transaction.rollback();
            throw e;
        }
    }
}
