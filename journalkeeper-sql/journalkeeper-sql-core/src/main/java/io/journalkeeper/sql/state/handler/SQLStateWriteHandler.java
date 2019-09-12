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
import io.journalkeeper.sql.client.domain.OperationTypes;
import io.journalkeeper.sql.client.domain.WriteRequest;
import io.journalkeeper.sql.client.domain.WriteResponse;
import io.journalkeeper.sql.state.SQLExecutor;
import io.journalkeeper.sql.state.SQLTransactionExecutor;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
            case TRANSACTION_BEGIN: {
                return doBeginTransaction(request);
            }
            case TRANSACTION_COMMIT: {
                return doCommitTransaction(request);
            }
            case TRANSACTION_ROLLBACK: {
                return doRollbackTransaction(request);
            }
            default: {
                throw new UnsupportedOperationException(String.format("unsupported type, %s", type));
            }
        }
    }

    // TODO 简单写，需要重构
    protected WriteResponse doInsert(WriteRequest request) {
        if (StringUtils.isNotBlank(request.getId())) {
            SQLTransactionExecutor transaction = sqlExecutor.getTransaction(request.getId());
            if (transaction == null) {
                logger.warn("transaction not exist, id: {}", request.getId());
                return new WriteResponse(Codes.TRANSACTION_NOT_EXIST.getCode());
            }
            String result = transaction.insert(request.getSql(), request.getParams());
            return new WriteResponse(Codes.SUCCESS.getCode(), result, null);
        } else {
            String result = sqlExecutor.insert(request.getSql(), request.getParams());
            return new WriteResponse(Codes.SUCCESS.getCode(), result, null);
        }
    }

    protected WriteResponse doUpdate(WriteRequest request) {
        if (StringUtils.isNotBlank(request.getId())) {
            SQLTransactionExecutor transaction = sqlExecutor.getTransaction(request.getId());
            if (transaction == null) {
                logger.warn("transaction not exist, id: {}", request.getId());
                return new WriteResponse(Codes.TRANSACTION_NOT_EXIST.getCode());
            }
            int result = transaction.update(request.getSql(), request.getParams());
            return new WriteResponse(Codes.SUCCESS.getCode(), String.valueOf(result), null);
        } else {
            int result = sqlExecutor.update(request.getSql(), request.getParams());
            return new WriteResponse(Codes.SUCCESS.getCode(), String.valueOf(result), null);
        }
    }

    protected WriteResponse doDelete(WriteRequest request) {
        if (StringUtils.isNotBlank(request.getId())) {
            SQLTransactionExecutor transaction = sqlExecutor.getTransaction(request.getId());
            if (transaction == null) {
                logger.warn("transaction not exist, id: {}", request.getId());
                return new WriteResponse(Codes.TRANSACTION_NOT_EXIST.getCode());
            }
            int result = transaction.delete(request.getSql(), request.getParams());
            return new WriteResponse(Codes.SUCCESS.getCode(), String.valueOf(result), null);
        } else {
            int result = sqlExecutor.delete(request.getSql(), request.getParams());
            return new WriteResponse(Codes.SUCCESS.getCode(), String.valueOf(result), null);
        }
    }

    protected WriteResponse doBeginTransaction(WriteRequest request) {
        if (StringUtils.isBlank(request.getId())) {
            return new WriteResponse(Codes.TRANSACTION_NOT_EXIST.getCode());
        }
        String id = request.getId();
        SQLTransactionExecutor transaction = sqlExecutor.beginTransaction(id);
        return new WriteResponse(Codes.SUCCESS.getCode(), id, null);
    }

    protected WriteResponse doCommitTransaction(WriteRequest request) {
        SQLTransactionExecutor transaction = sqlExecutor.getTransaction(request.getId());
        if (transaction == null) {
            logger.warn("transaction not exist, id: {}", request.getId());
            return new WriteResponse(Codes.TRANSACTION_NOT_EXIST.getCode());
        }
        transaction.commit();
        return new WriteResponse(Codes.SUCCESS.getCode());
    }

    protected WriteResponse doRollbackTransaction(WriteRequest request) {
        SQLTransactionExecutor transaction = sqlExecutor.getTransaction(request.getId());
        if (transaction == null) {
            logger.warn("transaction not exist, id: {}", request.getId());
            return new WriteResponse(Codes.TRANSACTION_NOT_EXIST.getCode());
        }
        transaction.rollback();
        return new WriteResponse(Codes.SUCCESS.getCode());
    }
}
