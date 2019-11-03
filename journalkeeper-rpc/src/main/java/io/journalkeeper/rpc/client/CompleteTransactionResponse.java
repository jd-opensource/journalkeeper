package io.journalkeeper.rpc.client;

import io.journalkeeper.rpc.LeaderResponse;
import io.journalkeeper.rpc.StatusCode;

import java.util.UUID;

/**
 * 创建事务的响应
 */
public class CompleteTransactionResponse extends LeaderResponse {


    public CompleteTransactionResponse(Throwable throwable){
        super(throwable);
    }

    public CompleteTransactionResponse() {
        super(StatusCode.SUCCESS);
    }


}
